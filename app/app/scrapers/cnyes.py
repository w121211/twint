import asyncio
import datetime
import dataclasses
import json
import logging
import re
from typing import Tuple, List, Iterable

import aiohttp
import pandas as pd
import elasticsearch
from bs4 import BeautifulSoup
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent
from lxml import etree

from ..store import es
from .. import fetch
from .base import BasePageScraper, TickerText, Parsed

log = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class CnyesApiScraper:
    def __init__(self, cfg: DictConfig = None):
        super().__init__()
        self.cfg = cfg
        # self.start = datetime.datetime(*cfg.scraper.cnyes_api.start)
        # self.until = datetime.datetime(cfg.scraper.cnyes_api.until)
        self.start = datetime.datetime.now() - datetime.timedelta(days=30)
        self.until = datetime.datetime.now()
        self.error_urls = []

    def parse(self, resp: aiohttp.ClientResponse, data: dict) -> Tuple[List[es.Page], List[str]]:
        pages, new_urls = [], []
        if data["items"]['next_page_url'] is not None:
            new_urls.append(
                f'https://news.cnyes.com{data["items"]["next_page_url"]}')
        for e in data['items']['data']:
            url = f"https://news.cnyes.com/news/id/{e['newsId']}"
            try:
                es.Page.get(id=url)
            except elasticsearch.NotFoundError:
                p = es.Page(
                    from_url=url,
                    entry_title=e['title'],
                    entry_summary=e['summary'],
                    entry_published_at=datetime.datetime.fromtimestamp(
                        e['publishAt']),
                    entry_tickers=[
                        x for x in map(lambda x: x["symbol"], e['market'])],
                    entry_meta=json.dumps(e),)
                pages.append(p)
        return pages, new_urls

    def startpoints(self, start: datetime.datetime, until: datetime.datetime) -> List[str]:
        urls = []
        while start < until:
            _until = start + datetime.timedelta(days=30)
            if _until > until:
                _until = until
            urls.append('https://news.cnyes.com/api/v3/news/category/headline' +
                        f'?startAt={int(start.timestamp())}' +
                        f'&endAt={int(_until.timestamp())}' +
                        '&limit=100')
            start = _until
        return urls

    async def worker(self, queue: asyncio.Queue):
        ua = UserAgent(verify_ssl=False, use_cache_server=False).random
        async with aiohttp.ClientSession(raise_for_status=True, headers=[("User-Agent", ua)]) as sess:
            while True:
                url = await queue.get()
                try:
                    async with sess.get(url) as resp:
                        pages, new_urls = self.parse(resp, await resp.json())
                        for p in pages:
                            p.save()
                        for u in new_urls:
                            queue.put_nowait(u)
                        log.info(f'scraped: {url}')
                        await asyncio.sleep(30)
                except aiohttp.ClientError as e:
                    log.error(e)
                    self.error_urls.append(url)
                except Exception as e:
                    log.error(f'Error on: {url}')
                    log.error(e)
                finally:
                    queue.task_done()

    async def run(self, n_workers=1, loop_every=None):
        queue = asyncio.Queue()
        es.init()

        for url in self.startpoints(self.start, self.until):
            queue.put_nowait(url)
        tasks = [asyncio.create_task(self.worker(queue))
                 for _ in range(n_workers)]

        await queue.join()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # await asyncio.gather(*tasks)

        df = pd.DataFrame({'url': self.error_urls})
        df.to_csv("./error_urls.csv", index=False)


class CnyesPageScraper(BasePageScraper):
    domain = 'cnyes.com'
    kw_regex = re.compile(r'^\/tag\/(\w+)')

    def _parse_tickers(self, node: etree.Element) -> List[TickerText]:
        tickers = []
        # find all <a> and de-duplicate their parents
        for p in set([e.getparent() for e in node.cssselect('a')]):
            text = etree.tostring(
                p, method='text', encoding='utf-8').decode('utf-8').strip()
            tt = TickerText(text)

            for a in p.cssselect('a'):
                href = a.get('href')
                if 'invest.cnyes.com' in href:
                    tt.labels.append(("", a.text))
            if len(tt.labels) > 0:
                tickers.append(tt)
        return tickers

    def _parse_keywords(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, 'html.parser')
        e = soup.select_one('meta[itemprop="keywords"]')
        if e is not None:
            return e['content'].split(",")
        else:
            return []

    def parse(self, from_url: str, resp: aiohttp.ClientResponse, html: str) -> es.Page:
        article = Article(str(resp.url))
        article.set_html(html)
        article.parse()
        parsed = Parsed(
            keywords=self._parse_keywords(html),
            tickers=self._parse_tickers(article.clean_top_node),)
        page = es.Page.get_or_create(from_url)
        page.update(
            resolved_url=str(resp.url),
            http_status=resp.status,
            article_metadata=json.dumps(article.meta_data),
            article_published_at=article.publish_date,
            article_title=article.title,
            article_text=article.text,
            article_html=etree.tostring(
                article.clean_top_node, encoding='utf-8').decode('utf-8'),
            parsed=json.dumps(dataclasses.asdict(parsed)),
            fetched_at=datetime.datetime.now(),)
