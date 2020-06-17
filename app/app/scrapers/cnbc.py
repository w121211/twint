import asyncio
import dataclasses
import datetime
import json
import logging
import random
import sys
import urllib
from typing import Tuple, List, Iterable

import aiohttp
import elasticsearch
import pandas as pd
import requests
from lxml import etree
from newspaper import Article
from omegaconf import DictConfig
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent

from .base import BaseScraper
from .. import fetch
from ..store import es

log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))

ua = UserAgent(verify_ssl=False)
ua.update()


@dataclasses.dataclass
class _Ticker:
    '''<p>...some text...<a href='$AAA'>...some anchor text</a>...<a>....</a>...</p>'''
    text: str
    # (anchor_text, ticker)
    labels: List[Tuple[str, str]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Parsed:
    keywords: List[str] = dataclasses.field(default_factory=list)
    tickers: List[_Ticker] = dataclasses.field(default_factory=list)


def parse_tickers(node: etree.Element) -> List[_Ticker]:
    tickers = []

    # find all <a> and de-duplicate their parents
    for p in set([e.getparent() for e in node.cssselect('a')]):
        text = etree.tostring(
            p, method='text', encoding='utf-8').decode('utf-8').strip()
        tk = _Ticker(text)

        for a in p.cssselect('a'):
            href = a.get('href')
            queries = dict(urllib.parse.parse_qsl(
                urllib.parse.urlsplit(href).query))
            try:
                tk.labels.append((a.text, queries['symbol']))
            except KeyError:
                continue
        if len(tk.labels) > 0:
            tickers.append(tk)

    return tickers


class CnbcScraper(BaseScraper):
    def __init__(self, cfg: DictConfig = None, use_requests=False):
        super().__init__(cfg)
        self.use_requests = use_requests

    def parse(self, from_url: str, resp: aiohttp.ClientResponse, html: str) -> es.Page:
        article = Article(str(resp.url))
        article.set_html(html)
        article.parse()
        parsed = Parsed(
            keywords=article.meta_keywords,
            tickers=parse_tickers(article.clean_top_node))
        page = es.Page(
            from_url=from_url,
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
        page.save()

    def _requests_parse(self, from_url: str, resp: requests.Response, html: str) -> es.Page:
        article = Article(str(resp.url))
        article.set_html(html)
        article.parse()
        parsed = Parsed(
            keywords=article.meta_keywords,
            tickers=parse_tickers(article.clean_top_node))
        page = es.Page(
            from_url=from_url,
            resolved_url=str(resp.url),
            http_status=resp.status_code,
            article_metadata=json.dumps(article.meta_data),
            article_published_at=article.publish_date,
            article_title=article.title,
            article_text=article.text,
            article_html=etree.tostring(
                article.clean_top_node, encoding='utf-8').decode('utf-8'),
            parsed=json.dumps(dataclasses.asdict(parsed)),
            fetched_at=datetime.datetime.now(),)
        page.save()

    async def worker(self, queue: asyncio.Queue):
        if self.use_requests:
            await self._requests_worker(queue)
        else:
            ua = UserAgent(verify_ssl=False, use_cache_server=False).random
            async with aiohttp.ClientSession(raise_for_status=True, headers=[("User-Agent", ua)]) as sess:
                while True:
                    url = await queue.get()
                    try:
                        es.Page.get(id=url)
                        log.info('page existed, skip {}'.format(url))
                    except elasticsearch.NotFoundError:
                        try:
                            # resp, html = await fetch.get(url)
                            async with sess.get(url) as resp:
                                html = await resp.text()
                                self.parse(url, resp, html)
                                log.info('page scraped {}'.format(url))
                        except aiohttp.ClientResponseError as e:
                            page = es.Page(
                                from_url=url,
                                resolved_url=str(e.request_info.real_url),
                                http_status=e.status,)
                            page.save()
                            log.info("fetch error & skiped: {}".format(e))
                            log.error(e)
                            self.error_urls.append(url)
                    finally:
                        queue.task_done()

    async def _requests_fetch(self, url: str):
        if self.proxies is not None:
            p = random.choice(self.proxies)
            proxies = {"http": "http://{}".format(p),
                       "https": "http://{}".format(p)}
            print(proxies)

        with requests.Session() as s:
            try:
                await asyncio.sleep(3)
                log.info('start scraping page {}'.format(url))
                s.mount(url, HTTPAdapter(max_retries=3))
                resp = s.get(
                    url, headers={"User-Agent": ua.random}, timeout=5, proxies=proxies)
                resp.raise_for_status()
                html = resp.text
                self._requests_parse(url, resp, html)
                log.info('page scraped {}'.format(url))
            except requests.RequestException as e:
                page = es.Page(
                    from_url=url,
                    resolved_url=str(e.response.url),
                    http_status=e.response.status_code,)
                page.save()
                log.info("fetch error & skiped: {}".format(e))
                log.error(e)
                self.error_urls.append(url)

    async def _requests_worker(self, queue: asyncio.Queue):
        while True:
            url = await queue.get()
            try:
                page = es.Page.get(id=url)
                if "portal.vifb.vn:8880" in page.resolved_url:
                    await self._requests_fetch(url)
                else:
                    log.info('page existed, skip {}'.format(url))
            except elasticsearch.NotFoundError:
                await self._requests_fetch(url)
            finally:
                queue.task_done()

    def startpoints(self) -> Iterable[str]:
        # i = 0
        for hit in es.scan_twint('CNBC'):
            if not hasattr(hit, "urls"):
                continue
            for u in hit.urls:
                # i += 1
                # if i > 30:
                #     return
                yield u

    async def run(self, n_workers=1, *args, **kwargs):
        log.info("scraper start running: {} workers".format(n_workers))

        queue = asyncio.Queue()
        es.init()

        for url in self.startpoints(*args, **kwargs):
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

        log.info("all jobs done.")
