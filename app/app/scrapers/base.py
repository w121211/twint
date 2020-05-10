import abc
import asyncio
import datetime
import dataclasses
import json
import logging
import urllib
from typing import Tuple, Iterable, List

import aiohttp
import pandas as pd
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent
from lxml import etree

from ..store import es
from .. import fetch

log = logging.getLogger(__name__)


@dataclasses.dataclass
class TickerText:
    '''<p>...some text...<a href='$AAA'>...some anchor text</a>...<a>....</a>...</p>'''
    # p_text
    text: str
    # [(anchor_text, ticker), (...), (...), ...]
    labels: List[Tuple[str, str]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Parsed:
    keywords: List[str] = dataclasses.field(default_factory=list)
    tickers: List[TickerText] = dataclasses.field(default_factory=list)


class BaseScraper:
    def __init__(self, cfg: DictConfig = None):
        super().__init__()
        self.cfg = cfg
        self.error_urls = []

    @abc.abstractmethod
    def parse(self, from_url: str, resp: aiohttp.ClientResponse, text: str):
        pass

    @abc.abstractmethod
    def startpoints(self, *args, **kwargs) -> Iterable[str]:
        pass

    async def worker(self, queue: asyncio.Queue):
        ua = UserAgent(verify_ssl=False, use_cache_server=False).random
        async with aiohttp.ClientSession(raise_for_status=True, headers=[("User-Agent", ua)]) as sess:
            while True:
                url = await queue.get()
                try:
                    async with sess.get(url) as resp:
                        # j = await resp.json()
                        new_urls = self.parse(url, resp, await resp.text())
                        if new_urls is not None:
                            for u in new_urls:
                                queue.put_nowait(u)
                        log.info(f'scraped: {url}')
                except aiohttp.ClientError as e:
                    log.error(e)
                    self.error_urls.append(url)
                finally:
                    queue.task_done()

    async def run(self, n_workers=1, *args, **kwargs):
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


class BasePageScraper(BaseScraper):
    # for filtering page entries
    domain = 'domain.com'

    def _parse_tickers(self, node: etree.Element) -> List[TickerText]:
        tickers = []

        # find all <a> and de-duplicate their parents
        for p in set([e.getparent() for e in node.cssselect('a')]):
            text = etree.tostring(
                p, method='text', encoding='utf-8').decode('utf-8').strip()
            tk = TickerText(text)
            print(text)

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

    def startpoints(self, *args, **kwargs) -> Iterable[str]:
        # yield es.Page.scan_urls(self.domain)
        for i, url in enumerate(es.Page.scan_urls(self.domain)):
            yield url
            if i > 10:
                break

    def parse(self, from_url: str, resp: aiohttp.ClientResponse, text: str) -> es.Page:
        article = Article(str(resp.url))
        article.set_html(html)
        article.parse()
        parsed = Parsed(
            keywords=article.meta_keywords,
            tickers=self._parse_tickers(article.clean_top_node))
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
