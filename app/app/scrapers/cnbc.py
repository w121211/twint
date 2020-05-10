import asyncio
import datetime
import json
import logging
import sys
import dataclasses
import urllib
from typing import Tuple, List, Iterable

import aiohttp
import elasticsearch
import pandas as pd
from lxml import etree
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent

from .base import BaseScraper
from .. import fetch
from ..store import es

log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))


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
    def __init__(self, cfg: DictConfig = None):
        super().__init__(cfg)

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

    def startpoints(self) -> Iterable[str]:
        for hit in es.scan_twint('ReutersBiz'):
            for u in hit.urls:
                yield u

    async def worker(self, queue: asyncio.Queue):
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
