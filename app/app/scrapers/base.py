import abc
import asyncio
import datetime
import dataclasses
import json
import logging
import random
import urllib
from typing import Tuple, Iterable, List, Any

import aiohttp
import elasticsearch
import pandas as pd
from hydra import utils
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent
from lxml import etree

from ..store import es
from .. import fetch

log = logging.getLogger(__name__)

ua = UserAgent(verify_ssl=False)

data = pd.read_csv('./resource/proxies.txt', sep=" ", header=None)
proxies = list(data[0])


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
        self.proxies = None
        # self.proxies = pd.read_csv(
        #     utils.to_absolute_path(cfg.proxy.path),
        #     sep=" ", header=None)[0] if cfg.proxy.path is not None else None

    @abc.abstractmethod
    def parse(self, from_url: str, resp: aiohttp.ClientResponse, text: str):
        pass

    @abc.abstractmethod
    def startpoints(self, *args, **kwargs) -> Iterable[Any]:
        pass

    async def worker(self, queue: asyncio.Queue):
        async with aiohttp.ClientSession(raise_for_status=True, headers=[("User-Agent", ua.random)]) as sess:
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

    async def _run(self, n_workers=1, *args, **kwargs):
        queue = asyncio.Queue()
        es.init()

        for startpoint in self.startpoints(*args, **kwargs):
            queue.put_nowait(startpoint)
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

    async def run(self, n_workers=1, loop_every=None, *args, **kwargs):
        if loop_every is not None:
            while True:
                log.info(
                    f"scraper start running: {n_workers} workers, loop every {loop_every} seconds")
                start = datetime.datetime.now()
                await self._run(n_workers, *args, **kwargs)
                wait = start + \
                    datetime.timedelta(0, loop_every) - datetime.datetime.now()
                log.info(
                    f'all jobs done, scraper sleep for {wait.total_seconds()} seconds')
                await asyncio.sleep(wait.total_seconds())
        else:
            log.info(f"scraper start running: {n_workers} workers")
            await self._run(n_workers, *args, **kwargs)
            log.info(f'all jobs done')


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

    def parse(self, from_url: str, resp: aiohttp.ClientResponse, html: str) -> es.Page:
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

    async def worker(self, queue: asyncio.Queue):
        async with aiohttp.ClientSession(
                raise_for_status=True,
                headers=[("User-Agent", ua.random)],
                timeout=aiohttp.ClientTimeout(total=60)) as sess:
            while True:
                url = await queue.get()
                try:
                    page = es.Page.get(id=url)
                    if page.http_status != 200:
                        raise elasticsearch.NotFoundError()
                    log.info(
                        'page existed and scraped (code=200), skip {}'.format(url))
                except elasticsearch.NotFoundError:
                    try:
                        args = {"proxy": None, "proxy_auth": None}

                        if self.cfg.proxy.enabled:
                            px = random.choice(proxies).split(':')
                            args = {
                                "proxy": f"http://{px[0]}:{px[1]}",
                                "proxy_auth": aiohttp.BasicAuth(px[2], px[3])
                            }

                        async with sess.get(url, **args) as resp:
                            log.info('page fetching {}'.format(url))
                            html = await resp.text()
                            log.info('page downloaded {}'.format(url))
                            self.parse(url, resp, html)
                            log.info('page scraped {}'.format(url))
                            await asyncio.sleep(3)

                    except aiohttp.ClientResponseError as e:
                        page = es.Page(
                            from_url=url,
                            resolved_url=str(e.request_info.real_url),
                            http_status=e.status,)
                        page.save()
                        log.info("fetch error & skiped: {}".format(e))
                        log.error(e)
                        self.error_urls.append(url)

                    except Exception as e:
                        log.info(
                            "scrape internal error & skiped: {}".format(e))
                        log.error(e)
                        self.error_urls.append(url)
                except Exception as e:
                    log.info("scrape internal error & skiped: {}".format(e))
                    log.error(e)
                    self.error_urls.append(url)
                finally:
                    queue.task_done()
