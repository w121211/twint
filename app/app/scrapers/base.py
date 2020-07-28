import abc
import asyncio
import datetime
import dataclasses
import json
import logging
import random
import urllib
from typing import Tuple, Iterable, List, Any, Optional, Union

import aiohttp
import elasticsearch
import pandas as pd
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent
from lxml import etree

from ..store import es
from .. import fetch

log = logging.getLogger(__name__)

ua = UserAgent(verify_ssl=False)


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
    def __init__(self, cfg: DictConfig, proxies: Optional[List[str]] = None):
        super().__init__()
        self.cfg = cfg
        self.error_urls = []
        self.proxies = proxies

        self.proxy_enabled = self.cfg.proxy.enabled
        self.sleep_for = self.cfg.run.sleep_for

    @abc.abstractmethod
    def startpoints(self, *args, **kwargs) -> Iterable[Any]:
        pass

    @classmethod
    @abc.abstractmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[es.Page]:
        pass

    def _is_scraped(self, url: str) -> bool:
        try:
            page = es.Page.get(id=url)
            if page.http_status != 200:
                raise elasticsearch.NotFoundError()
            log.info(
                'page existed and scraped (code=200), skip {}'.format(url))
            return True
        except elasticsearch.NotFoundError:
            return False
        except Exception as e:
            log.info("scrape internal error & skiped: {}".format(e))
            log.error(e)
            self.error_urls.append(url)
            return True

    async def worker(self, queue: asyncio.Queue):
        async with aiohttp.ClientSession(
                raise_for_status=True,
                headers=[("User-Agent", ua.random)],
                timeout=aiohttp.ClientTimeout(total=60)) as sess:

            while True:
                url = await queue.get()

                if self._is_scraped(url):
                    queue.task_done()
                    continue

                try:
                    args = {"proxy": None, "proxy_auth": None}

                    if self.proxy_enabled:
                        px = random.choice(self.proxies).split(':')
                        args = {
                            "proxy": f"http://{px[0]}:{px[1]}",
                            "proxy_auth": aiohttp.BasicAuth(px[2], px[3])
                        }
                    async with sess.get(url, **args) as resp:
                        log.info('page fetching {}'.format(url))
                        html = await resp.text()
                        log.info('page downloaded {}'.format(url))
                        parsed = self.parse(
                            url, str(resp.url), resp.status, html)
                        log.info('page scraped {}'.format(url))

                        await asyncio.sleep(self.sleep_for)

                except aiohttp.ClientResponseError as e:
                    p = es.Page.get_or_create(url)
                    p.update(
                        from_url=url,
                        resolved_url=str(e.request_info.real_url),
                        http_status=e.status,)
                    log.info("fetch error & skiped: {}".format(e))
                    log.error(e)
                    self.error_urls.append(url)

                except Exception as e:
                    log.info("scrape internal error & skiped: {}".format(e))
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


def _parse_tickers(node: etree.Element) -> List[TickerText]:
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


class BasePageScraper(BaseScraper):
    domain = 'example.com'  # for filtering page entries

    def startpoints(self, *args, **kwargs) -> Iterable[str]:
        # yield es.Page.scan_urls(self.domain)
        print(self.domain)
        for i, url in enumerate(es.Page.scan_urls(self.domain)):
            print(url)
            yield url
            # if i > 10:
            #     break

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[es.Page]:
        article = Article(resolved_url)
        article.set_html(html)
        article.parse()
        parsed = Parsed(
            keywords=article.meta_keywords,
            tickers=_parse_tickers(article.clean_top_node))
        p = es.Page(
            from_url=from_url,
            resolved_url=resolved_url,
            http_status=http_status,
            article_metadata=json.dumps(article.meta_data, ensure_ascii=False),
            article_published_at=article.publish_date,
            article_title=article.title,
            article_text=article.text,
            article_html=etree.tostring(
                article.clean_top_node, encoding='utf-8').decode('utf-8'),
            parsed=json.dumps(dataclasses.asdict(parsed), ensure_ascii=False),
            fetched_at=datetime.datetime.now(),)
        p.save()

        return [p]
