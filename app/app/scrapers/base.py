import abc
import asyncio
import datetime
import dataclasses
import json
import logging
import random
import urllib
from typing import Tuple, Iterable, List, Any, Optional, Union, Callable, Type

import aiohttp
import elasticsearch
import hydra
import msgpack
import pandas as pd
import redis
from newspaper import Article
from omegaconf import DictConfig
from fake_useragent import UserAgent
from lxml import etree

from ..store import es, model

log = logging.getLogger(__name__)
ua = UserAgent(verify_ssl=False)


class BaseScraper:
    def __init__(self, cfg: DictConfig, proxies: Optional[List[str]] = None):
        super().__init__()
        self.cfg = cfg
        self.error_urls = []
        self.proxies = proxies
        self.redis = redis.Redis(host='redis', port=6379, db=0)
        self.proxy_enabled: bool = self.cfg.proxy.enabled
        self.sleep_for: int = self.cfg.run.sleep_for
        self.max_startpoints: int = self.cfg.run.max_startpoints
        self.startpoints_csv: str = self.cfg.run.startpoints_csv

    @classmethod
    @abc.abstractmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str, *args, **kwargs) -> List[model.Page]:
        pass

    @classmethod
    def save(cls, pages: List[model.Page]) -> None:
        for p in pages:
            data = es.Page.jsondumps(dataclasses.asdict(p))
            try:
                doc = es.Page.get(id=p.from_url)
                doc.update(**data)
            except elasticsearch.NotFoundError:
                doc = es.Page(**data)
                doc.save()

    def _is_scraped(self, url: str) -> bool:
        try:
            page = es.Page.get(id=url)
            if page.http_status != 200:
                return False
            log.info(f"page existed and scraped (code=200), skip {url}")
            return True
        except elasticsearch.NotFoundError:
            return False
        except Exception as e:
            log.info(f"scraper unexpected error & skiped: {e}")
            log.error(e)
            self.error_urls.append(url)
            return True

    async def worker(self, queue: asyncio.Queue) -> None:
        # Set `raise_for_status=True` to catch any unsuccessful fetch
        # See: https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse.raise_for_status
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
                    # Is cached?
                    cache = self.redis.get(url)
                    if cache is not None:
                        fetched = msgpack.unpackb(cache, raw=False)

                    # No cache found, start fetching
                    else:
                        args = {"proxy": None, "proxy_auth": None}
                        if self.proxy_enabled:
                            px = random.choice(self.proxies).split(':')
                            args = {
                                "proxy": f"http://{px[0]}:{px[1]}",
                                "proxy_auth": aiohttp.BasicAuth(px[2], px[3])
                            }
                        async with sess.get(url, **args) as resp:
                            log.info('page fetching {}'.format(url))
                            # Cache html & status
                            fetched = {
                                "resolved_url": str(resp.url),
                                "http_status": resp.status,
                                "html": await resp.text(),
                            }
                            self.redis.set(url, msgpack.packb(
                                fetched, use_bin_type=True))
                            log.info('page downloaded {}'.format(url))

                            # Throttle
                            await asyncio.sleep(self.sleep_for)

                    # Parsing
                    parsed = self.parse(url, **fetched)
                    self.save(parsed)
                    log.info('page scraped {}'.format(url))

                except aiohttp.ClientResponseError as e:
                    try:
                        p = es.Page.get(id=url)
                        p.update(
                            resolved_url=str(e.request_info.real_url),
                            http_status=e.status,)
                    except elasticsearch.NotFoundError:
                        p = es.Page(
                            from_url=url,
                            resolved_url=str(e.request_info.real_url),
                            http_status=e.status,)
                        p.save()
                    log.info("fetch error & skiped: {}".format(e))
                    log.error(e)
                    self.error_urls.append(url)

                except Exception as e:
                    log.info("scrape internal error & skiped: {}".format(e))
                    log.error(e)
                    self.error_urls.append(url)

                finally:
                    queue.task_done()

    def startpoints(self, *args, **kwargs) -> Iterable[str]:
        data = pd.read_csv(hydra.utils.to_absolute_path(self.startpoints_csv))
        return list(data["url"])

    async def _run(self, n_workers=1, *args, **kwargs) -> None:
        queue = asyncio.Queue()

        for p in self.startpoints(*args, **kwargs):
            queue.put_nowait(p)
        tasks = [asyncio.create_task(self.worker(queue))
                 for _ in range(n_workers)]

        await queue.join()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        df = pd.DataFrame({'url': self.error_urls})
        df.to_csv("./error_urls.csv", index=False)

        log.info("all jobs done.")

    async def run(self, n_workers=1, loop_every=None, *args, **kwargs) -> None:
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
    domain = 'example.basepage.com'  # for filtering page entries

    def startpoints(self, *args, **kwargs) -> Iterable[str]:
        try:
            return super().startpoints()
        except:
            for i, url in enumerate(es.Page.scan_urls(self.domain)):
                if self.max_startpoints > 0 and i > self.max_startpoints:
                    break
                yield url

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str) -> List[model.Page]:
        a = Article(resolved_url)
        a.set_html(html)
        a.parse()
        parsed = model.Parsed(keywords=a.meta_keywords, tickers=[])
        return [
            model.Page(
                from_url=from_url,
                resolved_url=resolved_url,
                http_status=http_status,
                article_metadata=a.meta_data,
                article_published_at=a.publish_date,
                article_title=a.title,
                article_text=a.text,
                parsed=parsed,
                fetched_at=datetime.datetime.now(),
            )
        ]
