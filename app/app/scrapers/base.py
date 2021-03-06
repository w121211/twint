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
        self.throttle: int = self.cfg.run.throttle
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
            log.info(f"Page existed and scraped (code=200), skip {url}")
            return True
        except elasticsearch.NotFoundError:
            return False

    async def worker(self, queue: asyncio.Queue) -> None:
        async def fetch(url: str):
            # Set `raise_for_status=True` to catch any unsuccessful fetch
            # See: https://docs.aiohttp.org/en/stable/client_reference.html#aiohttp.ClientResponse.raise_for_status
            async with aiohttp.ClientSession(
                    raise_for_status=True,
                    headers=[("User-Agent", ua.random)],
                    timeout=aiohttp.ClientTimeout(total=30)) as sess:

                # Proxy setup
                args = {"proxy": None, "proxy_auth": None}
                if self.proxy_enabled:
                    px = random.choice(self.proxies).split(':')
                    args = {
                        "proxy": f"http://{px[0]}:{px[1]}",
                        "proxy_auth": aiohttp.BasicAuth(px[2], px[3])
                    }

                async with sess.get(url, **args) as resp:
                    html = await resp.text("big5hkscs") if resp.charset == "big5" else await resp.text()
                    log.info(f"Page downloaded: {url}")
                    await asyncio.sleep(self.throttle)
                    return html, resp

        while True:
            try:
                # Start scraping
                url = await queue.get()
                if self._is_scraped(url):
                    log.info(f"{url} is scraped, skip")
                    queue.task_done()
                    continue

                # Is cached?
                cache = self.redis.get(url)
                if cache is not None:
                    log.info(f"Page cached {url}, use cache")
                    fetched = msgpack.unpackb(cache, raw=False)
                else:
                    html, resp = await fetch(url)
                    fetched = {
                        "resolved_url": str(resp.url),
                        "http_status": resp.status,
                        "html": html,
                    }
                    self.redis.set(url, msgpack.packb(
                        fetched, use_bin_type=True))

                # Parsing
                self.save(self.parse(url, **fetched))
                log.info(f'Page scraped {url}')

            except aiohttp.ClientResponseError as e:
                try:
                    p = es.Page.get(id=url)
                    n_retries = p.n_retries + 1
                    p.update(
                        resolved_url=str(e.request_info.real_url),
                        http_status=e.status,
                        n_retries=n_retries)
                except elasticsearch.NotFoundError:
                    p = es.Page(
                        from_url=url,
                        resolved_url=str(e.request_info.real_url),
                        http_status=e.status,
                        n_retries=1,)
                    p.save()
                except Exception as e:
                    log.info(f"Internal error, skip: {e}")
                    log.error(type(e).__name__, e.args)
                    self.error_urls.append(url)

            except Exception as e:
                log.info(f"Internal error, skip: {e}")
                log.error(type(e).__name__, e.args)
                self.error_urls.append(url)

            finally:
                queue.task_done()

    def startpoints(self, *args, **kwargs) -> Iterable[str]:
        data = pd.read_csv(hydra.utils.to_absolute_path(self.startpoints_csv))
        for u in data["url"]:
            yield u

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
            for u in super().startpoints():
                yield u
        except Exception as e:
            print(
                f"Possibly not given urls.csv, switch to custom startpoints method: {e}")
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
