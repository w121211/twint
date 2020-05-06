import asyncio
import datetime
import json
import logging
from typing import Tuple, List

import aiohttp
import pandas as pd
import elasticsearch
from omegaconf import DictConfig
from fake_useragent import UserAgent

from ..store import es
from .. import fetch

log = logging.getLogger(__name__)


class CnyestPageScraper:
    pass


class CnyesApiScraper:
    def __init__(self, cfg: DictConfig = None):
        super().__init__()
        self.cfg = cfg
        self.error_urls = []

    def parse(self, resp: aiohttp.ClientResponse, data: dict):
        new_urls = []
        if data["items"]['next_page_url'] is not None:
            new_urls.append(
                f'https://news.cnyes.com{data["items"]["next_page_url"]}')
        for e in data['items']['data']:
            url = f"https://news.cnyes.com/news/id/{e['newsId']}"
            try:
                es.Page.get(id=url)
            except elasticsearch.NotFoundError:
                page = es.Page(
                    from_url=url,
                    entry_title=e['title'],
                    entry_summary=e['summary'],
                    entry_published_at=datetime.datetime.fromtimestamp(
                        e['publishAt']),
                    entry_tickers=[
                        x for x in map(lambda x: x["symbol"], e['market'])],
                    entry_meta=json.dumps(e),)
                page.save()
        return new_urls

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
                        j = await resp.json()
                        found_urls = self.parse(resp, j)
                        for u in found_urls:
                            queue.put_nowait(u)
                        log.info(f'scraped: {url}')
                except aiohttp.ClientError as e:
                    log.error(e)
                    self.error_urls.append(url)
                finally:
                    queue.task_done()

    async def run(self,
                  start=datetime.datetime.now() - datetime.timedelta(days=30),
                  until=datetime.datetime.now(),
                  n_workers=1):
        queue = asyncio.Queue()
        es.init()

        for url in self.startpoints(start, until):
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
