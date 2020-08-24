import asyncio
import datetime
import json
import logging
import math
import random
import sys
import time
from typing import Tuple, List, Iterable

import aiohttp
import feedparser
import hydra
import pandas as pd
import elasticsearch
from hydra import utils
from omegaconf import DictConfig
from newspaper import Article, ArticleException
from fake_useragent import UserAgent

from ..store import es
from .base import BaseScraper, ua


log = logging.getLogger(__name__)


class NoRssEntries(Exception):
    pass


class RssScraper(BaseScraper):

    @classmethod
    def parse(cls, from_url: str, resolved_url: str, http_status: int, html: str, rss: es.Rss,
              fetch_rss_every_n_seconds=604800) -> List[es.Page]:
        feed = feedparser.parse(html)
        if len(feed['entries']) == 0:
            raise NoRssEntries(
                "No entries found: {}".format(str(resolved_url)))

        # update rss.freq based on entry's published time
        stamps = [datetime.datetime.fromtimestamp(time.mktime(d['published_parsed']))
                  for d in feed['entries']]
        stamps.sort()
        if len(stamps) == 1:
            freq = fetch_rss_every_n_seconds
        else:
            freq = int(min((stamps[-1] - stamps[0]).total_seconds() / 3,
                           fetch_rss_every_n_seconds))
        rss.freq = freq
        rss.n_retries = 0
        rss.fetched_at = datetime.datetime.now()
        rss.last_published_at = stamps[-1]
        rss.save()

        # create or update pages (as entrypoints, no-fetching)
        pages = []
        for e in feed["entries"]:
            from_url = e["link"]
            try:
                p = es.Page.get(id=from_url)
                urls = set(p.entry_urls)
                urls.add(rss.url)
                p.entry_urls = list(urls)

                if rss.ticker is not None:
                    tks = set(p.entry_tickers)
                    tks.add(rss.ticker)
                    p.entry_tickers = list(tks)
                p.save()
            except elasticsearch.NotFoundError:
                p = es.Page(
                    from_url=from_url,
                    entry_title=e["title"],
                    entry_summary=e["summary"] if "summary" in e else None,
                    entry_published_at=datetime.datetime.fromtimestamp(
                        time.mktime(e['published_parsed'])) if "published_parsed" in e else None,
                    entry_tickers=[
                        rss.ticker] if rss.ticker is not None else [],
                    entry_urls=[rss.url],
                )
                p.save()
            pages.append(p)
        return pages

    async def worker(self, queue: asyncio.Queue):
        async def fetch(url: str):
            async with aiohttp.ClientSession(
                    raise_for_status=True,
                    headers=[("User-Agent", ua.random)],
                    timeout=aiohttp.ClientTimeout(total=60)) as sess:
                proxy = random.choice(self.proxies).split(':')
                async with sess.get(
                        url,
                        proxy="http://{}:{}".format(proxy[0], proxy[1]),
                        proxy_auth=aiohttp.BasicAuth(proxy[2], proxy[3]),) as resp:
                    html = await resp.text()
                    log.info(f"Page downloaded: {url}")
                    await asyncio.sleep(3)
                    return html, resp

        while True:
            try:
                url, ticker = await queue.get()  # startpoint
                # Get or create Rss item
                try:
                    rss = es.Rss.get(id=url)
                except elasticsearch.NotFoundError:
                    rss = es.Rss(
                        url=url,
                        ticker=ticker,
                        n_retries=0,
                        freq=self.cfg.scraper.rss.fetch_rss_every_n_seconds)
                log.info(f"Start scraping: {url}")
                html, resp = await fetch(rss.url)
                self.parse(url, str(resp.url), resp.status, html, rss,
                           self.cfg.scraper.rss.fetch_rss_every_n_seconds)
                log.info("Page parsed & saved: {}".format(url))

            except (NoRssEntries, aiohttp.ClientResponseError) as e:
                log.info(f"Fetching error & skiped: {url}")
                log.error(type(e).__name__, e.args)
                self.error_urls.append(url)
                rss.fetched_at = datetime.datetime.now()
                rss.n_retries = rss.n_retries + 1
                rss.save()
            except Exception as e:
                log.info(f"Scraper error & skiped: {rss.url}")
                log.error(type(e).__name__, e.args)
                self.error_urls.append(url)
            finally:
                queue.task_done()

    def startpoints(self) -> Iterable[Tuple[str, str]]:
        df = pd.read_csv(utils.to_absolute_path(self.cfg.scraper.rss.entry))
        for _, r in df.iterrows():
            # if isinstance(r['ticker'], str):
            #     tk = r['ticker']
            # else:
            tk = r['ticker'] if isinstance(r['ticker'], str) else None
            try:
                rss = es.Rss.get(id=r['url'])
            except elasticsearch.NotFoundError:
                pass
            else:
                print(rss.to_dict())
                if rss.fetched_at is not None and rss.freq is not None and not self.cfg.scraper.rss.force_fetch:
                    secs_to_sleep = (rss.fetched_at +
                                     datetime.timedelta(seconds=rss.freq) -
                                     datetime.datetime.now()
                                     ).total_seconds()
                    if secs_to_sleep > 0:
                        log.info("sleep for {} second: {}".format(
                            int(secs_to_sleep), rss.url))
                        continue
            yield r['url'], tk
