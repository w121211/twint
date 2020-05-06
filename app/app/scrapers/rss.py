import asyncio
import datetime
import json
import logging
import sys
import time
from typing import Tuple, List

import aiohttp
import feedparser
import hydra
import pandas as pd
import elasticsearch
from hydra import utils
from omegaconf import DictConfig
from newspaper import Article, ArticleException

from ..store import es
from .. import fetch


log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))


class NoRssEntries(Exception):
    pass


# def save_rss_item(db: PostgresqlDatabase, entry: Entry, rss: feedparser.FeedParserDict) -> None:
#     with db.atomic():
#         for item in rss['entries']:
#             try:
#                 page = Page.get(url=item['link'])

#                 # add rss tikcer
#                 tks = page.rss_tickers.split(",")
#                 if entry.ticker is not None and entry.ticker not in tks:
#                     tks += [entry.ticker]
#                     query = Page.update(
#                         tickers=','.join(tks),
#                         updated_at=datetime.datetime.now()
#                     ).where(Page.id == page.id)
#                     query.execute()
#             except:
#                 data = dict(
#                     url=item['link'],
#                     rss_title=item['title'],
#                     rss_summary=item['summary'],
#                     rss_published_at=datetime.datetime.fromtimestamp(
#                         time.mktime(item['published_parsed'])),
#                     rss_tickers=entry.ticker,
#                     fetched_at=datetime.datetime.now())
#                 page = Page.create(**data)


# async def scrape_page(entry: Entry, item: feedparser.FeedParserDict) -> Page:
#     try:
#         page = Page.get(url=item['link'])

#         # add rss tikcer
#         tks = page.rss_tickers.split(",")
#         if entry.ticker is not None and entry.ticker not in tks:
#             tks += [entry.ticker]
#             query = Page.update(
#                 tickers=','.join(tks),
#                 updated_at=datetime.datetime.now()
#             ).where(Page.id == page.id)
#             query.execute()
#         return page

#     except Page.DoesNotExist:
#         try:
#             async with aiohttp.ClientSession() as sess:
#                 async with sess.get(item['link']) as resp:
#                     html = await resp.text()
#         except aiohttp.ClientError as e:
#             log.error(e)
#             html = None

#         data = dict(
#             url=item['link'],
#             rss_title=item['title'],
#             rss_summary=item['summary'],
#             rss_published_at=item['published_at'],
#             rss_tickers=entry['ticker'],
#             raw=html,
#             fetched_at=datetime.datetime.now())
#         try:
#             article = Article(item['link'])
#             article.set_html(html)
#             article.parse()
#             data = dict(** data,
#                         parsed_title=article.title,
#                         parsed_text=article.text,
#                         parsed_published_at=article.publish_date)
#         except ArticleException as e:
#             log.debug(e)
#         finally:
#             page = Page.create(**data)
#         return page


class RssScraper:
    def __init__(self, cfg: DictConfig):
        super().__init__()
        self.cfg = cfg

    def parse(self, resp: aiohttp.ClientResponse, html: str, rss: es.Rss):
        feed = feedparser.parse(html)
        if len(feed['entries']) == 0:
            raise NoRssEntries("No entries found: {}".format(str(resp.url)))

        # update rss.freq
        stamps = [datetime.datetime.fromtimestamp(time.mktime(d['published_parsed']))
                  for d in feed['entries']]
        stamps.sort()
        if len(stamps) == 1:
            freq = self.cfg.fetch_rss_every_n_seconds
        else:
            freq = int(min((stamps[-1] - stamps[0]).total_seconds() / 3,
                           self.cfg.fetch_rss_every_n_seconds))
        rss.update(
            freq=freq,
            n_retries=0,
            fetched_at=datetime.datetime.now(),
            last_published_at=stamps[-1],
        )

        # create or update pages (as entrypoints, no-fetching)
        for e in feed["entries"]:
            page = es.Page.get_or_create(e["link"])
            tickers = page.rss_tickers or []
            if rss.ticker is not None:
                tickers.append(rss.ticker)
            page.update(
                from_url=e["link"],
                rss_title=e["title"],
                rss_summary=e["summary"],
                rss_published_at=datetime.datetime.fromtimestamp(
                    time.mktime(e['published_parsed'])),
                rss_tickers=tickers,
            )
        # return pages

    async def scrape(self, url, ticker):
        rss = es.Rss.get_or_create(url, ticker)
        if rss.fetched_at is not None:
            secs_to_sleep = (rss.fetched_at +
                             datetime.timedelta(seconds=rss.freq) -
                             datetime.datetime.now()
                             ).total_seconds()
            if secs_to_sleep > 0:
                log.debug("{} sleep for {} second".format(
                    rss.url, int(secs_to_sleep)))
                await asyncio.sleep(secs_to_sleep)
        try:
            resp, html = await fetch.get(rss.url)
            self.parse(resp, html, rss)
        except (NoRssEntries, aiohttp.ClientError) as e:
            log.error(e)
            rss.update(fetched_at=datetime.datetime.now(),
                       n_retries=rss.n_retries + 1)

    def entrypoints(self) -> Tuple[str, str]:
        df = pd.read_csv(utils.to_absolute_path(self.cfg.csv.path))
        for _, r in df.iterrows():
            print(r['url'], r['ticker'])
            yield r['url'], r['ticker']

    async def run(self):
        es.init()

        _entrypoints = []
        for i, (url, ticker) in enumerate(self.entrypoints()):
            if i > 10:
                break
            _entrypoints.append((url, ticker))
        await asyncio.gather(*[self.scrape(url, ticker) for url, ticker in _entrypoints])
