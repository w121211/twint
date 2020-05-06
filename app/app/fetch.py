from typing import Tuple

import aiohttp
from fake_useragent import UserAgent

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


async def get(url: str) -> Tuple[aiohttp.ClientResponse, str]:
    ua = UserAgent(verify_ssl=False, use_cache_server=False).random
    async with aiohttp.ClientSession(raise_for_status=True, headers=[("User-Agent", ua)]) as sess:
        async with sess.get(url) as resp:
            html = await resp.text()
            return resp, html


async def get_json(url: str, sess: aiohttp.ClientSession) -> Tuple[aiohttp.ClientResponse, str]:
    # ua = UserAgent(verify_ssl=False, use_cache_server=False).random
    # async with aiohttp.ClientSession(raise_for_status=True, headers=[("User-Agent", ua)]) as sess:
    async with sess.get(url) as resp:
        j = await resp.json()
        return resp, j
