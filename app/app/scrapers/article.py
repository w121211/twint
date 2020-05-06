import asyncio
import datetime
import json
import logging
import sys
import time
from typing import Tuple, List
from dataclasses import dataclass

from newspaper import Article, ArticleException

# from app.store import es

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))


def parse(resp, html):
    article = Article(str(resp.url))
    article.set_html(html)
    article.parse()
    return article
    # try:
    #     article = Article(str(resp.url))
    #     article.set_html(html)
    #     article.parse()
    #     return article
    # except ArticleException as e:
    #     log.debug(e)
    # return None


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


async def scrape_page(entry: Entry, item: feedparser.FeedParserDict) -> Page:
    try:
        page = Page.get(url=item['link'])

        # add rss tikcer
        tks = page.rss_tickers.split(",")
        if entry.ticker is not None and entry.ticker not in tks:
            tks += [entry.ticker]
            query = Page.update(
                tickers=','.join(tks),
                updated_at=datetime.datetime.now()
            ).where(Page.id == page.id)
            query.execute()
        return page

    except Page.DoesNotExist:
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(item['link']) as resp:
                    html = await resp.text()
        except aiohttp.ClientError as e:
            log.error(e)
            html = None

        data = dict(
            url=item['link'],
            rss_title=item['title'],
            rss_summary=item['summary'],
            rss_published_at=item['published_at'],
            rss_tickers=entry['ticker'],
            raw=html,
            fetched_at=datetime.datetime.now())
        try:
            article = Article(item['link'])
            article.set_html(html)
            article.parse()
            data = dict(** data,
                        parsed_title=article.title,
                        parsed_text=article.text,
                        parsed_published_at=article.publish_date)
        except ArticleException as e:
            log.debug(e)
        finally:
            page = Page.create(**data)
        return page


def parse(html: str):
    pass


async def scrape_cnbc(tweet: Tweet, cfg: DictConfig):
    def _parse(html: str):
        data = dict(
            url=item['link'],
            rss_title=item['title'],
            rss_summary=item['summary'],
            rss_published_at=item['published_at'],
            rss_tickers=entry['ticker'],
            raw=html,
            fetched_at=datetime.datetime.now())
        try:
            article = Article(item['link'])
            article.set_html(html)
            article.parse()
            data = dict(** data,
                        parsed_title=article.title,
                        parsed_text=article.text,
                        parsed_published_at=article.publish_date)
        except ArticleException as e:
            log.debug(e)
        finally:
            page = Page.create(**data)

    for l in tweet.links:
        if len(page.search(url=l)) > 0:
            continue
        async with aiohttp.ClientSession() as sess:
            async with sess.get(l) as resp:
                page = _parse(await resp.text())

                # log.debug("{}: {}".format(rss.url, resp.status))
                # feed, freq, last_published_at = _parse_rss(await resp.text())
                # query = Rss.update(
                #     freq=freq,
                #     # last_published_at=last_published_at,
                #     fetched_at=datetime.datetime.now()
                # ).where(Rss.id == rss.id)
                # query.execute()
                # return feed


async def scrape_rss(rss: Rss, cfg: DictConfig) -> feedparser.FeedParserDict:
    def _parse_rss(html: str):
        feed = feedparser.parse(html)
        if len(feed['entries']) == 0:
            raise NoRssEntries("No entries found: {}".format(rss.url))

        stamps = [datetime.datetime.fromtimestamp(time.mktime(d['published_parsed']))
                  for d in feed['entries']]
        stamps.sort()
        if len(stamps) == 1:
            freq = cfg.scrape.fetch_rss_every_n_seconds
        else:
            freq = int(min((stamps[-1] - stamps[0]).total_seconds() / 3,
                           cfg.scrape.fetch_rss_every_n_seconds))
        last_published_at = stamps[-1]
        return feed, freq, last_published_at

    if rss.fetched_at is not None:
        secs_to_sleep = (rss.fetched_at + datetime.timedelta(seconds=rss.freq) -
                         datetime.datetime.now()).total_seconds()
        if secs_to_sleep > 0:
            log.debug("{} sleep for {} second".format(
                rss.url, int(secs_to_sleep)))
            await asyncio.sleep(secs_to_sleep)

    # fetch URL and parse RSS, TODO: add etag
    async with aiohttp.ClientSession() as sess:
        async with sess.get(rss.url) as resp:
            log.debug("{}: {}".format(rss.url, resp.status))
            feed, freq, last_published_at = _parse_rss(await resp.text())
            query = Rss.update(
                freq=freq,
                # last_published_at=last_published_at,
                fetched_at=datetime.datetime.now()
            ).where(Rss.id == rss.id)
            query.execute()
            return feed


async def _main(cfg: DictConfig) -> None:
    db.init('rss', user="postgres", password="postgrespassword", host="pg")
    # create_tables()
    seed()
    # db.close()
    # return

    # read from csv
    # url = "http://finance.yahoo.com/rss/headline?s=yhoo"
    # ticker = "yhoo"
    url = "http://finance.yahoo.com/rss/headline?s=msft"
    ticker = "msft"

    rss, _ = Rss.get_or_create(url=url, defaults={
        'ticker': ticker,
        'freq': cfg.scrape.fetch_rss_every_n_seconds})
    if rss.n_retries < 10:
        try:
            feed = await scrape_rss(rss, cfg)
            RssShot.create(
                url=url,
                ticker=ticker,
                entries=json.dumps(feed['entries']))
            # save_rss_item(db, entry, rss)
            # await asyncio.gather(*[scrape_page(entry, d) for d in rss['entries']])
        except (NoRssEntries, aiohttp.ClientError) as e:
            log.error(e)
            query = Rss.update(
                fetched_at=datetime.datetime.now(),
                n_retries=rss.n_retries + 1).where(Rss.id == rss.id)
            query.execute()

    # df = pd.read_csv(utils.to_absolute_path(cfg.csv.path))
    # async with aiohttp.ClientSession() as sess:
    # await scrape("http://finance.yahoo.com/rss/headline?s=yhoo", sess, db)
    # tasks = []
    # for _, r in df.iterrows():
    #     tasks.append(scrape(r['url'], sess, db, r['ticker']))
    #     break
    # tasks.append(scrape(
    #     r['url'], sess, db, r['ticker']
    # ))
    # await asyncio.gather(*tasks)

    db.close()


@hydra.main(config_path="config.yaml")
def main(cfg: DictConfig) -> None:
    asyncio.run(_main(cfg))


if __name__ == "__main__":
    main()
