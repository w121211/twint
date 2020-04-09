
import asyncio
import datetime
from typing import Tuple, List

import aiohttp
import feedparser
import hydra
from hydra import utils
from omegaconf import DictConfig
import pandas as pd
import psycopg2

from .db import Client, Rss, Article


async def fetch(session, url: str):
    await asyncio.sleep(1)
    return "html<{}>".format(url)


def _run():
    # load urls
    df = pd.DataFrame({
        "url": ["aaa", "bbb", "ccc"],
        "freq": [10, 100, 1000],
        "fetched_at": [0, 10, 20]
    })
    for _, r in df.iterrows():
        # print(row['url'], row['c2'])
        # html, fetched_at = await fetch(r['url'], r['freq'], r['fetched_at'])
        pass


def parse(html: str):
    return "parsed <{}>".format(html)


def save(doc):
    print("saved: {}".format(doc))


def query_rss(url):
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM rss WHERE url=%s", (url,))
            res = cur.fetchall()
    return res

# def inert_rss():
#     with conn:
#         with conn.cursor() as cur:
#             cur.execute("SELECT * FROM rss WHERE url=%s", (url,))
#             res = cur.fetchall()


async def scrape_articles(session, urls):
    async def scrape(url):
        # is fetched?
        if es.has(clean(url)):
            pass
        return

        async with session.get(url) as resp:
            print(resp.status)
            html = await resp.text()
            article.set_html(html)
            article.parse()
            es.index(article)

    asyncio.gather(*[scrape(url) for url in urls])


def last_published(ts: List[datetime.datetime]):
    pass


async def scrape(url: str, sess: aiohttp.ClientSession, db: Client, ticker=None) -> None:
    rss = db.get_rss(url)
    if rss is not None:
        # fetch now or until when
        secs = rss.fetched_at + rss.freq - datetime.datetime.now()
        await asyncio.sleep(secs)
    # else:
    #     # create new rss entity

    async with sess.get(url) as resp:
        print(resp.status)
        t = await resp.text()
        d = feedparser.parse(t)


#         rss = parse_rss(html)

    res = query_rss(url)
    try:
        d = res[0]
#         secs = d["fetched_at"] + d["freq"] - now
        secs = d[3]-d[2]-now
        await asyncio.sleep(secs)
    except IndexError:
        # create new rss entity
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO rss (url, ticker, fetched_at) VALUES (%s, %s, %s) RETURNING id;",
                    (url, "YHOO", datetime.datetime.now())
                )
#     async with session.get(url) as resp:
#         print(resp.status)
#         html = await resp.text()
# #         rss = parse_rss(html)

#         with conn:
#             with conn.cursor() as cur:
#                 cur.execute(
#                     "UPDATE rss SET fetched_at=now() WHERE ID=%s;", (d[0]))
#                 cur.execute(
#                     "SELECT * FROM feeds ORDER BY published_at DESC LIMIT 1")
#                 res = cur.fetchall()

#                 for e in rss['entries']:
#                     pass
#                     cur.execute("INSERT INTO feeds (url, title, content, created_at) VALUES (%s, %s, %s) RETURNING id;",
#                                 (d[0]))
#         rss = parse_rss(html)
#         save_rss(rss)
#         await asyncio.gather(*[scrape_article(e["url"]) for e in rss.entries])


async def _main(cfg: DictConfig) -> None:
    df = pd.read_csv(utils.to_absolute_path(cfg.csv.path))
    # try:
    #     Client.drop_db(cfg.db)
    #     Client.create_db(cfg.db)
    # except:
    #     pass
    db = Client(cfg.db)
    # print(db.insert_rss(Rss(url="aaa2.com")))
    print(db.get_rss(url="aaaa.com"))

    # async with aiohttp.ClientSession() as sess:
    #     tasks = []
    #     for _, r in df.iterrows():
    #         tasks.append(scrape(r['url'], sess, conn, r['ticker']))
    #     await asyncio.gather(*tasks)

    db.close()


@hydra.main(config_path="config.yaml")
def main(cfg: DictConfig) -> None:
    asyncio.run(_main(cfg))


if __name__ == "__main__":
    main()
