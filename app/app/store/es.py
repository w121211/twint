"""
Setup (once only)
$ python es.py
"""
from __future__ import annotations
import collections
import datetime
import json
from typing import Iterable, List, Tuple, Optional

import elasticsearch
from elasticsearch_dsl import connections, Document, Date, Keyword, Q, Search, Text, Range, Integer


APP_ERROR_HTTP_STATUS = 9999  # app內部錯誤時使用

RSS_ALIAS = "scraper-rss"
RSS_PATTERN = RSS_ALIAS + '-*'
PAGE_ALIAS = "scraper-page"
PAGE_PATTERN = PAGE_ALIAS + '-*'


class Rss(Document):
    url = Keyword(required=True)
    ticker = Keyword()
    freq = Integer()  # seconds (default: 2 days)
    n_retries = Integer()
    fetched_at = Date()
    last_published_at = Date()

    class Index:
        name = RSS_ALIAS
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

    def save(self, **kwargs):
        if 'id' not in self.meta:
            self.meta.id = self.url
        self.n_retries = self.n_retries or 0
        # self.created_at = datetime.datetime.now()
        return super().save(**kwargs)

    # @classmethod
    # def get_or_create(cls, url, ticker=None) -> Rss:
    #     try:
    #         rss = cls.get(id=url)
    #     except elasticsearch.NotFoundError:
    #         # print(type(ticker))
    #         rss = cls(url=url, ticker=ticker)
    #         rss.save()
    #     return rss

    # @classmethod
    # def is_existed(cls, src_url: str) -> bool:
    #     s = cls.search()
    #     s.query = Q({"match": {"src_url": src_url}})
    #     resp = s.execute()
    #     if resp.hits.total.value > 0:
    #         return True
    #     return False


class Page(Document):
    from_url = Keyword(required=True)
    resolved_url = Keyword()
    http_status = Integer()
    entry_urls = Keyword(index=False, multi=True)  # array
    entry_tickers = Keyword(index=False, multi=True)  # array
    entry_title = Text(index=False)
    entry_summary = Text(index=False)
    entry_published_at = Date()
    entry_meta = Text(index=False)  # JSON
    article_metadata = Text(index=False)  # JSON
    article_published_at = Date()
    article_title = Text(index=False)
    article_text = Text(index=False)
    article_summary = Text(index=False)  # meta.description
    article_html = Text(index=False)  # clean_top_node, depcreated
    parsed = Text(index=False)  # JSON for flexible data format
    created_at = Date(required=True)
    fetched_at = Date()  # null for not-yet-fetched

    class Index:
        name = PAGE_ALIAS
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

    @classmethod
    def jsondumps(cls, d: dict):
        def dump(o: Optional[dict]):
            if o is None:
                return None
            if isinstance(o, dict):
                return json.dumps(o, ensure_ascii=False)
            else:
                raise Exception("Input must be a dict")

        for k, v in d.items():
            if k in ("parsed", "entry_meta", "article_metadata"):
                d[k] = dump(v)
        for k, v in dict(d).items():
            if v is None:
                del d[k]
        return d

    def save(self, **kwargs):
        if 'id' not in self.meta:
            self.meta.id = self.from_url
        if self.created_at is None:
            self.created_at = datetime.datetime.now()
        return super().save(**kwargs)

    # @classmethod
    # def get_or_create(cls, from_url: str) -> Page:
    #     try:
    #         p = cls.get(id=from_url)
    #     except elasticsearch.NotFoundError:
    #         p = cls(from_url=from_url)
    #     return p

    @classmethod
    def scan_urls(cls, domain: Optional[str] = None) -> Iterable[str]:
        if isinstance(domain, str) and '/' in domain:
            raise Exception(
                "Domain cannot include `/`, because elasticsearch wildcard doen't work with `/`")
        if domain is None:
            q = ~Q("term", http_status=200)
        else:
            q = Q('wildcard', from_url=domain.lower()) & \
                ~Q("term", http_status=200)
        for page in cls.search().filter(q).scan():
            yield page.from_url


def seed():
    # import pprint
    # pprint.pprint(Page().to_dict(), indent=2)

    # parsed_symbols = [
    #     ["some symbol outer text go here", "symbol direct text", "SYMBOL"],
    #     ["some symbol outer text go here", "symbol direct text", "SYMBOL"],
    # ]

    # Page(src_url="aaa.com",
    #      article_url="aaa.com",
    #      article_metadata="{title: 'this is some json string'}",
    #      parsed_symbols=json.dumps(parsed_symbols)
    #      ).save()

    Page._index.refresh()


def scan_twint(user: str,
               since="2000-01-01 00:00:00",
               until="2025-01-01 00:00:00"):
    q = Q({
        "range": {
            "date": {
                "gte": since,
                "lt": until,
            }
        }
    })
    es = connections.get_connection()
    s = Search(using=es, index="twinttweets").query(
        q).filter("terms", username=[user])
    return s.scan()


def query_twint():
    es = connections.get_connection()
    s = Search(using=es, index="twinttweets").query(
        q).filter("terms", username=[user])
    # return s.scan()


def query_page(from_url="", start="", until=""):
    q = Q("wildcard", from_url="kmdj") & \
        Q("range", created_at={"gte": "2020-08-05", "lt": None}) & \
        ~Q("term", http_status=200)
    connect()
    s = Page.search().filter(q)
    resp = s.execute()

    print(resp.hits.total)
    for h in resp[0:3]:
        print(h.to_dict())

# ---------------
# setup functions
# ---------------


def create_patterned_index(alias: str, pattern: str, create_alias=True) -> None:
    """Run only one time to setup"""
    name = pattern.replace(
        '*', datetime.datetime.now().strftime('%Y%m%d%H%M'))
    # create_index
    es = connections.get_connection()
    es.indices.create(index=name)
    if create_alias:
        es.indices.update_aliases(body={
            'actions': [
                {"remove": {"alias": alias, "index": pattern}},
                {"add": {"alias": alias, "index": name}},
            ]
        })


def migrate(src, dest):
    es = connections.get_connection()
    es.reindex(body={"source": {"index": src}, "dest": {"index": dest}})
    es.indices.refresh(index=dest)


def connect():
    connections.create_connection(hosts=['es:9200'])
    # Rss.init()
    # Page.init()


def setup(move: bool = False):
    create_patterned_index(PAGE_ALIAS, PAGE_PATTERN)
    create_patterned_index(RSS_ALIAS, RSS_PATTERN)
    if move:
        migrate("news_page", PAGE_ALIAS)
        migrate("news_rss", RSS_ALIAS)


if __name__ == '__main__':
    connect()
    # seed()
    setup(move=False)
    # migrate("news_rss", RSS_ALIAS)
