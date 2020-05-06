from __future__ import annotations
import datetime
import json
# from elasticsearch_dsl import Search
# from elasticsearch import Elasticsearch
import elasticsearch
from elasticsearch_dsl import connections, Document, Date, Keyword, Q, Search, Text, Range, Integer


class Rss(Document):
    url = Keyword(required=True)
    ticker = Keyword()
    freq = Integer()  # seconds (default: 2 days)
    n_retries = Integer()
    fetched_at = Date()
    last_published_at = Date()

    class Index:
        name = "news_rss"
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

    def save(self, **kwargs):
        self.meta.id = self.url
        self.n_retries = self.n_retries or 0
        self.freq = self.freq or 2*24*60*60
        # self.created_at = datetime.datetime.now()
        return super().save(**kwargs)

    @classmethod
    def get_or_create(cls, url, ticker=None) -> Rss:
        try:
            rss = cls.get(id=url)
        except elasticsearch.NotFoundError:
            rss = cls(url=url, ticker=ticker)
            rss.save()
        return rss

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
    entry_title = Text()
    entry_summary = Text()
    entry_published_at = Date()
    entry_tickers = Keyword()  # array
    entry_meta = Text()
    article_metadata = Text()
    article_published_at = Date()
    article_title = Text()
    article_text = Text()
    article_html = Text()
    parsed = Text()  # JSON for flexible data format
    created_at = Date(required=True)
    fetched_at = Date()  # null for not-yet-fetched

    class Index:
        name = "news_page"
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

    def save(self, **kwargs):
        self.meta.id = self.from_url
        self.created_at = datetime.datetime.now()
        return super().save(**kwargs)

    # @classmethod
    # def is_existed(cls, src_url: str) -> bool:
    #     s = cls.search()
    #     s.query = Q({"match": {"src_url": src_url}})
    #     resp = s.execute()
    #     if resp.hits.total.value > 0:
    #         return True
    #     return False

    @classmethod
    def is_fetched(cls):
        pass

    @classmethod
    def get_or_create(cls, url) -> Page:
        try:
            page = cls.get(id=url)
        except elasticsearch.NotFoundError:
            page = cls(from_url=url)
            page.save()
        return page


def seed():
    connections.create_connection(hosts=['es:9200'])
    Rss.init()
    Page.init()

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
    client = elasticsearch.Elasticsearch(['es:9200'])
    s = Search(using=client, index="twinttweets").query(q)
    return s.scan()


def init():
    connections.create_connection(hosts=['es:9200'])
    Rss.init()
    Page.init()


if __name__ == '__main__':
    seed()
