import asyncio
import datetime
import json
import logging
import sys
import dataclasses
import urllib
from typing import Tuple, List, Iterable

import aiohttp
from lxml import etree
from newspaper import Article
from omegaconf import DictConfig

from .. import fetch
from ..store import es

log = logging.getLogger(__name__)
# log.addHandler(logging.StreamHandler(sys.stdout))


@dataclasses.dataclass
class _Ticker:
    '''<p>...some text...<a href='$AAA'>...some anchor text</a>...<a>....</a>...</p>'''
    text: str
    # (anchor_text, ticker)
    labels: List[Tuple[str, str]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class Parsed:
    keywords: List[str] = dataclasses.field(default_factory=list)
    tickers: List[_Ticker] = dataclasses.field(default_factory=list)


def _parse_tickers(node: etree.Element) -> List[_Ticker]:
    tickers = []

    # find all <a> and de-duplicate their parents
    for p in set([e.getparent() for e in node.cssselect('a')]):
        text = etree.tostring(
            p, method='text', encoding='utf-8').decode('utf-8').strip()
        tk = _Ticker(text)

        for a in p.cssselect('a'):
            href = a.get('href')
            queries = dict(urllib.parse.parse_qsl(
                urllib.parse.urlsplit(href).query))
            try:
                tk.labels.append((a.text, queries['symbol']))
            except KeyError:
                continue
        if len(tk.labels) > 0:
            tickers.append(tk)

    return tickers


def parse(resp, html) -> es.Page:
    article = Article(str(resp.url))
    article.set_html(html)
    article.parse()
    parsed = Parsed(keywords=article.meta_keywords,
                    tickers=_parse_tickers(article.clean_top_node))
    return es.Page(
        resolved_url=str(resp.url),
        status=resp.status,
        article_published_at=article.publish_date,
        article_title=article.title,
        article_text=article.text,
        article_metadata=json.dumps(article.meta_data),
        article_html=etree.tostring(
            article.clean_top_node, encoding='utf-8').decode('utf-8'),
        parsed=json.dumps(dataclasses.asdict(parsed))
    )


def source() -> Iterable[str]:
    for hit in es.scan_twint('ReutersBiz'):
        for u in hit.urls:
            yield u


async def run(cfg: DictConfig):
    es.init()

    async def _scrape(i, url):
        if es.Page.is_existed(src_url=url):
            log.info('{} page existed, skip {}'.format(i, url))
            return
        try:
            resp, html = await fetch.get(url)
            page = parse(resp, html)
            page.src_url = url
            page.save()
            log.info('{} page saved {}'.format(i, url))
        except aiohttp.ClientResponseError as e:
            page = es.Page(
                src_url=url,
                resolved_url=str(e.request_info.real_url),
                status=e.status,)
            page.save()
            log.info("{} skip: {}".format(i, e))

    # for i, url in enumerate(source()):
    #     if i >= 3:
    #         break
    #     if es.Page.is_existed(src_url=url):
    #         log.debug('{} skip {}'.format(i, url))
    #         continue
    #     log.debug('{} fetch {}'.format(i, url))
    #     resp, html = await fetch.fetch_url(url)
    #     page = parse(resp, html)
    #     page.src_url = url
    #     page.save()

    urls = []
    for i, url in enumerate(source()):
        if i > 10:
            break
        urls.append(url)
    await asyncio.gather(*[_scrape(i, url) for i, url in enumerate(urls)])
    # await asyncio.gather(*[_scrape(i, url) for i, url in enumerate(source())])


# def runner(source: Callable[[], Iterable[str]],
#            fetch: Callable[[str], Tuple[aiohttp.ClientResponse, str]],
#            parse: Callable[[aiohttp.ClientResponse, str], dict],
#            store):
#     for url in source():
#         resp, html = fetch(url)
#         parsed = parse(resp, html)
#         stored = store(parsed)
