import pytest
# import aiohttp
import requests
import requests_cache
from newspaper import Article
from lxml import etree

from app.scrapers import cnbc

# async def fetch_and_parse():
#     url = 'https://www.cnbc.com/2020/04/19/why-big-techs-coronavirus-goodwill-wont-help-in-antitrust-probes.html'
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url) as resp:
#             article = Article(str(resp.url))
#             article.set_html(await resp.text())
#             article.parse()
#             return article, resp


# @pytest.fixture
# async def aio_article(request):
#     # v = request.config.cache.get("fetch/article", None)
#     # if v is None:
#     #     v = await fetch_and_parse()
#     #     request.config.cache.set("fetch/article", v)
#     # return v
#     url = 'https://www.cnbc.com/2020/04/19/why-big-techs-coronavirus-goodwill-wont-help-in-antitrust-probes.html'
#     async with aiohttp.ClientSession() as session:
#         async with session.get(url) as resp:
#             article = Article(str(resp.url))
#             article.set_html(await resp.text())
#             article.parse()
#             return article, resp

@pytest.fixture
def article():
    url = 'https://www.cnbc.com/2020/04/19/why-big-techs-coronavirus-goodwill-wont-help-in-antitrust-probes.html'
    requests_cache.install_cache()
    r = requests.get(url)
    article = Article(url)
    article.set_html(r.text)
    article.parse()
    # assert r.from_cache == True

    return article


@pytest.fixture
def cnyes_api_data():
    url = 'https://news.cnyes.com/api/v3/news/category/headline?startAt=1587488400&endAt=1588438799&limit=30'
    r = requests.get(url)
    return r.json()

# @pytest.mark.asyncio


def _test_function(article):
    from app.scrapers.cnbc import _Ticker

    tickers = cnbc._parse_tickers(article.clean_top_node)
    tickers.sort(key=lambda x: x.text)

    assert tickers[0].text == "Recent actions by both agencies back up their claims. At the end of March, the DOJ Antitrust Division announced it would require military technology companies Raytheon and United Technologies Corporation to divest parts of their businesses to proceed with their proposed merger. On April 1, the FTC announced its lawsuit to unwind tobacco company Altria's $12.8 billion investment in Juul, the e-cigarette company accused of luring adolescents to its products with its marketing."
    assert tickers[0].labels == [('Raytheon ', 'UTX'), ('Altria', 'MO')]


def test_cnyes_api(cnyes_api_data):
    # print(cnyes_api_data)
    # assert cnyes_api_data, {"aaa"}
    # d = cnyes_api_data['items']['data'][0]
    assert cnyes_api_data['items']['next_page_url'] == "/api/v3/news/category/headline?limit=30&startAt=1587484800&endAt=1588521599&page=2"
    # assert cnyes_api_data['items']['data'][0] == {}
