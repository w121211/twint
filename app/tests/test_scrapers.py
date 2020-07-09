import datetime
import pytest
import requests
import requests_cache
from newspaper import Article
from lxml import etree
from hydra.experimental import compose, initialize

# from app import scrapers
from app.scrapers.base import TickerText
from app.scrapers import cnyes
from app.scrapers import cnbc
from app.store import es

"""
python -m pytest tests/test_scrapers.py::test_cnyes_page_parse_tickers -vv
"""

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
def expected(request):
    return request.param


@pytest.fixture(scope="module")
def page_html(request):
    requests_cache.install_cache()
    r = requests.get(request.param)
    return r.text


@pytest.fixture
def cnyes_api_json():
    # url = 'https://news.cnyes.com/api/v3/news/category/headline?startAt=1587488400&endAt=1588438799&limit=30'
    url = 'https://news.cnyes.com/api/v3/news/category/headline?startAt=1588636800&endAt=1589047393&limit=100'
    requests_cache.install_cache()
    r = requests.get(url)
    return r.json()


@pytest.fixture
def cnyes_page_html():
    url = 'https://news.cnyes.com/news/id/4474677'
    requests_cache.install_cache()
    r = requests.get(url)
    return r.text


def _test_function(article):
    from app.scrapers.cnbc import _Ticker

    tickers = cnbc._parse_tickers(article.clean_top_node)
    tickers.sort(key=lambda x: x.text)

    assert tickers[0].text == "Recent actions by both agencies back up their claims. At the end of March, the DOJ Antitrust Division announced it would require military technology companies Raytheon and United Technologies Corporation to divest parts of their businesses to proceed with their proposed merger. On April 1, the FTC announced its lawsuit to unwind tobacco company Altria's $12.8 billion investment in Juul, the e-cigarette company accused of luring adolescents to its products with its marketing."
    assert tickers[0].labels == [('Raytheon ', 'UTX'), ('Altria', 'MO')]


def test_cnyes_api(cnyes_api_json):
    es.init()
    # urls = ['https://news.cnyes.com/api/v3/news/category/headline?startAt=1588636800&endAt=1589047393&limit=100']
    scp = CnyesApiScraper()
    pages, urls = scp.parse(None, cnyes_api_json)
    assert pages[0].__dict__ == {
        '_d_': {'entry_meta': '{"newsId": 4474691, "title": '
                '"\\u3008\\u5831\\u7a05\\u6559\\u6230\\u624b\\u518a\\u3009\\u80a1\\u5229\\u6240\\u5f97\\u300c\\u4e00\\u8d77\\u7a05\\u300d\\u9084\\u662f\\u300c\\u5206\\u958b\\u7a05\\u300d\\u5212\\u7b97\\uff1f\\u95dc\\u9375\\u770b\\u9019\\u500b\\u6578\\u64da", '
                '"hasCoverPhoto": 1, "isIndex": 1, "summary": '
                '"\\u7a05\\u52d9\\u5c08\\u5bb6\\u63d0\\u9192\\uff0c\\u80a1\\u5229\\u6240\\u5f97\\u7a05\\u7533\\u5831\\u4e3b\\u8981\\u6709\\u300c\\u4f75\\u5165\\u6240\\u5f97\\u7533\\u5831\\u300d\\u8207\\u300c\\u5206\\u958b\\u8a08\\u7a05\\u300d\\u5169\\u7a2e\\uff0c\\u6c11\\u773e\\u53ef\\u64c7\\u512a\\u7533\\u5831\\uff0c\\u81f3\\u65bc\\u600e\\u9ebc\\u9078\\u624d\\u5212\\u7b97\\u95dc\\u9375\\u5728\\u65bc\\u5168\\u5e74\\u80a1\\u5229\\u6240\\u5f97\\u662f\\u5426\\u8d85\\u904e '
                '94 \\u842c\\u5143\\u3002", "isCategoryHeadline": 1, '
                '"video": "", "payment": 0, "publishAt": 1589039883, '
                '"coverSrc": {"xs": {"src": '
                '"https://cimg.cnyes.cool/prod/news/4474691/xs/df95ee03138e5ad46aa23d11b99b6e66.jpg", '
                '"width": 100, "height": 56}, "s": {"src": '
                '"https://cimg.cnyes.cool/prod/news/4474691/s/df95ee03138e5ad46aa23d11b99b6e66.jpg", '
                '"width": 180, "height": 101}, "m": {"src": '
                '"https://cimg.cnyes.cool/prod/news/4474691/m/df95ee03138e5ad46aa23d11b99b6e66.jpg", '
                '"width": 380, "height": 214}, "l": {"src": '
                '"https://cimg.cnyes.cool/prod/news/4474691/l/df95ee03138e5ad46aa23d11b99b6e66.jpg", '
                '"width": 640, "height": 360}, "xl": {"src": '
                '"https://cimg.cnyes.cool/prod/news/4474691/xl/df95ee03138e5ad46aa23d11b99b6e66.jpg", '
                '"width": 960, "height": 540}, "xxl": {"src": '
                '"https://cimg.cnyes.cool/prod/news/4474691/xl/df95ee03138e5ad46aa23d11b99b6e66.jpg", '
                '"width": 960, "height": 540}}, "abTesting": null, '
                '"categoryId": 851, "columnists": null, '
                '"fundCategoryAbbr": [], "etf": [], "fbShare": 0, '
                '"fbComment": 0, "fbCommentPluginCount": 0, "market": '
                '[]}',
                'entry_published_at': datetime.datetime(2020, 5, 9, 15, 58, 3),
                'entry_summary': '稅務專家提醒，股利所得稅申報主要有「併入所得申報」與「分開計稅」兩種，民眾可擇優申報，至於怎麼選才划算關鍵在於全年股利所得是否超過 94 萬元。',
                'entry_tickers': [],
                'entry_title': '〈報稅教戰手冊〉股利所得「一起稅」還是「分開稅」划算？關鍵看這個數據',
                'from_url': 'https://news.cnyes.com/news/id/4474691'},
        'meta': {},
    }
    assert [p.entry_tickers for p in pages] == [
        [], [], ['TWS:2301:STOCK'], [], [], [], [], [], [], [], [], [], [], [], [], [],
        ['TWS:3380:STOCK', 'TWS:2352:STOCK'],
        ['TWS:5471:STOCK'], ['TWS:4142:STOCK'],
        ['TWS:3557:STOCK'], ['TWS:3044:STOCK'], ['TWS:8454:STOCK'],
        ['TWG:6706:STOCK', 'TWS:2448:STOCK'],
        [], [], [], [], [], [], [],
    ]
    assert urls == [
        'https://news.cnyes.com/api/v3/news/category/headline?limit=30&startAt=1588608000&endAt=1589126399&page=2']


def test_cnyes_page_tags(cnyes_page_html):
    article = Article("")
    article.set_html(cnyes_page_html)
    article.parse()

    scp = CnyesPageScraper()
    assert scp._parse_keywords(cnyes_page_html) == [
        '光寶科', 'LED', '資訊', '電源', '裁員']
    assert scp._parse_tickers(article.clean_top_node) == [
        TickerText(
            text='光寶科 (2301-TW) 今 (9) 日傳出旗下工業自動化事業部裁員百人，對此，光寶科澄清，只是因應考核，進行的組織內部常態性調整，以內轉為優先，並非如外傳所說的裁員上百人。',
            labels=[('', '2301-TW')]
        )]


@pytest.mark.parametrize('page_html,expected', [
    # ('https://www.cnbc.com/2020/04/19/why-big-techs-coronavirus-goodwill-wont-help-in-antitrust-probes.html',
    #  None),
    # ('http://cnb.cx/sytyjc', [])
    ('https://cnb.cx/2Vl4nts', [])
])
def test_cnbc_page_tags(page_html, expected):
    article = Article("http://test.url")
    article.set_html(page_html)
    article.parse()

    initialize(config_dir="../app")
    # cfg = compose("config.yaml")
    # print(cfg)
    # scp = cnbc.CnbcScraper(cfg)
    # scp.parse()
    assert cnbc.parse_tickers(article.clean_top_node) == expected

    # assert article.meta_keywords == None

    # scp.
    # assert scp._parse_keywords(cnyes_page_html) == [
    #     '光寶科', 'LED', '資訊', '電源', '裁員']
    # assert scp._parse_tickers(article.clean_top_node) == [
    #     TickerText(
    #         text='光寶科 (2301-TW) 今 (9) 日傳出旗下工業自動化事業部裁員百人，對此，光寶科澄清，只是因應考核，進行的組織內部常態性調整，以內轉為優先，並非如外傳所說的裁員上百人。',
    #         labels=[('', '2301-TW')]
    #     )]


@pytest.mark.parametrize(
    'page_html,expected', [
        ('https://news.cnyes.com/news/id/4484825',
         ['美元匯價', '做空美元', '經濟重啟', '新冠疫情', '放空美元']),
        ("https://news.cnyes.com/news/id/4504226",
         ['文曄', '大聯大', '半導體通路', '手機', '筆電'])
    ],
    indirect=True)
def test_cnyes_page_parse_keywords(page_html, expected):
    # initialize(config_dir="../app")
    # cfg = compose("config.yaml")
    # print(cfg)
    # scp = cnbc.CnbcScraper(cfg)
    # scp.parse()
    scp = cnyes.CnyesPageScraper()
    assert scp._parse_keywords(page_html) == expected


@pytest.mark.parametrize(
    'page_html,expected', [
        (
            "https://news.cnyes.com/news/id/4504226",
            [
                TickerText(
                    text="半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日公布第二季營收，分別達 1498.1 億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G 基地台產品出貨暢旺，加上筆電需求持強，第二季營收創歷史次高，文曄則因手機下游面臨庫存調整，第二季營收較上季、去年同期略減，預計本季可望重返成長。", labels=[('', '3702-TW'), ('', '3036-TW')])
            ]
        ),
        (
            "https://news.cnyes.com/news/id/4504071",
            [
                TickerText(
                    text='台灣東洋 (4105-TW) 新任總經理施俊良今 (9) 日表示，公司正研發兩款新藥，分別為抗黴藥 Lipo-AB 及微球型產品 Octreotide LAR，其中，Octreotide LAR 正與國際藥廠洽談商業銷售合約細節，最快今年可完成，里程碑金上看 500 萬美元 (約新台幣 1.5 億元)。', labels=[('', '4105-TW')]),
                TickerText(
                    text='東洋共有兩座廠房，分別位於中壢與六堵，其中，中壢廠為癌症專屬針劑廠，主要是為國際大廠嬌生 (J&J) 進行藥品代工；六堵廠則斥資超過 10 億元建置微脂體、微球產線，目前主要是為日本客戶及子公司東生華 (8432-TW) 代工藥品。未來抗黴藥 Lipo-AB 及微球型產品 Octreotide LAR 上市後，也將由六堵廠進行生產。', labels=[('', '8432-TW')])
            ]
        )
    ], indirect=True)
def test_cnyes_page_parse_tickers(page_html, expected):
    article = Article("http://test.url")
    article.set_html(page_html)
    article.parse()
    scp = cnyes.CnyesPageScraper()

    assert scp._parse_tickers(article.clean_top_node) == expected
