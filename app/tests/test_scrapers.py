import datetime
from typing import Union, List

import dateutil
import pytest
import requests
import requests_cache
from newspaper import Article
from lxml import etree
from hydra.experimental import compose, initialize
from elasticsearch_dsl import Document

from app.scrapers import cnyes, cnbc, rss, moneydj
from app.scrapers.base import TickerText
from app.store import es

"""
pip install -e .
python -m pytest tests/test_scrapers.py::test_cnyes_page_parse_tickers -vv
python -m pytest tests/test_scrapers.py::test_cnyes_page_parse_tickers -vv
"""


def doc_to_dict(docs: List[Document]):
    s = []
    for doc in docs:
        d = doc.to_dict()
        d.pop("created_at")
        d.pop("fetched_at")
        s.append(d)
    return s


@pytest.fixture
def expected(request):
    return request.param


@pytest.fixture(scope="module")
def page_html(request):
    requests_cache.install_cache()
    r = requests.get(request.param)
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
    scp = cnbc.CnyesApiScraper()
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


@pytest.mark.parametrize(
    'page_html,expected', [
        (
            "https://news.cnyes.com/news/id/4504226",
            [
                {'article_metadata': '{"fb": {"app_id": 325242790993768, "pages": '
                 '"107207125979624,341172005895696,316306791736155,659317060868369,586654018059088"}, '
                 '"og": {"site_name": "Anue鉅亨", "locale": "zh_TW", '
                 '"title": "IC通路雙雄Q2營收不同調 大聯大年增15.97% 文曄持平 | Anue鉅亨 - '
                 '台股新聞", "description": "半導體通路雙雄大聯大 (3702-TW)、文曄 '
                 '(3036-TW) 今(9) 日皆公布第二季營收，分別達 1498.1 億元、747.24 億元，年增 '
                 '15.97%、年減 0.04%；大聯大受惠 5G 基地台產品出貨暢旺，加上筆電需求持強，第", "type": '
                 '"article", "url": '
                 '"https://news.cnyes.com/news/id/4504226", "image": '
                 '{"identifier": '
                 '"https://cimg.cnyes.cool/prod/news/4504226/l/96f5a246d4a0e7d0d6f9e5cbb8085aee.jpg", '
                 '"width": 640, "height": 360}}, "description": '
                 '"半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日皆公布第二季營收，分別達 '
                 '1498.1 億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G '
                 '基地台產品出貨暢旺，加上筆電需求持強，第", "article": {"publisher": '
                 '"https://www.facebook.com/cnYES", "published_time": '
                 '"2020-07-09T19:36:01+08:00", "author": '
                 '"https://www.facebook.com/cnYES"}, '
                 '"msapplication-TileColor": "#da532c", '
                 '"msapplication-TileImage": "/mstile-144x144.png", '
                 '"theme-color": "#ffffff", "viewport": '
                 '"width=device-width, initial-scale=1, '
                 'user-scalable=yes", "buildName": "3.19.0"}',
                 'article_published_at': datetime.datetime(2020, 7, 9, 19, 36, 1, tzinfo=dateutil.tz.tzoffset(None, 28800)),
                 'article_text': '半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日公布第二季營收，分別達 1498.1 '
                 '億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G '
                 '基地台產品出貨暢旺，加上筆電需求持強，第二季營收創歷史次高，文曄則因手機下游面臨庫存調整，第二季營收較上季、去年同期略減，預計本季可望重返成長。\n'
                 '\n'
                 '大聯大 6 月營收 473.5 億元，月增 1.37%，年增 2.16%，第二季營收 1498.1 億元，季增 '
                 '14.32%，年增 15.97%，累計上半年營收 2335 億元，年增 17.21%。\n'
                 '\n'
                 '文曄 6 月營收 237.6 億元，月增 2.93%，年減 4%，第二季營收 747.24 億元，季減 '
                 '3.56%，年減 0.04%，累計上半年營收 1522 億元，年增 7.02%。\n'
                 '\n'
                 '大聯大指出，隨著 5G 基地台陸續建置，手機及 IoT '
                 '相關產品商機顯現，企業與家庭用戶也持續投資遠端工作環境，帶動筆電、PC、網通設備、伺服器等硬體出貨持續暢旺，推升第二季營收創下歷史次高。\n'
                 '\n'
                 '文曄近期則因手機下游進行庫存調整，加上消費性電子需求疲軟，導致第二季營收較上季略減，不過，隨著 5G '
                 '手機將在第三季陸續問世，文曄營收也可望重返成長。',
                 'article_title': 'IC通路雙雄Q2營收不同調 大聯大年增15.97% 文曄持平',
                 'from_url': 'test.url',
                 'http_status': 200,
                 'parsed': '{"keywords": ["文曄", "大聯大", "半導體通路", "手機", "筆電"], "tickers": '
                 '[{"text": "半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日公布第二季營收，分別達 '
                 '1498.1 億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G '
                 '基地台產品出貨暢旺，加上筆電需求持強，第二季營收創歷史次高，文曄則因手機下游面臨庫存調整，第二季營收較上季、去年同期略減，預計本季可望重返成長。", '
                 '"labels": [["", "3702-TW"], ["", "3036-TW"]]}]}',
                 'resolved_url': 'test.url'},

            ]
        ),
    ], indirect=True)
def test_cnyes_parse_page(page_html, expected):
    es.init()
    url = "test.url"
    parsed = cnyes.CnyesPageScraper.parse(url, url, 200, page_html)
    assert doc_to_dict(parsed) == expected


@pytest.mark.parametrize(
    'page_html,expected', [
        (
            "https://www.moneydj.com/KMDJ/News/NewsRealList.aspx?a=MB010000",
            [
                {'entry_title': 'Ifo調查：德國製造業出口預期創2019年2月以來最高',
                 'from_url': 'https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=ad821f07-002d-4d7f-b82e-2dd06836b95f&c=MB010000'},
                {'entry_title': '震盪近500點！台股收跌1點/萬三得而復失，成交爆天量',
                 'from_url': 'https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=60c34bfb-7e17-486c-a575-dce100a4cabe&c=MB010000'},
                {'entry_title': '在家工作衝擊市區餐飲業！學者：美國還有印鈔空間',
                 'from_url': 'https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=e0b4bb33-fed2-4550-9b74-cf11da0ff173&c=MB010000'},
            ]
        ),
    ], indirect=True)
def test_moneydj_parse_index(page_html, expected):
    es.init()
    url = "test.url"
    parsed = moneydj.MoneydjIndexScraper.parse(url, url, 200, page_html)
    assert doc_to_dict(parsed)[:3] == expected


@pytest.mark.parametrize(
    'page_html,expected', [
        (
            "https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=f639f8d5-5d93-4cde-87f7-ee2646123a87&c=MB010000",
            [
                {'article_metadata': '{"description": "《美國經濟》ISM非製造業指數連3降 遜於預期。 精實新聞 '
                 '2011-12-06 06:41:17 記者 賴宏昌 '
                 '報導美國供應管理協會(ISM)5日公佈，2011年11月非製造業景氣判斷指數自前月的5", '
                 '"keywords": "國家貿易,經濟指標", "og": {"title": '
                 '"《美國經濟》ISM非製造業指數連3降 遜於預期-MoneyDJ理財網", "image": '
                 '"https://www.moneydj.com/InfoSvc/Thumbnail.aspx?id=f639f8d5-5d93-4cde-87f7-ee2646123a87", '
                 '"type": "website", "url": '
                 '"https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=f639f8d5-5d93-4cde-87f7-ee2646123a87", '
                 '"description": '
                 '"美國供應管理協會(ISM)5日公佈，2011年11月非製造業景氣判斷指數自前月的52.9降至52.0，創2010年1月(50.7)以來新低，為連續第3個月呈現下滑，但為連續第24個月高於景氣榮枯分界點(50)。根據Thomson '
                 'R...", "site_name": "MoneyDJ理財網"}, "fb": {"app_id": '
                 '122887437721589}}',
                 'article_text': '精實新聞 2011-12-06 06:41:17 記者 賴宏昌 報導\n'
                 '\n'
                 '美國供應管理協會(ISM)5日公佈，2011年11月非製造業景氣判斷指數自前月的52.9降至52.0，創2010年1月(50.7)以來新低，為連續第3個月呈現下滑，但為連續第24個月高於景氣榮枯分界點(50)。根據Thomson '
                 'Reuters的統計，經濟學家原先普遍預期11月指數將上揚至53.5。\n'
                 '\n'
                 '過去12個月美國非製造業景氣判斷指數平均值為54.8；區間高低點分別為59.7、52.0。彭博社統計顯示，在截至2007年12月為止的5年期間ISM非製造業景氣判斷指數平均值為56.1。\n'
                 '\n'
                 '11月非製造業景氣判斷指數當中的就業指數自53.3降至48.9，為過去3個月以來第2度跌破50。彭博社報導此為2009年3月以來最大單月跌幅。與2011年10月相比，5個產業回報就業機會增加，8個產業回報縮減，5個產業回報持平。',
                 'article_title': '《美國經濟》ISM非製造業指數連3降 遜於預期',
                 'entry_meta': '{"alternativeHeadline": "《美國經濟》ISM非製造業指數連3降 遜於預期-MoneyDJ理財網", '
                 '"thumbnailUrl": '
                 '"https://www.moneydj.com/InfoSvc/Thumbnail.aspx?id=f639f8d5-5d93-4cde-87f7-ee2646123a87", '
                 '"image": '
                 '"https://www.moneydj.com/InfoSvc/Thumbnail.aspx?id=f639f8d5-5d93-4cde-87f7-ee2646123a87", '
                 '"description": '
                 '"財經新聞讓您掌握國內外最新、最快的財經新聞，國際觀點橫掃財經新聞全貌、各種國際趨勢、金融商品趨勢、產業趨勢等財經新聞一應俱全。", '
                 '"headline": "《美國經濟》ISM非製造業指數連3降 遜於預期-MoneyDJ理財網", "author": '
                 '"賴宏昌", "url": '
                 '"https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=f639f8d5-5d93-4cde-87f7-ee2646123a87", '
                 '"dateModified": "2011-12-06T06:41:17", "datePublished": '
                 '"2011-12-06T06:41:17", "dateCreated": "2011-12-06T06:41:17", '
                 '"articleSection": "asia", "isPartOf": "news"}',
                 'entry_published_at': datetime.datetime(2011, 12, 6, 6, 41, 17),
                 'from_url': 'test.url',
                 'http_status': 200,
                 'parsed': '{"keywords": ["國家貿易", "經濟指標"], "tickers": []}',
                 'resolved_url': 'test.url'},

            ]
        ),
    ], indirect=True)
def test_moneydj_parse_page(page_html, expected):
    es.init()
    url = "test.url"
    parsed = moneydj.MoneydjPageScraper.parse(url, url, 200, page_html)
    assert doc_to_dict(parsed) == expected
