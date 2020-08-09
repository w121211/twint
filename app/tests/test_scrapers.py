"""
cd /workspace/twint/app
pip install -e .
python -m pytest tests/test_scrapers.py::test_cnbc_page_parse -vv
python -m pytest tests/test_scrapers.py::test_cnyes_page_parse -vv
python -m pytest tests/test_scrapers.py::test_moneydj_index_parse -vv
"""
import datetime
import dateutil
from typing import Union, List

import pytest
import requests
import requests_cache
from newspaper import Article
from lxml import etree
from hydra.experimental import compose, initialize
from elasticsearch_dsl import Document

from app.store import es
from app.store.model import Page, Parsed, TickerText
from app.scrapers import base, cnyes, cnbc, rss, moneydj, yahoo


def cleanup(pages: List[Page]) -> List[Page]:
    for p in pages:
        p.fetched_at = None
        p.article_metadata = dict(p.article_metadata)
    return pages


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
    ('https://www.cnbc.com/2020/08/08/berkshire-hathaway-earnings-q2-2020.html', [Page(from_url='test.url', resolved_url='test.url', http_status=200, entry_urls=[], entry_tickers=[], entry_title=None, entry_summary=None, entry_published_at=None, entry_meta=None, article_metadata={'viewport': 'initial-scale=1.0, width=device-width', 'AssetType': 'cnbcnewsstory', 'pageNodeId': 106654680, 'description': "Berkshire announced it bought $5.1 billion of its own shares during the second quarter as the pandemic dented the conglomerate's operations.", 'og': {'type': 'article', 'title': 'Buffett buys back record $5.1 billion in Berkshire stock as coronavirus hits operating earnings', 'description': "Berkshire announced it bought $5.1 billion of its own shares during the second quarter as the pandemic dented the conglomerate's operations.", 'url': 'https://www.cnbc.com/2020/08/08/berkshire-hathaway-earnings-q2-2020.html', 'site_name': 'CNBC', 'image': 'https://image.cnbcfm.com/api/v1/image/105894584-15571486323141u8a0031r.jpg?v=1596826906'}, 'twitter': {'image': {'src': 'https://image.cnbcfm.com/api/v1/image/105894584-15571486323141u8a0031r.jpg?v=1596826906'}, 'card': 'summary_large_image', 'site': '@CNBC', 'url': 'https://www.cnbc.com/2020/08/08/berkshire-hathaway-earnings-q2-2020.html', 'title': 'Buffett buys back record $5.1 billion in Berkshire stock as coronavirus hits operating earnings', 'description': "Berkshire announced it bought $5.1 billion of its own shares during the second quarter as the pandemic dented the conglomerate's operations.", 'creator': 'foimbert'}, 'article': {'publisher': 'https://www.facebook.com/cnbc', 'published_time': '2020-08-08T12:29:24+0000', 'modified_time': '2020-08-08T13:34:07+0000', 'section': 'Markets', 'author': 'https://www.facebook.com/CNBC', 'opinion': 'false', 'tag': 'Markets'}, 'news_keywords': 'Breaking News: Earnings,Earnings,Berkshire Hathaway Inc,Wall Street,Investment strategy,Stock markets,Breaking News: Markets,Markets,business news', 'keywords': 'Breaking News: Earnings,Earnings,Berkshire Hathaway Inc,Wall Street,Investment strategy,Stock markets,Breaking News: Markets,Markets,business news', 'tp': {'PreferredRuntimes': 'universal', 'PreferredFormats': 'M3U,MPEG4', 'initialize': 'false'}, 'apple-itunes-app': 'app-id=398018310', 'al': {'ios': {'app_name': 'CNBC Business News and Finance', 'app_store_id': 398018310}}, 'parsely-metadata': '{"nodeid":106654680,"originalImage":"https://image.cnbcfm.com/api/v1/image/105894584-15571486323141u8a0031r.jpg?v=1596826906"}', 'author': 'Fred Imbert'}, article_published_at=datetime.datetime(2020, 8, 8, 12, 29, 24, tzinfo=dateutil.tz.tzlocal(
    )), article_title='Buffett buys back record $5.1 billion in Berkshire stock as coronavirus hits operating earnings', article_text='Berkshire Hathaway announced on Saturday it bought back a record amount of its own stock during the second quarter as the coronavirus pandemic dented operations for Warren Buffett\'s conglomerate.\n\nThe company said it repurchased $5.1 billion worth in stock in May and June. Berkshire repurchased more than $4.6 billion of its Class B stock and about $486.6 million in Class A shares.\n\nThe share repurchase is the most ever in a single period for Buffett, nearly double the $2.2 billion the conglomerate bought back in the final quarter of 2019. In fact, the amount is slightly more than what Buffett spent buying back Berkshire stock in all of 2019. Despite the company\'s record buybacks last quarter, the Berkshire\'s cash hoard grew to more than $140 billion.\n\nBerkshire Class A and Class B shares plunged more than 19% in the first quarter and lagged the S&P 500 during the second quarter with declines of more than 1%.\n\nThose buybacks come during a tough period for some of Berkshire\'s wholly owned businesses as the pandemic thwarted economic activity in the U.S. and across the globe.\n\nOperating profits for Berkshire fell 10% during the second quarter, dropping to $5.51 billion from $6.14 billion in the year-earlier period. The company also took a charge of approximately $10 billion from Precision Castparts, Berkshire\'s largest business within its manufacturing segment.\n\nBerkshire\'s investments in public markets gained $34.5 billion in the quarter. That gain caused overall second-quarter net earnings to surge to $26.3 billion, up from $14.1 billion a year ago. However, unrealized gains from investments quarter to quarter are volatile and Buffett himself warns investors not to focus on that overall net earnings figure.\n\nThe company is heavily invested in several companies that have rallied since the broader stock market bottomed in late March. Apple — Berkshire\'s biggest common stock holding — has nearly doubled since March 23. JPMorgan Chase is up more than 27% over that time period and Amazon has popped more than 66%.\n\nTo be sure, Berkshire warned of the uncertainty presented to its businesses by the ongoing coronavirus pandemic, noting: "The risks and uncertainties resulting from the pandemic that may affect our future earnings, cash flows and financial condition include the nature and duration of the curtailment or closure of our various facilities and the long-term effect on the demand for our products and services."\n\nThe company also said insurance giant Geico — which is owned by Berkshire — will likely see its underwriting results "negatively affected" by the pandemic for the rest of 2020 and into the first quarter of next year.\n\nSubscribe to CNBC PRO for exclusive insights and analysis, and live business day programming from around the world.\n\nCorrection: This story has been updated to reflect Berkshire\'s operating profits fell 10% to $5.51 billion. A previous version of this story misstated the figure.', article_html=None, article_summary="Berkshire announced it bought $5.1 billion of its own shares during the second quarter as the pandemic dented the conglomerate's operations.", parsed=Parsed(keywords=['Breaking News: Earnings', 'Earnings', 'Berkshire Hathaway Inc', 'Wall Street', 'Investment strategy', 'Stock markets', 'Breaking News: Markets', 'Markets', 'business news'], tickers=[TickerText(text="Berkshire Hathaway\xa0announced on Saturday it bought back a record amount of its own stock\xa0during the second quarter as the coronavirus pandemic dented operations for Warren Buffett's conglomerate.", labels=[('Berkshire Hathaway', 'BRK.A')])]), fetched_at=None)]),
    ('http://cnb.cx/sytyjc', [Page(from_url='test.url', resolved_url='test.url', http_status=200, entry_urls=[], entry_tickers=[], entry_title=None, entry_summary=None, entry_published_at=None, entry_meta=None, article_metadata={'viewport': 'initial-scale=1.0, width=device-width', 'AssetType': 'franchise', 'pageNodeId': 103620081, 'description': 'CNBC PRO is your all-access pass to CNBC’s premium digital experience. It’s a comprehensive 24-hour online destination that provides behind-the-scenes content and tools for active investors and business decision makers.', 'og': {'type': 'website', 'title': 'CNBC PRO', 'description': 'CNBC PRO is your all-access pass to CNBC’s premium digital experience. It’s a comprehensive 24-hour online destination that provides behind-the-scenes content and tools for active investors and business decision makers.', 'url': 'https://www.cnbc.com/pro/', 'site_name': 'CNBC', 'image': 'https://sc.cnbcfm.com/applications/cnbc.com/staticcontent/img/cnbc_logo.gif?v=1524171804'}, 'twitter': {'image': {'src': 'https://sc.cnbcfm.com/applications/cnbc.com/staticcontent/img/cnbc_logo.gif?v=1524171804'}, 'card': 'summary_large_image', 'site': '@CNBC', 'url': 'https://www.cnbc.com/pro/', 'title': 'CNBC PRO', 'description': 'CNBC PRO is your all-access pass to CNBC’s premium digital experience. It’s a comprehensive 24-hour online destination that provides behind-the-scenes content and tools for active investors and business decision makers.', 'creator': '@CNBC'}, 'article': {
     'publisher': 'https://www.facebook.com/cnbc', 'published_time': '2016-05-09T20:46:42+0000', 'modified_time': '2020-08-06T17:14:30+0000'}, 'news_keywords': 'CNBC Pro,Pro Uncut,Pro: Street Calls ,Pro: Follow the Pros ,Pro: Investing trends,Pro: Pro Talks,business news', 'keywords': 'CNBC Pro,Pro Uncut,Pro: Street Calls ,Pro: Follow the Pros ,Pro: Investing trends,Pro: Pro Talks,business news', 'tp': {'PreferredRuntimes': 'universal', 'PreferredFormats': 'M3U,MPEG4', 'initialize': 'false'}, 'apple-itunes-app': 'app-id=398018310', 'al': {'ios': {'app_name': 'CNBC Business News and Finance', 'app_store_id': 398018310}}, 'parsely-metadata': '{"nodeid":103620081,"originalImage":"https://sc.cnbcfm.com/applications/cnbc.com/resources/img/editorial/2015/03/30/102546467-Most-Popular--Image-Placeholder-Large.jpg"}'}, article_published_at=datetime.datetime(2016, 5, 9, 20, 46, 42, tzinfo=dateutil.tz.tzlocal()), article_title='CNBC PRO', article_text='', article_html=None, article_summary='CNBC PRO is your all-access pass to CNBC’s premium digital experience. It’s a comprehensive 24-hour online destination that provides behind-the-scenes content and tools for active investors and business decision makers.', parsed=Parsed(keywords=['CNBC Pro', 'Pro Uncut', 'Pro: Street Calls', 'Pro: Follow the Pros', 'Pro: Investing trends', 'Pro: Pro Talks', 'business news'], tickers=None), fetched_at=None)]),
    ('https://cnb.cx/2Vl4nts',  [Page(from_url='test.url', resolved_url='test.url', http_status=200, entry_urls=[], entry_tickers=[], entry_title=None, entry_summary=None, entry_published_at=None, entry_meta=None, article_metadata={'viewport': 'width=device-width, initial-scale=1, maximum-scale=1, user-scalable=0', 'og': {'title': 'Shake Shack’s Big Return; Microsoft’s President; Covid-Positive Plasma Treatments by Squawk Pod', 'type': 'music.song', 'image': {'identifier': 'https://megaphone.imgix.net/podcasts/e0ac94b4-db21-11e9-9a79-23d4f260bcfe/image/uploads_2F1572032286020-j0o0zmbl2zp-9997a584d8b3ba823853a8134fb13f2b_2FSQUAWK_POD_3000X3000_APPLE.jpg?ixlib=rails-2.1.2&w=500&h=500', 'url': 'https://megaphone.imgix.net/podcasts/e0ac94b4-db21-11e9-9a79-23d4f260bcfe/image/uploads_2F1572032286020-j0o0zmbl2zp-9997a584d8b3ba823853a8134fb13f2b_2FSQUAWK_POD_3000X3000_APPLE.jpg?ixlib=rails-2.1.2&w=500&h=500', 'width': 500, 'height': 500}, 'url': 'https://megaphone.link/CNBC7576578220', 'audio': {
     'identifier': 'http://traffic.megaphone.fm/CNBC7576578220.mp3', 'secure_url': 'https://traffic.megaphone.fm/CNBC7576578220.mp3', 'type': 'audio/mpeg', 'artist': 'Squawk Pod', 'title': 'Shake Shack’s Big Return; Microsoft’s President; Covid-Positive Plasma Treatments'}, 'site_name': 'Megaphone.fm'}, 'twitter': {'card': 'player', 'site': '@MegaphonePods', 'title': 'Shake Shack’s Big Return; Microsoft’s President; Covid-Positive Plasma Treatments by Squawk Pod', 'image': 'https://megaphone.imgix.net/podcasts/e0ac94b4-db21-11e9-9a79-23d4f260bcfe/image/uploads_2F1572032286020-j0o0zmbl2zp-9997a584d8b3ba823853a8134fb13f2b_2FSQUAWK_POD_3000X3000_APPLE.jpg?ixlib=rails-2.1.2&w=500&h=500', 'player': {'identifier': 'https://player.megaphone.fm/CNBC7576578220', 'width': 670, 'height': 200, 'stream': {'identifier': 'https://traffic.megaphone.fm/CNBC7576578220.mp3', 'content_type': 'audio/mpeg'}}}}, article_published_at=None, article_title='Megaphone: A Modern Podcasting Platform', article_text='', article_html=None, article_summary=None, parsed=Parsed(keywords=[''], tickers=None), fetched_at=None)]),
], indirect=True)
def test_cnbc_page_parse(page_html, expected):
    url = "test.url"
    parsed = cnbc.CnbcPageScraper.parse(url, url, 200, page_html)
    assert cleanup(parsed) == expected


@ pytest.mark.parametrize('page_html,expected', [
    ("https://news.cnyes.com/news/id/4504226", [Page(from_url='test.url', resolved_url='test.url', http_status=200, entry_urls=[], entry_tickers=[], entry_title=None, entry_summary=None, entry_published_at=None, entry_meta=None, article_metadata={'fb': {'app_id': 325242790993768, 'pages': '107207125979624,341172005895696,316306791736155,659317060868369,586654018059088'}, 'og': {'site_name': 'Anue鉅亨', 'locale': 'zh_TW', 'title': 'IC通路雙雄Q2營收不同調 大聯大年增15.97% 文曄持平 | Anue鉅亨 - 台股新聞', 'description': '半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日皆公布第二季營收，分別達 1498.1 億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G 基地台產品出貨暢旺，加上筆電需求持強，第', 'type': 'article', 'url': 'https://news.cnyes.com/news/id/4504226', 'image': {'identifier': 'https://cimg.cnyes.cool/prod/news/4504226/l/96f5a246d4a0e7d0d6f9e5cbb8085aee.jpg', 'width': 640, 'height': 360}}, 'description': '半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日皆公布第二季營收，分別達 1498.1 億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G 基地台產品出貨暢旺，加上筆電需求持強，第', 'article': {'publisher': 'https://www.facebook.com/cnYES', 'published_time': '2020-07-09T19:36:01+08:00', 'author': 'https://www.facebook.com/cnYES'}, 'msapplication-TileColor': '#da532c', 'msapplication-TileImage': '/mstile-144x144.png',
                                                                                                                                                                                                                                                       'theme-color': '#ffffff', 'viewport': 'width=device-width, initial-scale=1, user-scalable=yes', 'buildName': '3.17.1'}, article_published_at=datetime.datetime(2020, 7, 9, 19, 36, 1, tzinfo=dateutil.tz.tzoffset(None, 28800)), article_title='IC通路雙雄Q2營收不同調 大聯大年增15.97% 文曄持平', article_text='半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日公布第二季營收，分別達 1498.1 億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G 基地台產品出貨暢旺，加上筆電需求持強，第二季營收創歷史次高，文曄則因手機下游面臨庫存調整，第二季營收較上季、去年同期略減，預計本季可望重返成長。\n\n大聯大 6 月營收 473.5 億元，月增 1.37%，年增 2.16%，第二季營收 1498.1 億元，季增 14.32%，年增 15.97%，累計上半年營收 2335 億元，年增 17.21%。\n\n文曄 6 月營收 237.6 億元，月增 2.93%，年減 4%，第二季營收 747.24 億元，季減 3.56%，年減 0.04%，累計上半年營收 1522 億元，年增 7.02%。\n\n大聯大指出，隨著 5G 基地台陸續建置，手機及 IoT 相關產品商機顯現，企業與家庭用戶也持續投資遠端工作環境，帶動筆電、PC、網通設備、伺服器等硬體出貨持續暢旺，推升第二季營收創下歷史次高。\n\n文曄近期則因手機下游進行庫存調整，加上消費性電子需求疲軟，導致第二季營收較上季略減，不過，隨著 5G 手機將在第三季陸續問世，文曄營收也可望重返成長。', article_html=None, article_summary=None, parsed=Parsed(keywords=['文曄', '大聯大', '半導體通路', '手機', '筆電'], tickers=[TickerText(text='半導體通路雙雄大聯大 (3702-TW)、文曄 (3036-TW) 今(9) 日公布第二季營收，分別達 1498.1 億元、747.24 億元，年增 15.97%、年減 0.04%；大聯大受惠 5G 基地台產品出貨暢旺，加上筆電需求持強，第二季營收創歷史次高，文曄則因手機下游面臨庫存調整，第二季營收較上季、去年同期略減，預計本季可望重返成長。', labels=[('', '3702-TW'), ('', '3036-TW')])]), fetched_at=None)]),
    ("https://news.cnyes.com/news/id/1083373",  [Page(from_url='test.url', resolved_url='test.url', http_status=200, entry_urls=[], entry_tickers=[], entry_title=None, entry_summary=None, entry_published_at=None, entry_meta=None, article_metadata={'fb': {'app_id': 325242790993768, 'pages': '107207125979624,341172005895696,316306791736155,659317060868369,586654018059088'}, 'og': {'site_name': 'Anue鉅亨', 'locale': 'zh_TW', 'title': '印尼央行将参考利率维持在6.75% | Anue鉅亨 - 國際政經', 'description': '印尼央行将参考利率维持在6.75%。', 'type': 'article', 'url': 'https://news.cnyes.com/news/id/1083373', 'image': 'https://news.cnyes.com/s/cnyes-og-315x315.jpg'}, 'description': '印尼央行将参考利率维持在6.75%。', 'article': {
        'publisher': 'https://www.facebook.com/cnYES', 'published_time': '2016-04-21T16:28:04+08:00', 'author': 'https://www.facebook.com/cnYES'}, 'msapplication-TileColor': '#da532c', 'msapplication-TileImage': '/mstile-144x144.png', 'theme-color': '#ffffff', 'viewport': 'width=device-width, initial-scale=1, user-scalable=yes', 'buildName': '3.20.0'}, article_published_at=datetime.datetime(2016, 4, 21, 16, 28, 4, tzinfo=dateutil.tz.tzoffset(None, 28800)), article_title='印尼央行将参考利率维持在6.75%', article_text='', article_html=None, article_summary=None, parsed=Parsed(keywords=[], tickers=None), fetched_at=None)]),
], indirect=True)
def test_cnyes_page_parse(page_html, expected):
    url = "test.url"
    parsed = cnyes.CnyesPageScraper.parse(url, url, 200, page_html)
    assert cleanup(parsed) == expected


@ pytest.mark.parametrize(
    'page_html,expected', [
        ("https://www.moneydj.com/KMDJ/News/NewsRealList.aspx?a=MB010000",
         [Page(from_url='https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=ad821f07-002d-4d7f-b82e-2dd06836b95f&c=MB010000', resolved_url=None, http_status=None, entry_urls=[], entry_tickers=[], entry_title='Ifo調查：德國製造業出口預期創2019年2月以來最高',
               entry_summary=None, entry_published_at=None, entry_meta=None, article_metadata=None, article_published_at=None, article_title=None, article_text=None, article_html=None, article_summary=None, parsed=None, fetched_at=None)]),
    ], indirect=True)
def test_moneydj_index_parse(page_html, expected):
    url = "test.url"
    parsed = moneydj.MoneydjIndexScraper.parse(url, url, 200, page_html)
    assert cleanup(parsed)[:1] == expected


@ pytest.mark.parametrize(
    'page_html,expected', [
        (
            "https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=f639f8d5-5d93-4cde-87f7-ee2646123a87&c=MB010000",
            [Page(from_url='test.url', resolved_url='test.url', http_status=200, entry_urls=[], entry_tickers=[], entry_title=None, entry_summary=None, entry_published_at=datetime.datetime(2011, 12, 6, 6, 41, 17), entry_meta={'alternativeHeadline': '《美國經濟》ISM非製造業指數連3降 遜於預期-MoneyDJ理財網', 'thumbnailUrl': 'https://www.moneydj.com/InfoSvc/Thumbnail.aspx?id=f639f8d5-5d93-4cde-87f7-ee2646123a87', 'image': 'https://www.moneydj.com/InfoSvc/Thumbnail.aspx?id=f639f8d5-5d93-4cde-87f7-ee2646123a87', 'description': '財經新聞讓您掌握國內外最新、最快的財經新聞，國際觀點橫掃財經新聞全貌、各種國際趨勢、金融商品趨勢、產業趨勢等財經新聞一應俱全。', 'headline': '《美國經濟》ISM非製造業指數連3降 遜於預期-MoneyDJ理財網', 'author': '賴宏昌', 'url': 'https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=f639f8d5-5d93-4cde-87f7-ee2646123a87', 'dateModified': '2011-12-06T06:41:17', 'datePublished': '2011-12-06T06:41:17', 'dateCreated': '2011-12-06T06:41:17', 'articleSection': 'asia', 'isPartOf': 'news'}, article_metadata={'description': '《美國經濟》ISM非製造業指數連3降 遜於預期。 精實新聞 2011-12-06 06:41:17 記者 賴宏昌 報導美國供應管理協會(ISM)5日公佈，2011年11月非製造業景氣判斷指數自前月的5', 'keywords': '國家貿易,經濟指標', 'og': {
                  'title': '《美國經濟》ISM非製造業指數連3降 遜於預期-MoneyDJ理財網', 'image': 'https://www.moneydj.com/InfoSvc/Thumbnail.aspx?id=f639f8d5-5d93-4cde-87f7-ee2646123a87', 'type': 'website', 'url': 'https://www.moneydj.com/KMDJ/News/NewsViewer.aspx?a=f639f8d5-5d93-4cde-87f7-ee2646123a87', 'description': '美國供應管理協會(ISM)5日公佈，2011年11月非製造業景氣判斷指數自前月的52.9降至52.0，創2010年1月(50.7)以來新低，為連續第3個月呈現下滑，但為連續第24個月高於景氣榮枯分界點(50)。根據Thomson R...', 'site_name': 'MoneyDJ理財網'}, 'fb': {'app_id': 122887437721589}}, article_published_at=None, article_title='《美國經濟》ISM非製造業指數連3降 遜於預期', article_text='精實新聞 2011-12-06 06:41:17 記者 賴宏昌 報導\n\n美國供應管理協會(ISM)5日公佈，2011年11月非製造業景氣判斷指數自前月的52.9降至52.0，創2010年1月(50.7)以來新低，為連續第3個月呈現下滑，但為連續第24個月高於景氣榮枯分界點(50)。根據Thomson Reuters的統計，經濟學家原先普遍預期11月指數將上揚至53.5。\n\n過去12個月美國非製造業景氣判斷指數平均值為54.8；區間高低點分別為59.7、52.0。彭博社統計顯示，在截至2007年12月為止的5年期間ISM非製造業景氣判斷指數平均值為56.1。\n\n11月非製造業景氣判斷指數當中的就業指數自53.3降至48.9，為過去3個月以來第2度跌破50。彭博社報導此為2009年3月以來最大單月跌幅。與2011年10月相比，5個產業回報就業機會增加，8個產業回報縮減，5個產業回報持平。', article_html=None, article_summary=None, parsed=Parsed(keywords=['國家貿易', '經濟指標'], tickers=None), fetched_at=None)]
        ),
    ], indirect=True)
def test_moneydj_page_parse(page_html, expected):
    es.connect()
    url = "test.url"
    parsed = moneydj.MoneydjPageScraper.parse(url, url, 200, page_html)
    moneydj.MoneydjPageScraper.save(parsed)
    # raise Exception
    assert cleanup(parsed) == expected


@ pytest.mark.parametrize(
    'page_html,expected', [
        (
            "https://finance.yahoo.com/news/stock-market-news-live-august-7-2020-222526572.html",
            [Page(from_url='test.url', resolved_url='test.url', http_status=200, entry_urls=[], entry_tickers=[], entry_title=None, entry_summary=None, entry_published_at=None, entry_meta=None, article_metadata={'oath': {'guce': {'consent-host': 'guce.yahoo.com'}}, 'msapplication-TileColor': '#6e329d', 'msapplication-TileImage': 'https://s.yimg.com/rz/p/yahoo_frontpage_en-US_s_f_w_bestfit_frontpage.png', 'msvalidate.01': 'A9862C0E6E1BE95BCE0BF3D0298FD58B', 'referrer': 'unsafe-url', 'theme-color': '#400090', 'twitter': {'dnt': 'on', 'site': '@YahooFinance', 'card': 'summary_large_image', 'description': 'Stock futures were little changed to slightly lower Thursday evening as investors braced for the results of the July jobs report Friday morning, which will offer further signals of the direction and pace of economic activity given the ongoing pandemic. Debates in Washington, D.C., over another round', 'image': {'identifier': 'https://s.yimg.com/uu/api/res/1.2/6HL_SzrOFCT6mUM6AsMZrQ--~B/aD0zNTc3O3c9NTM2NjtzbT0xO2FwcGlkPXl0YWNoeW9u/https://media-mbst-pub-ue1.s3.amazonaws.com/creatr-uploaded-images/2020-08/23fa9d60-d833-11ea-bffe-f75de6cf184d', 'src': 'https://s.yimg.com/uu/api/res/1.2/6HL_SzrOFCT6mUM6AsMZrQ--~B/aD0zNTc3O3c9NTM2NjtzbT0xO2FwcGlkPXl0YWNoeW9u/https://media-mbst-pub-ue1.s3.amazonaws.com/creatr-uploaded-images/2020-08/23fa9d60-d833-11ea-bffe-f75de6cf184d'}, 'title': 'Stock market news live updates: Stock futures cling to the flat line ahead of July jobs report'}, 'news_keywords': 'Stock futures', 'apple-itunes-app': 'app-id=304158842,app-argument=yahoo://article/view?uuid=e76eb668-d474-3eb4-95d9-66a12b55cd95&src=web', 'description': 'Stock futures were little changed to slightly lower Thursday evening as investors braced for the results of the July jobs report Friday morning, which will offer further signals of the direction and pace of economic activity given the ongoing pandemic. Debates in Washington, D.C., over another round', 'og': {'type': 'article', 'image': 'https://s.yimg.com/uu/api/res/1.2/6HL_SzrOFCT6mUM6AsMZrQ--~B/aD0zNTc3O3c9NTM2NjtzbT0xO2FwcGlkPXl0YWNoeW9u/https://media-mbst-pub-ue1.s3.amazonaws.com/creatr-uploaded-images/2020-08/23fa9d60-d833-11ea-bffe-f75de6cf184d', 'description': 'Stock futures were little changed to slightly lower Thursday evening as investors braced for the results of the July jobs report Friday morning, which will offer further signals of the direction and pace of economic activity given the ongoing pandemic. Debates in Washington, D.C., over another round', 'title': 'Stock market news live updates: Stock futures cling to the flat line ahead of July jobs report', 'url': 'https://finance.yahoo.com/news/stock-market-news-live-august-7-2020-222526572.html'}, 'al': {'android': {'app_name': 'Yahoo', 'package': 'com.yahoo.mobile.client.android.yahoo', 'url': 'yahoo://article/view?uuid=e76eb668-d474-3eb4-95d9-66a12b55cd95&src=web'}, 'ios': {'app_name': 'Yahoo', 'app_store_id': 304158842, 'url': 'yahoo://article/view?uuid=e76eb668-d474-3eb4-95d9-66a12b55cd95&src=web'}}}, article_published_at=datetime.datetime(2020, 8, 6, 22, 25, 26, tzinfo=dateutil.tz.tzlocal(
            )), article_title='Stock market news live updates: Stock futures cling to the flat line ahead of July jobs report', article_text='Stock futures were little changed to slightly lower Thursday evening as investors braced for the results of the July jobs report Friday morning, which will offer further signals of the direction and pace of economic activity amid the ongoing pandemic. Debates in Washington, D.C., over another round of virus-related relief measures are set to continue on Friday, with lawmakers signaling they will be unlikely to hit their previous self-imposed deadline of hashing out a deal by the end of this week.\n\nEarlier on Thursday, stocks ended higher for a fifth straight session as equities continued their seemingly interminable march higher. The Nasdaq Composite ended at a record high, closing above more than 11,000 for the first time ever, as shares of Facebook (FB), Apple (AAPL) and Amazon (AMZN) closed at records. The S&P 500 ended just 1.1% below its record closing high from February 19.\n\nThe rally came amid better than expected data on the US labor market, with new jobless claims shown to have remained historically elevated, but improved to a pandemic-era low of 1.19 million last week.\n\nOn Friday, the Labor Department’s monthly jobs report will round out the economic data calendar for this week, offering a fuller picture of the state of the US economy last month. Consensus economists expect that the economy added a back 1.48 million non-farm payrolls in July, moderating from the record 4.8 million added during June.\n\n“On balance, we expect the July employment report performance to be positive, extending improvement in the US labor market,” Sam Bullard, senior economist for Wells Fargo corporate and investment banking, wrote in a note Thursday. “That said, questions around the strength and duration of the economic rebound are increasing given the recent upswing in new virus cases in the South and Sunbelt states. This has intensified the uncertainty about whether state and local economies can continue to recover, keeping the markets’ focus squarely on the dashboard of labor market indicators.”\n\nElsewhere in markets, gold climbed further above $2,000 per ounce as investors still sought out safe haven assets but fretted over low-yielding Treasuries.\n\nOn the earnings front, Uber (UBER) on Thursday reported its first-ever year over year decline in revenue as the coronavirus pandemic wiped out demand for rides. Gross bookings in the company’s food delivery business more than doubled to overtake bookings in its ride-hailing business for the first time. However, the recently public company reaffirmed that it expected to hit adjusted EBITDA profitability in 2021. Shares of Uber were lower in late trading.\n\n—\n\n6:09 p.m. ET Thursday: Stock futures open slightly lower\n\nHere were the main moves in equity markets, as of 6:09 p.m. ET:\n\nS&P 500 futures ( ES=F ) : 3,341.25, down 3 points, or 0.09%\n\nDow futures ( YM=F ) : 27,265.00, down 20 points, or 0.07%\n\nNasdaq futures (NQ=F): 11,255.00, down 6.25 points, or 0.06%\n\nNEW YORK, NEW YORK - MARCH 10: A man in a medical mask walks by the New York Stock Exchange (NYSE) on March 10, 2020 in New York City. After losing nearly 8 percent in a market rout yesterday, the Dow Jones Industrial Average was up over 700 points in morning trading as investors look to a possible tax cut and other measures by the Trump administration to combat the coronavirus. (Photo by Spencer Platt/Getty Images) More\n\n—\n\nFollow Yahoo Finance on Twitter, Facebook, Instagram, Flipboard, LinkedIn, and reddit.\n\nFind live stock market quotes and the latest business and finance news\n\nFor tutorials and information on investing and trading stocks, check out Cashay', article_html=None, article_summary='Stock futures were little changed to slightly lower Thursday evening as investors braced for the results of the July jobs report Friday morning, which will offer further signals of the direction and pace of economic activity given the ongoing pandemic. Debates in Washington, D.C., over another round', parsed=Parsed(keywords=['Stock futures'], tickers=[]), fetched_at=None)]
        ),
    ], indirect=True)
def test_yahoo_page_parse(page_html, expected):
    url = "test.url"
    parsed = yahoo.YahooPageScraper.parse(url, url, 200, page_html)
    assert cleanup(parsed) == expected


@ pytest.mark.parametrize(
    'page_html,expected', [
        (
            "https://finance.yahoo.com/news/stock-market-news-live-august-7-2020-222526572.html",
            []
        ),
    ], indirect=True)
def test_base_page_parse(page_html, expected):
    url = "test.url"
    parsed = base.BasePageScraper.parse(url, url, 200, page_html)
    assert cleanup(parsed) == expected
