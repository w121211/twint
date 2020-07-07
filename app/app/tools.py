from typing import List

import pandas as pd


def generate_rss_yahoo_csv(save_to="../resource/rss_yahoo_us.csv") -> None:
    from get_all_tickers.get_tickers import get_tickers
    tickers = get_tickers()
    urls = [
        "http://finance.yahoo.com/rss/headline?s={}".format(t) for t in tickers]
    df = pd.DataFrame({
        "ticker": tickers,
        "url": urls,
    })
    df.to_csv(save_to, index=False)
