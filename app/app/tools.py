from typing import List

import pandas as pd

"""
%cd /workspace/twint/app
from app import tools

tools.generate_rss_yahoo_csv(
    save_to="./resource/rss_yahoo_us_indicies.csv",
    symbol_path="./resource/symbol_indicies.csv")
"""


def generate_rss_yahoo_csv(
        save_to="./resource/rss_yahoo_us_stock.csv",
        symbol_path=None) -> None:

    if symbol_path is None:
        from get_all_tickers.get_tickers import get_tickers
        symbols = get_tickers()
    else:
        symbols = pd.read_csv(symbol_path, header=None)[0]

    urls = [
        f"http://finance.yahoo.com/rss/headline?s={s}" for s in symbols]
    df = pd.DataFrame({
        "ticker": symbols,
        "url": urls,
    })
    df.to_csv(save_to, index=False)
