"""Standalone price extractor - testable outside Airflow.

Usage: python scripts/fetch_price.py AAPL [MSFT GOOGL ...]
Prints current price, previous close, and pct change for each ticker.
"""
import sys
import time

import yfinance as yf


def fetch_price(ticker: str) -> dict | None:
    t = yf.Ticker(ticker)
    hist = t.history(period="5d")
    if hist is None or hist.empty:
        return None
    closes = hist["Close"].dropna()
    if len(closes) < 2:
        return None
    price = float(closes.iloc[-1])
    prev_close = float(closes.iloc[-2])
    return {
        "ticker": ticker,
        "price": round(price, 4),
        "prev_close": round(prev_close, 4),
        "pct_change": round((price - prev_close) / prev_close * 100, 4),
    }


def main() -> None:
    tickers = sys.argv[1:] or ["AAPL"]
    results = []
    for i, ticker in enumerate(tickers):
        if i > 0:
            time.sleep(1.5)  # space out calls to avoid Yahoo rate limiting
        try:
            row = fetch_price(ticker)
            if row is None:
                print(f"{ticker}: NO DATA (empty history - check ticker symbol or yfinance issue)")
            else:
                print(
                    f"{row['ticker']}: price={row['price']} "
                    f"prev_close={row['prev_close']} pct_change={row['pct_change']}%"
                )
                results.append(row)
        except Exception as exc:  # noqa: BLE001 - per-ticker isolation, must not crash the batch
            print(f"{ticker}: ERROR {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
