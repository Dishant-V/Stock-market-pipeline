"""Backfill 1 year of historical price data for all watchlist tickers.

Usage: python scripts/backfill_history.py

Fetches ~1 year of daily OHLCV via yfinance and inserts into price_history.
Skips rows that already exist (based on ticker + fetched_at date).
"""
import os
import time
import mysql.connector
import yfinance as yf
from datetime import datetime, timedelta, timezone


def get_connection():
    return mysql.connector.connect(
        host=os.environ.get("APP_DB_HOST", "mysql"),
        port=int(os.environ.get("APP_DB_PORT", "3306")),
        user=os.environ.get("APP_DB_USER", "app"),
        password=os.environ.get("APP_DB_PASSWORD", "apppass"),
        database=os.environ.get("APP_DB_NAME", "stocks"),
    )


def fetch_year(ticker: str):
    """Fetch ~1 year of daily data from yfinance."""
    t = yf.Ticker(ticker)
    hist = t.history(period="1y", interval="1d")
    if hist is None or hist.empty:
        return None
    return hist


def main():
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT ticker FROM watchlist WHERE is_active = TRUE")
        watchlist = [row["ticker"] for row in cur.fetchall()]
        cur.close()

        if not watchlist:
            print("No active tickers in watchlist")
            return

        print(f"Backfilling 1 year for {len(watchlist)} tickers...")

        for i, ticker in enumerate(watchlist):
            if i > 0:
                time.sleep(1.5)  # rate limit
            print(f"\n{ticker}: fetching 1y...")
            hist = fetch_year(ticker)
            if hist is None or hist.empty:
                print(f"  {ticker}: NO DATA (empty history)")
                continue

            inserted = 0
            skipped = 0
            for idx, row in hist.iterrows():
                price = float(row["Close"])
                prev_close = float(row["Open"])  # using Open as proxy for prev close
                pct_change = ((price - prev_close) / prev_close * 100) if prev_close else 0
                fetched_at = idx.to_pydatetime()
                if fetched_at.tzinfo is None:
                    fetched_at = fetched_at.replace(tzinfo=timezone.utc)

                # Use INSERT IGNORE to skip duplicates (unique constraint on ticker + fetched_at)
                cur = conn.cursor()
                try:
                    cur.execute(
                        "INSERT IGNORE INTO price_history (ticker, price, prev_close, pct_change, fetched_at) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (ticker, round(price, 4), round(prev_close, 4), round(pct_change, 4), fetched_at)
                    )
                    conn.commit()
                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
                finally:
                    cur.close()

            print(f"  {ticker}: inserted {inserted}, skipped {skipped}")

        print("\nBackfill complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()