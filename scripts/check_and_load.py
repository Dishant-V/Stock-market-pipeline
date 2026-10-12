"""Check-and-load step: fetch prices for active watchlist tickers,
write them to price_history, and flag threshold breaches.

Usage: python scripts/check_and_load.py
Reads DB config from APP_DB_* environment variables.
"""
import os
import time

import mysql.connector
from fetch_price import fetch_price


def get_connection():
    return mysql.connector.connect(
        host=os.environ.get("APP_DB_HOST", "mysql"),
        port=int(os.environ.get("APP_DB_PORT", "3306")),
        user=os.environ.get("APP_DB_USER", "app"),
        password=os.environ.get("APP_DB_PASSWORD", "apppass"),
        database=os.environ.get("APP_DB_NAME", "stocks"),
    )


def load_watchlist(conn) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT ticker, threshold_type, threshold_value "
        "FROM watchlist WHERE is_active = TRUE"
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def insert_price(conn, row: dict) -> None:
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO price_history (ticker, price, prev_close, pct_change) "
        "VALUES (%s, %s, %s, %s)",
        (row["ticker"], row["price"], row["prev_close"], row["pct_change"]),
    )
    conn.commit()
    cur.close()


def is_breach(threshold_type: str, threshold_value: float, row: dict) -> bool:
    if threshold_type == "pct_drop":
        return row["pct_change"] is not None and row["pct_change"] <= -threshold_value
    if threshold_type == "price_below":
        return row["price"] < threshold_value
    if threshold_type == "price_above":
        return row["price"] > threshold_value
    return False


def main() -> None:
    conn = get_connection()
    try:
        entries = load_watchlist(conn)
        if not entries:
            print("Watchlist is empty or inactive - nothing to do.")
            return
        breaches = []
        for i, entry in enumerate(entries):
            if i > 0:
                time.sleep(1.5)
            ticker = entry["ticker"]
            threshold_value = float(entry["threshold_value"])
            row = fetch_price(ticker)
            if row is None:
                print(f"{ticker}: NO DATA - skipping")
                continue
            insert_price(conn, row)
            breached = is_breach(entry["threshold_type"], threshold_value, row)
            status = "BREACH" if breached else "ok"
            print(
                f"{ticker}: {status} price={row['price']} "
                f"pct_change={row['pct_change']}% "
                f"(rule={entry['threshold_type']} @ {threshold_value})"
            )
            if breached:
                breaches.append({**row, "rule": entry["threshold_type"], "threshold": threshold_value})
        print(f"\nLoaded {len(entries)} rows into price_history. Breaches: {len(breaches)}")
        for b in breaches:
            print(f"  ALERT-WORTHY: {b['ticker']} {b['rule']} threshold={b['threshold']} observed={b['pct_change']}%")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
