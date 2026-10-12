from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_pipeline():
    """Full pipeline: fetch prices, check thresholds, log history, alert on breaches."""
    import os
    import smtplib
    import time
    from email.mime.text import MIMEText

    import mysql.connector
    import yfinance as yf

    # --- DB config ---
    db_host = os.environ.get("APP_DB_HOST", "mysql")
    db_port = int(os.environ.get("APP_DB_PORT", "3306"))
    db_user = os.environ.get("APP_DB_USER", "app")
    db_pass = os.environ.get("APP_DB_PASSWORD", "apppass")
    db_name = os.environ.get("APP_DB_NAME", "stocks")

    # --- SMTP config ---
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASSWORD")
    alert_to = os.environ.get("ALERT_EMAIL_TO")

    # Cooldown window (minutes) — skip re-alerting for same ticker/rule within this window
    COOLDOWN_MINUTES = int(os.environ.get("ALERT_COOLDOWN_MINUTES", "30"))

    def send_alert(ticker, rule, threshold, observed, price, prev_close, pct_change):
        subject = f"[StockWatch] {ticker} threshold breach: {rule}"
        body = (
            f"Ticker: {ticker}\n"
            f"Rule: {rule} @ {threshold}\n"
            f"Observed: {observed:.4f}%\n"
            f"Price: {price:.4f}\n"
            f"Prev Close: {prev_close:.4f}\n"
            f"Pct Change: {pct_change:.4f}%\n"
            f"Time: {datetime.now(UTC).isoformat()}"
        )
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = alert_to
        with smtplib.SMTP(smtp_host, smtp_port) as s:
            s.starttls()
            s.login(smtp_user, smtp_pass)
            s.send_message(msg)

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

    def is_breach(rule: str, threshold: float, row: dict) -> bool:
        if rule == "pct_drop":
            return row["pct_change"] is not None and row["pct_change"] <= -threshold
        if rule == "price_below":
            return row["price"] < threshold
        if rule == "price_above":
            return row["price"] > threshold
        return False

    def cooldown_active(conn, ticker: str, rule: str, threshold: float) -> bool:
        """Return True if an alert for this ticker+rule+threshold was sent within COOLDOWN_MINUTES."""
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM alerts_fired "
                "WHERE ticker = %s AND rule_type = %s AND threshold_value = %s "
                "AND last_alerted_at >= DATE_SUB(NOW(), INTERVAL %s MINUTE)",
                (ticker, rule, threshold, COOLDOWN_MINUTES)
            )
            return cur.fetchone() is not None
        finally:
            cur.close()

    conn = mysql.connector.connect(
        host=db_host, port=db_port, user=db_user, password=db_pass, database=db_name
    )
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT ticker, threshold_type, threshold_value "
            "FROM watchlist WHERE is_active = TRUE"
        )
        watchlist = cur.fetchall()
        cur.close()

        if not watchlist:
            print("No active tickers in watchlist")
            return

        for i, entry in enumerate(watchlist):
            if i > 0:
                time.sleep(1.5)
            ticker = entry["ticker"]
            rule = entry["threshold_type"]
            threshold = float(entry["threshold_value"])

            row = fetch_price(ticker)
            if row is None:
                print(f"{ticker}: NO DATA")
                continue

            # insert price history
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO price_history (ticker, price, prev_close, pct_change) "
                "VALUES (%s, %s, %s, %s)",
                (row["ticker"], row["price"], row["prev_close"], row["pct_change"]),
            )
            conn.commit()
            cur.close()

            breached = is_breach(rule, threshold, row)
            print(
                f"{ticker}: {'BREACH' if breached else 'ok'} "
                f"price={row['price']} pct_change={row['pct_change']}% "
                f"(rule={rule} @ {threshold})"
            )

            if breached and smtp_user and smtp_pass and alert_to:
                # Check cooldown before sending
                if cooldown_active(conn, ticker, rule, threshold):
                    print(f"  -> cooldown active for {ticker} ({rule} @ {threshold}), skipping alert")
                else:
                    send_alert(
                        ticker, rule, threshold,
                        row["pct_change"], row["price"],
                        row["prev_close"], row["pct_change"]
                    )
                    # log alert fired with last_alerted_at
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO alerts_fired (ticker, rule_type, threshold_value, "
                        "observed_value, message, emailed_to, last_alerted_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, NOW())",
                        (
                            ticker, rule, threshold, row["pct_change"],
                            f"{rule} @ {threshold} breached", alert_to
                        ),
                    )
                    conn.commit()
                    cur.close()
                    print(f"  -> alert sent & logged for {ticker}")
    finally:
        conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="stock_watchlist",
    default_args=default_args,
    description="Poll watchlist tickers every 5 min during market hours, alert on threshold breach",
    schedule_interval="*/5 3-9,13-20 * * 1-5",  # NSE: 3:45-10:00 UTC, US: 13:00-20:00 UTC
    start_date=datetime(2026, 8, 25),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "watchlist", "monitoring"],
    default_view="grid",
    doc_md="""
# Stock Watchlist Monitoring Pipeline

**Streamlit Dashboard:** [📊 Open Dashboard](http://localhost:8501) (opens in new tab)

Monitors tickers (US + NSE) with configurable threshold rules.
Sends email alerts on breach with 30-min cooldown.
""",
) as dag:

    run_task = PythonOperator(
        task_id="poll_and_alert",
        python_callable=run_pipeline,
        execution_timeout=timedelta(minutes=4),
    )