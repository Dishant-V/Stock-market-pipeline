"""Streamlit monitoring dashboard — read-only, auto-refreshes every 60s."""
import os
import time
from datetime import datetime, timedelta, timezone

import mysql.connector
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Stock Watchlist Monitor", layout="wide")

IST = timezone(timedelta(hours=5, minutes=30))


def get_connection():
    return mysql.connector.connect(
        host=os.environ.get("APP_DB_HOST", "mysql"),
        port=int(os.environ.get("APP_DB_PORT", "3306")),
        user=os.environ.get("APP_DB_USER", "app"),
        password=os.environ.get("APP_DB_PASSWORD", "apppass"),
        database=os.environ.get("APP_DB_NAME", "stocks"),
    )


@st.cache_data(ttl=60)
def load_watchlist() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql(
            "SELECT ticker, threshold_type, threshold_value, is_active "
            "FROM watchlist ORDER BY ticker",
            conn,
        )
    finally:
        conn.close()
    return df


@st.cache_data(ttl=60)
def load_price_history() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql(
            "SELECT ticker, price, prev_close, pct_change, fetched_at "
            "FROM price_history ORDER BY fetched_at",
            conn,
        )
    finally:
        conn.close()
    if not df.empty:
        # Convert UTC to IST for display
        df["fetched_at_ist"] = pd.to_datetime(df["fetched_at"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    return df


@st.cache_data(ttl=60)
def load_alerts() -> pd.DataFrame:
    conn = get_connection()
    try:
        df = pd.read_sql(
            "SELECT ticker, rule_type, threshold_value, observed_value, message, emailed_to, fired_at "
            "FROM alerts_fired ORDER BY fired_at DESC",
            conn,
        )
    finally:
        conn.close()
    if not df.empty:
        df["fired_at_ist"] = pd.to_datetime(df["fired_at"]).dt.tz_localize("UTC").dt.tz_convert("Asia/Kolkata")
    return df


def fmt_ist(dt):
    """Format datetime as IST string."""
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


# --- UI ---
st.title("📈 Stock Watchlist Monitor (IST)")

# Auto-refresh indicator
placeholder = st.empty()
with placeholder.container():
    st.caption(f"Auto-refreshes every 60s • Last update: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S %Z')}")

# --- 1. Price History Line Charts ---
st.subheader("Price History (per ticker)")
price_df = load_price_history()
if price_df.empty:
    st.info("No price data yet — DAG will populate on next scheduled run.")
else:
    tickers = price_df["ticker"].unique()
    for ticker in tickers:
        tdf = price_df[price_df["ticker"] == ticker].copy()
        fig = px.line(
            tdf,
            x="fetched_at_ist",
            y="price",
            title=f"{ticker} — Price Over Time (IST)",
            hover_data=["prev_close", "pct_change"],
            markers=True,
        )
        fig.update_layout(height=300, margin=dict(l=40, r=20, t=40, b=40))
        st.plotly_chart(fig, use_container_width=True)

# --- 2. Watchlist Table ---
st.subheader("Active Watchlist")
wl_df = load_watchlist()
if wl_df.empty:
    st.info("Watchlist is empty.")
else:
    wl_df["is_active"] = wl_df["is_active"].map({1: "✅ Active", 0: "❌ Inactive"})
    st.dataframe(wl_df, use_container_width=True, hide_index=True)

# --- 3. Alerts Fired Table ---
st.subheader("Alerts Fired (most recent first)")
alerts_df = load_alerts()
if alerts_df.empty:
    st.info("No alerts fired yet.")
else:
    # Format IST column for display
    display_df = alerts_df.copy()
    display_df["fired_at"] = display_df["fired_at_ist"].apply(fmt_ist)
    st.dataframe(display_df[["ticker", "rule_type", "threshold_value", "observed_value", "message", "emailed_to", "fired_at"]], use_container_width=True, hide_index=True)

# Trigger rerun after 60s
time.sleep(60)
st.rerun()