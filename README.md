# Stock Market Watchlist Pipeline

> Automated pipeline that polls live stock prices (US + NSE India), checks per-ticker threshold rules, emails breach alerts, and visualizes everything on a live dashboard — orchestrated entirely by Apache Airflow.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.4-017CEE?logo=apacheairflow&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.38.0-FF4B4B?logo=streamlit&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-5.24.0-3F4F75?logo=plotly&logoColor=white)
![yfinance](https://img.shields.io/badge/yfinance-1.6.0-2D2D2D?logo=yahoo&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.x-150458?logo=pandas&logoColor=white)
![Gmail SMTP](https://img.shields.io/badge/Gmail-SMTP-EA4335?logo=gmail&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-CI-2088FF?logo=githubactions&logoColor=white)

---

## Tech Stack

<p align="center">
  <img src="https://cdn.simpleicons.org/python/3776AB" width="48" alt="Python" title="Python 3.11"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/apacheairflow/017CEE" width="48" alt="Apache Airflow" title="Apache Airflow 2.10.4"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/mysql/4479A1" width="48" alt="MySQL" title="MySQL 8.0"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/docker/2496ED" width="48" alt="Docker" title="Docker Compose"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/streamlit/FF4B4B" width="48" alt="Streamlit" title="Streamlit 1.38.0"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/plotly/3F4F75" width="48" alt="Plotly" title="Plotly 5.24.0"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/yahoo/6001D2" width="48" alt="yfinance" title="yfinance 1.6.0"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/pandas/150458" width="48" alt="pandas" title="pandas"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/gmail/EA4335" width="48" alt="Gmail SMTP" title="Gmail SMTP alerts"/>
  &nbsp;&nbsp;
  <img src="https://cdn.simpleicons.org/githubactions/2088FF" width="48" alt="GitHub Actions" title="GitHub Actions CI"/>
</p>

| Technology | Version | Role in Project |
|------------|---------|-----------------|
| <img src="https://cdn.simpleicons.org/python/3776AB" width="20"/> **Python** | 3.11 | Core language for DAG, scripts, and dashboard |
| <img src="https://cdn.simpleicons.org/apacheairflow/017CEE" width="20"/> **Apache Airflow** | 2.10.4 | Workflow orchestration — schedules the DAG every 5 min during market hours |
| <img src="https://cdn.simpleicons.org/mysql/4479A1" width="20"/> **MySQL** | 8.0 | Persistent storage — `watchlist`, `price_history`, `alerts_fired` tables |
| <img src="https://cdn.simpleicons.org/docker/2496ED" width="20"/> **Docker Compose** | - | One-command local deployment of all 4 services (mem-limited for 16 GB laptop) |
| <img src="https://cdn.simpleicons.org/yahoo/6001D2" width="20"/> **yfinance** | 1.6.0 | Live + historical stock data source (US tickers + NSE via `.NS` suffix) |
| <img src="https://cdn.simpleicons.org/streamlit/FF4B4B" width="20"/> **Streamlit** | 1.38.0 | Real-time monitoring dashboard with 60 s auto-refresh |
| <img src="https://cdn.simpleicons.org/plotly/3F4F75" width="20"/> **Plotly** | 5.24.0 | Interactive price-history charts on the dashboard |
| <img src="https://cdn.simpleicons.org/pandas/150458" width="20"/> **pandas** | 2.x | Data wrangling between yfinance, MySQL, and charts |
| <img src="https://cdn.simpleicons.org/gmail/EA4335" width="20"/> **Gmail SMTP** | - | Sends email alerts on threshold breaches (App Password auth, 30-min cooldown) |
| <img src="https://cdn.simpleicons.org/githubactions/2088FF" width="20"/> **GitHub Actions** | - | CI: Ruff lint + DAG import check + `docker compose config` validation |
| <img src="https://cdn.simpleicons.org/ruff/D7FF64" width="20"/> **Ruff** | latest | Fast Python linter used in CI |

---

## Architecture

```
yfinance (US + NSE tickers)
        │
        ▼
Airflow DAG: stock_watchlist          ◄── cron: */5 3-9,13-20 * * 1-5 (UTC)
  task: poll_and_alert                (NSE 03:45–10:00 UTC + US 13:00–20:00 UTC, Mon–Fri)
        │
        ├─▶ fetch prices ──▶ MySQL: price_history
        │
        ├─▶ check rules: pct_drop | price_below | price_above
        │        │
        │        └─▶ breach? ──(30-min cooldown)──▶ SMTP email ──▶ MySQL: alerts_fired
        │
        └─▶ Streamlit dashboard (localhost:8501) reads MySQL ──▶ charts + tables
```

---

## Project Structure

```
stock-watchlist-pipeline/
├── .github/workflows/ci.yml    # CI: ruff, DAG import check, compose validation
├── dags/
│   └── stock_watchlist.py       # Main DAG (fetch → check → alert → log)
├── scripts/
│   ├── fetch_price.py           # Standalone price fetch (test/CI)
│   ├── check_and_load.py        # Threshold check + DB load logic
│   ├── send_test_email.py       # Verifies SMTP configuration
│   └── backfill_history.py      # 1-year historical backfill
├── dashboard/
│   ├── app.py                   # Streamlit dashboard (IST time, Plotly charts)
│   └── Dockerfile
├── sql/
│   └── init.sql                 # Schema + seed watchlist
├── docker-compose.yml           # 4 services: mysql, init, webserver, scheduler (+ dashboard)
├── Dockerfile                   # Custom Airflow image with project deps
├── requirements.txt             # Pinned: yfinance, mysql-connector, streamlit, plotly
└── .env.example                 # Config template (SMTP, cooldown, DB)
```

---

## Quick Start

```bash
# Prereqs: Docker Desktop + WSL2 (memory=6GB in .wslconfig)

# 1. Clone & configure
git clone https://github.com/Dishant-V/Stock-market-pipeline.git
cd Stock-market-pipeline
cp .env.example .env
# Edit .env: set your Gmail App Password + alert recipient email

# 2. Start the stack
docker compose up -d

# 3. Open the UIs
#   Airflow:    http://localhost:8082  (admin/admin) → unpause "stock_watchlist"
#   Dashboard:  http://localhost:8501
#   MySQL:      localhost:3307 (root/changeme)

# 4. Verify
docker compose exec airflow-scheduler airflow tasks test stock_watchlist poll_and_alert 2026-08-27

# 5. Stop everything
docker compose down
```

> **Ports:** Airflow runs on **8082** (Windows reserves 7982–8081 via Hyper-V, so 8080/8081 can't be used). MySQL on **3307**, dashboard on **8501**.

---

## Watchlist Management

Seed tickers (from `sql/init.sql`): `AAPL`, `MSFT`, `GOOGL`. Add more — including Indian NSE stocks — via SQL:

```sql
INSERT INTO watchlist (ticker, threshold_type, threshold_value, is_active) VALUES
  ('NVDA',         'pct_drop', 1.0, TRUE),
  ('TSLA',         'pct_drop', 1.5, TRUE),
  ('RELIANCE.NS',  'pct_drop', 1.0, TRUE),
  ('TCS.NS',       'pct_drop', 1.0, TRUE),
  ('INFY.NS',      'pct_drop', 1.0, TRUE);
-- Rule types: pct_drop | price_below | price_above
```

---

## Alert Rules

| Rule | Fires when |
|------|-----------|
| `pct_drop` | Price drops ≥ N% from the previous close |
| `price_below` | Price falls below a fixed level |
| `price_above` | Price rises above a fixed level |

- Alerts emailed via Gmail SMTP to `ALERT_EMAIL_TO`
- **30-minute cooldown** per ticker/rule (`ALERT_COOLDOWN_MINUTES`) prevents spam
- Every fired alert is logged in the `alerts_fired` table (audit trail)

---

## CI/CD

Push to `main` triggers `.github/workflows/ci.yml`:

1. `ruff check .` — lint
2. `airflow dags list-import-errors` — DAG syntax validation
3. `docker compose config --quiet` — compose file validity

---

## Hardware Notes

Tested on an **Acer Swift Go 14 (16 GB RAM, no GPU)**:

- WSL2 memory capped at 6 GB via `.wslconfig`
- Docker `mem_limits`: MySQL 1 GB, Webserver 1 GB, Scheduler 1.5 GB, Streamlit 500 MB
- Total ≈ 4.5 GB — leaves headroom for IDE/browser
- LocalExecutor only (no Celery/Redis — single-machine deployment)

---

## Author

**Dishant Vaswani** — BCA, IFHE University
Project under guidance of Ms. Kiran Preet Bedi
