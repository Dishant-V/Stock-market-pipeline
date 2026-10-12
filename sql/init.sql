-- App database (separate schema from Airflow metadata DB)
CREATE DATABASE IF NOT EXISTS stocks CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY 'apppass';
GRANT ALL PRIVILEGES ON stocks.* TO 'app'@'%';
FLUSH PRIVILEGES;

USE stocks;

CREATE TABLE IF NOT EXISTS watchlist (
    ticker          VARCHAR(10)     NOT NULL PRIMARY KEY,
    threshold_type  ENUM('pct_drop','price_below','price_above') NOT NULL DEFAULT 'pct_drop',
    threshold_value DECIMAL(10,4)   NOT NULL,
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS price_history (
    id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    ticker      VARCHAR(10)     NOT NULL,
    price       DECIMAL(12,4)   NOT NULL,
    prev_close  DECIMAL(12,4)   NULL,
    pct_change  DECIMAL(8,4)    NULL,
    fetched_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ticker_time (ticker, fetched_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alerts_fired (
    id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    ticker          VARCHAR(10)     NOT NULL,
    rule_type       VARCHAR(20)     NOT NULL,
    threshold_value DECIMAL(10,4)   NULL,
    observed_value  DECIMAL(12,4)   NULL,
    message         TEXT            NULL,
    emailed_to      VARCHAR(255)    NULL,
    fired_at        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ticker_time (ticker, fired_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed: deliberately LOW thresholds so an alert fires quickly during testing.
INSERT INTO watchlist (ticker, threshold_type, threshold_value, is_active) VALUES
    ('AAPL',  'pct_drop', 0.5000, TRUE),
    ('MSFT',  'pct_drop', 0.5000, TRUE),
    ('GOOGL', 'pct_drop', 0.5000, TRUE)
ON DUPLICATE KEY UPDATE threshold_type = VALUES(threshold_type);
