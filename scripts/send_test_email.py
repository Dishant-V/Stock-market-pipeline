"""Standalone SMTP alert sender - testable outside Airflow.

Usage: python scripts/send_test_email.py [message]
Reads ALERT_EMAIL_TO / SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASSWORD
from environment variables (.env via docker compose).
"""
import os
import smtplib
import sys
from email.mime.text import MIMEText


def send_email(subject: str, body: str, recipient: str) -> None:
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASSWORD")

    if not user or not password:
        raise RuntimeError("SMTP_USER / SMTP_PASSWORD not set - check your .env file")

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = recipient

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(user, password)
        server.send_message(msg)


def main() -> None:
    recipient = os.environ.get("ALERT_EMAIL_TO", "")
    if not recipient or "@" not in recipient:
        raise RuntimeError("ALERT_EMAIL_TO not set to a valid email - check your .env file")

    body = sys.argv[1] if len(sys.argv) > 1 else (
        "Test alert from your stock watchlist pipeline.\n"
        "If you are reading this, SMTP delivery works end-to-end."
    )
    send_email("[StockWatch] Test Alert", body, recipient)
    print(f"Email sent successfully to {recipient}")


if __name__ == "__main__":
    main()
