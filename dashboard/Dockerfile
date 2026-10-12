FROM apache/airflow:2.10.4-python3.11
COPY dashboard/requirements.txt /tmp/
RUN pip install --default-timeout=300 --retries=5 --no-cache-dir -r /tmp/requirements.txt