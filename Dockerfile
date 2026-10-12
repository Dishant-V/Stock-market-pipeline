FROM apache/airflow:2.10.4-python3.11
COPY requirements.txt /
RUN pip install --default-timeout=120 --no-cache-dir -r /requirements.txt
