FROM apache/airflow:2.9.1-python3.11

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==2.9.1" \
    -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.11.txt"