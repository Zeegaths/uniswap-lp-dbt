FROM apache/airflow:3.3.1-python3.12

USER root
# curl is used by the compose healthchecks
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-fab" \
    "web3>=7,<8" \
    "psycopg2-binary>=2.9" \
    "python-dotenv>=1.0" \
    "dbt-postgres>=1.9,<2"
