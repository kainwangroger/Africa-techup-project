FROM apache/airflow:2.9.3-python3.10

USER root

# Dépendances système (optionnel mais fréquent)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    vim \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY airflow_requirements.txt /airflow_requirements.txt

USER airflow

# Providers Airflow (à adapter à ton projet)
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-redis \
    apache-airflow-providers-amazon \
    apache-airflow-providers-cncf-kubernetes \
    minio

RUN pip install --no-cache-dir -r /airflow_requirements.txt
