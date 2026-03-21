FROM apache/airflow:2.8.1

# Install additional python dependencies
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Spark requirements (Java)
USER root
RUN apt-get update && \
    apt-get install -y default-jdk && \
    apt-get clean
USER airflow
