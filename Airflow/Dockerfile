FROM apache/airflow:3.1.8

USER root
# Installation du JDK (Java 17 par exemple)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Installation du provider Great Expectations
RUN pip install airflow-provider-great-expectations