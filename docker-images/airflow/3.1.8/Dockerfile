FROM saidsow/spark:3.5.8 AS spark

FROM apache/airflow:3.1.8

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark /opt/spark /opt/spark
COPY .confs/spark /app/spark/confs

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark"