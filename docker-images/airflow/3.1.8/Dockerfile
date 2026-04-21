FROM apache/airflow:3.1.8

USER root

ARG SPARK_VERSION=3.5.8
ARG HADOOP_VERSION=3

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tar \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt \
    && ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark" \
    "apache-airflow-providers-amazon"