FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV SPARK_VERSION=3.5.8
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Spark
RUN wget -q https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && rm -rf ${SPARK_HOME}/examples \
    && rm -rf ${SPARK_HOME}/data \
    && rm -rf ${SPARK_HOME}/R \
    && rm -rf ${SPARK_HOME}/kubernetes/tests

# JARs
RUN wget -q -P ${SPARK_HOME}/jars \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.104.5/nessie-spark-extensions-3.5_2.12-0.104.5.jar \
    https://repo1.maven.org/maven2/io/trino/trino-jdbc/469/trino-jdbc-469.jar

# Python deps
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt