FROM apache/spark:3.5.8

USER root

ENV HOME=/home/spark
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

ARG COMPONENT=docker-images/confs/spark

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    ca-certificates \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
 && curl -fsSL -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.104.5.jar \
      https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.104.5/nessie-spark-extensions-3.5_2.12-0.104.5.jar \
 && curl -fsSL -o /opt/spark/jars/trino-jdbc-469.jar \
      https://repo1.maven.org/maven2/io/trino/trino-jdbc/469/trino-jdbc-469.jar

RUN mkdir -p /home/spark && chown -R spark:root /home/spark /opt/spark

COPY ${COMPONENT} /app/spark/confs

WORKDIR /opt/spark/work-dir

USER spark