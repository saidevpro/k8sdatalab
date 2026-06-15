FROM public.ecr.aws/emr-serverless/spark/emr-7.9.0:latest

USER root

RUN curl -fsSL -o /usr/lib/spark/jars/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.1/iceberg-spark-runtime-3.5_2.12-1.10.1.jar \
 && curl -fsSL -o /usr/lib/spark/jars/nessie-spark-extensions-3.5_2.12-0.104.5.jar \
      https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.104.5/nessie-spark-extensions-3.5_2.12-0.104.5.jar \
 && curl -fsSL -o /usr/lib/spark/jars/trino-jdbc-469.jar \
      https://repo1.maven.org/maven2/io/trino/trino-jdbc/469/trino-jdbc-469.jar \
 && curl -fsSL -o /usr/lib/spark/jars/openlineage-spark-1.46.0.jar \
      https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.46.0/openlineage-spark_2.12-1.46.0.jar

RUN python3 -m pip install --no-cache-dir \
    requests \
    boto3 \
    pandas \
    pyarrow \
    pynessie \
    mlflow

COPY .confs/spark /app/spark/confs
COPY src/spark/jobs /opt/spark/jobs

RUN chown -R hadoop:hadoop /app/spark/confs /opt/spark/jobs \
    && chmod -R g=u /app/spark/confs /opt/spark/jobs

USER hadoop:hadoop

