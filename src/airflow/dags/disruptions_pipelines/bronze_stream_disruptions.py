from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import shared_lib.helpers as h


dag_domain = "stream_disruptions"

SPARK_VERSION = "3.5.8"
SCALA_VERSION = "2.12"
KAFKA_PACKAGE = (
    f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
)


with DAG(
    dag_id=h.format_etl_dag_id(dag_domain),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[*h.generate_etl_dag_tags(dag_domain), "streaming", "kafka"],
) as dag:
    bronze_stream_task = SparkSubmitOperator(
        task_id=h.format_etl_bronze_dag_task_id(dag_domain),
        name=h.format_etl_bronze_dag_task_name(dag_domain),
        conn_id="spark_cluster",
        deploy_mode="cluster",
        application="local:///opt/spark/jobs/bronze/ingestion_stream_disruptions.py",
        properties_file="/app/spark/confs/spark-small.conf",
        packages=KAFKA_PACKAGE,
        env_vars={
            "KAFKA_BOOTSTRAP_SERVERS": "{{ var.value.KAFKA_BOOTSTRAP_SERVERS }}",
            "KAFKA_TOPIC": "idfm-disruptions-raw",
            "KAFKA_GROUP_ID_PREFIX": "spark-idfm-disruptions-iceberg-writer",
            "CHECKPOINT_LOCATION": "s3a://spark-checkpoints/bronze/disruptions/",
            "TRIGGER_INTERVAL": "60 seconds",
            "NESSIE_NAMESPACE": "bronze",
            "DESTINATION_TABLE": "nessie.bronze.disruptions",
        },
        conf={
            "spark.kubernetes.container.image": f"saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "bronze_ingestion",
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(dag_domain),
        },
        retries=0,
        verbose=True,
    )
