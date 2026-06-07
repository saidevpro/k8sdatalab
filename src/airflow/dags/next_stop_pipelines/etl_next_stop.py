from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import shared_lib.helpers as h


dag_domain = "next_stop"

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"


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
        application="local:///opt/spark/jobs/bronze/ingestion_stream_next_stop.py",
        properties_file="/app/spark/confs/spark-small.conf",
        packages=KAFKA_PACKAGE,
        env_vars={
            "KAFKA_BOOTSTRAP_SERVERS": "{{ var.value.KAFKA_BOOTSTRAP_SERVERS }}",
            "KAFKA_TOPIC": "idfm-next-stop-raw",
            "KAFKA_GROUP_ID_PREFIX": "spark-idfm-next-stop-iceberg-writer",
            "CHECKPOINT_LOCATION": "s3a://spark-checkpoints/bronze/next_stop/",
            "TRIGGER_INTERVAL": "60 seconds",
            "NESSIE_NAMESPACE": "bronze",
            "DESTINATION_TABLE": "nessie.bronze.next_stop",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "bronze_ingestion",
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(dag_domain),
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        verbose=True,
    )

    silver_stream_task = SparkSubmitOperator(
        task_id=h.format_etl_silver_dag_task_id(dag_domain),
        name=h.format_etl_silver_dag_task_name(dag_domain),
        conn_id="spark_cluster",
        deploy_mode="cluster",
        application="local:///opt/spark/jobs/silver/next_stop/silver_next_stop.py",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "SOURCE_TABLE": "nessie.bronze.next_stop",
            "NESSIE_CATALOG": "nessie",
            "SILVER_NAMESPACE": "silver",
            "CHECKPOINT_LOCATION": "s3a://spark-checkpoints/silver/next_stop/",
            "TRIGGER_INTERVAL": "60 seconds",
            "MAX_FILES_PER_MICRO_BATCH": "100",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "silver_transformation",
            "spark.openlineage.appName": h.format_etl_silver_dag_task_name(dag_domain),
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        verbose=True,
    )

    gold_task = SparkSubmitOperator(
        task_id=h.format_etl_gold_dag_task_id(dag_domain),
        name=h.format_etl_gold_dag_task_name(dag_domain),
        conn_id="spark_cluster",
        deploy_mode="cluster",
        application="local:///opt/spark/jobs/gold/next_stop/gold_next_stop.py",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "SOURCE_TABLE": "nessie.silver.next_stop",
            "NESSIE_CATALOG": "nessie",
            "SILVER_NAMESPACE": "silver",
            "GOLD_NAMESPACE": "gold",
            "CHECKPOINT_LOCATION": "s3a://spark-checkpoints/gold/next_stop/",
            "TRIGGER_INTERVAL": "60 seconds",
            "MAX_FILES_PER_MICRO_BATCH": "100",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "gold_publication",
            "spark.openlineage.appName": h.format_etl_gold_dag_task_name(dag_domain),
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        verbose=True,
    )

    features_task = SparkSubmitOperator(
        task_id="publish_next_stop_features__gold",
        name="publishing-next-stop-features--gold",
        conn_id="spark_cluster",
        deploy_mode="cluster",
        application="local:///opt/spark/jobs/gold/next_stop/next_stop_features.py",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "NESSIE_CATALOG": "nessie",
            "SILVER_NAMESPACE": "silver",
            "GOLD_NAMESPACE": "gold",
            "DELAY_THRESHOLD_SEC": "60",
        },
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "gold_publication",
            "spark.openlineage.appName": "publishing-next-stop-features--gold",
        },
        retries=2,
        retry_delay=timedelta(minutes=5),
        verbose=True,
    )
