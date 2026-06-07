from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import shared_lib.helpers as h

dag_domain = "calendar"

COMMON_CONF = {
    "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
    "spark.kubernetes.namespace": "spark-jobs",
    "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
    "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
    "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
    "spark.sql.catalog.nessie.ref": "dev",
    "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
}

COMMON_ENV = {
    "MINIO_URL": "{{ var.value.MINIO_URL }}",
    "MINIO_ACCESS_KEY": "{{ var.value.MINIO_ACCESS_KEY }}",
    "MINIO_SECRET_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
}


with DAG(
    dag_id=h.format_etl_dag_id(dag_domain),
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=h.generate_etl_dag_tags(dag_domain),
) as dag:
    bronze_task = SparkSubmitOperator(
        task_id=h.format_etl_bronze_dag_task_id(dag_domain),
        name=h.format_etl_bronze_dag_task_name(dag_domain),
        conn_id="spark_local",
        application="local:///opt/spark/jobs/bronze/ingestion_calendar_reference.py",
        properties_file="/app/spark/confs/spark-medium.conf",
        env_vars=COMMON_ENV,
        conf={
            **COMMON_CONF,
            "spark.openlineage.namespace": "bronze_ingestion",
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(dag_domain),
        },
        verbose=True,
    )

    silver_task = SparkSubmitOperator(
        task_id=h.format_etl_silver_dag_task_id(f"{dag_domain}_day_type"),
        name=h.format_etl_silver_dag_task_name(f"{dag_domain}_day_type"),
        conn_id="spark_local",
        application="local:///opt/spark/jobs/silver/calendar/silver_day_type_calendar.py",
        properties_file="/app/spark/confs/spark-medium.conf",
        env_vars=COMMON_ENV,
        conf={
            **COMMON_CONF,
            "spark.openlineage.namespace": "silver_ingestion",
            "spark.openlineage.appName": "calendar_silver_day_type_calendar",
        },
        verbose=True,
    )

    bronze_task >> silver_task
