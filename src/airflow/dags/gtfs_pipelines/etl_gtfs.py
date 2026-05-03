from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
import shared_lib.helpers as h

dag_domain = "gtfs"

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

# Silver tables to process — one Spark job per table
SILVER_TABLES = [
    "calendar_dates",
    "calendars",
    "trips",
    "pathways",
    "stop_times",
    "routes",
    "wheelchairs",
    "stop_entrances",
    "stations",
    "stop_points",
]


def get_conf(table):
    return 'spark-medium' if table != 'stop_times' else 'spark-large'


def get_conn_id(table):
    return 'spark_local' if table != 'stop_times' else 'spark_cluster'


with DAG(
    dag_id=h.format_etl_dag_id(dag_domain),
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=h.generate_etl_dag_tags(dag_domain),
) as dag:
    bronze_task = SparkSubmitOperator(
        task_id=h.format_etl_bronze_dag_task_id(dag_domain),
        name=h.format_etl_bronze_dag_task_name(dag_domain),
        conn_id="spark_local",
        application="local:///opt/spark/jobs/bronze/ingestion_gtfs_idfm_dataset.py",
        deploy_mode="client",
        properties_file="/app/spark/confs/spark-medium.conf",
        env_vars={
            "PRIM_DATASET_URI": "{{ var.value.PRIM_DATASET_URI }}",
            "PRIM_TOKEN": "{{ var.value.PRIM_TOKEN }}",
            **COMMON_ENV,
        },
        conf={
            **COMMON_CONF,
            "spark.openlineage.namespace": "bronze_ingestion",
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(dag_domain),
        },
        verbose=True,
    )

    with TaskGroup(group_id="silver_tasks") as silver_task_group:
        for table in SILVER_TABLES:
            table_domain = f"{dag_domain}_{table}"
            SparkSubmitOperator(
                task_id=h.format_etl_silver_dag_task_id(table_domain),
                name=h.format_etl_bronze_dag_task_name(table_domain),
                conn_id=get_conn_id(table),
                application=f"local:///opt/spark/jobs/silver/gtfs/transform_{table}.py",
                properties_file=f"/app/spark/confs/{get_conf(table)}.conf",
                env_vars=COMMON_ENV,
                conf={
                    **COMMON_CONF,
                    "spark.openlineage.namespace": "silver_ingestion",
                    "spark.openlineage.appName": f"gtfs_silver_{table}",
                },
                verbose=True,
            )

    bronze_task >> silver_task_group
