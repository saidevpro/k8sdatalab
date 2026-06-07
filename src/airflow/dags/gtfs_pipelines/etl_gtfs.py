from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
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
SILVER_TABLES = {
    "calendar_dates": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "calendars": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "trips": {
        "conf": "spark-medium",
        "conn_id": "spark_cluster"
    },
    "pathways": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "transfers": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "stop_times": {
        "conf": "spark-large",
        "conn_id": "spark_cluster"
    },
    "routes": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "wheelchairs": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "stop_entrances": {
        "conf": "spark-medium",
        "conn_id": "spark_cluster"
    },
    "stations": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "stop_points": {
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
}

# Gold jobs to publish — one Spark job per file
GOLD_JOBS = {
    "dim_stops": {
        "app": "gold/gtfs/dim_stops.py",
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
    "trip_schedule": {
        "app": "gold/gtfs/trip_schedule.py",
        "conf": "spark-large",
        "conn_id": "spark_cluster"
    },
    "service_calendar": {
        "app": "gold/gtfs/service_calendar.py",
        "conf": "spark-medium",
        "conn_id": "spark_cluster"
    },
    "transfer_walking": {
        "app": "gold/gtfs/transfer_walking.py",
        "conf": "spark-medium",
        "conn_id": "spark_local"
    },
}


with DAG(
    dag_id=h.format_etl_dag_id(dag_domain),
    start_date=datetime(2026, 5, 16),
    schedule="0 */4 * * *",
    catchup=False,
    max_active_tasks=3,
    tags=h.generate_etl_dag_tags(dag_domain),
) as dag:
    bronze_task = SparkSubmitOperator(
        task_id=h.format_etl_bronze_dag_task_id(dag_domain),
        name=h.format_etl_bronze_dag_task_name(dag_domain),
        conn_id="spark_local",
        application="local:///opt/spark/jobs/bronze/ingestion_gtfs_idfm_dataset.py",
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
        for table, settings in SILVER_TABLES.items():
            table_domain = f"{dag_domain}_{table}"
            SparkSubmitOperator(
                task_id=h.format_etl_silver_dag_task_id(table_domain),
                name=h.format_etl_bronze_dag_task_name(table_domain),
                conn_id=settings["conn_id"],
                application=f"local:///opt/spark/jobs/silver/gtfs/transform_{table}.py",
                properties_file=f"/app/spark/confs/{settings['conf']}.conf",
                env_vars=COMMON_ENV,
                conf={
                    **COMMON_CONF,
                    "spark.openlineage.namespace": "silver_ingestion",
                    "spark.openlineage.appName": f"gtfs_silver_{table}",
                },
                verbose=True,
            )

    with TaskGroup(group_id="gold_tasks") as gold_task_group:
        for job, settings in GOLD_JOBS.items():
            job_domain = f"{dag_domain}_{job}"
            SparkSubmitOperator(
                task_id=h.format_etl_gold_dag_task_id(job_domain),
                name=h.format_etl_gold_dag_task_name(job_domain),
                conn_id=settings["conn_id"],
                application=f"local:///opt/spark/jobs/{settings['app']}",
                properties_file=f"/app/spark/confs/{settings['conf']}.conf",
                env_vars=COMMON_ENV,
                conf={
                    **COMMON_CONF,
                    "spark.openlineage.namespace": "gold_publication",
                    "spark.openlineage.appName": f"gtfs_gold_{job}",
                },
                trigger_rule=TriggerRule.ALL_DONE,
                verbose=True,
            )

    bronze_task >> silver_task_group >> gold_task_group
