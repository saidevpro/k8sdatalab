from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import shared_lib.helpers as h

dag_domain = "validations"

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

# Silver jobs to process — one Spark job per file
SILVER_JOBS = {
    "validations_daily": {
        "app": "silver/validations/silver_validations_daily.py",
        "conf": "spark-medium",
        "conn_id": "spark_cluster",
    },
    "validations_hourly_profile": {
        "app": "silver/validations/silver_validations_hourly_profile.py",
        "conf": "spark-medium",
        "conn_id": "spark_cluster",
    },
}

# Gold jobs to publish — one Spark job per file
GOLD_JOBS = {
    "validations_dashboards": {
        "app": "gold/validations/gold_validations_dashboards.py",
        "conf": "spark-medium",
        "conn_id": "spark_cluster",
    },
    "crowding_features": {
        "app": "gold/validations/crowding_features.py",
        "conf": "spark-large",
        "conn_id": "spark_cluster",
    },
}


with DAG(
    dag_id=h.format_etl_dag_id(dag_domain),
    start_date=datetime(2026, 1, 1),
    schedule="@yearly",
    catchup=False,
    max_active_tasks=2,
    tags=h.generate_etl_dag_tags(dag_domain),
) as dag:
    bronze_task = SparkSubmitOperator(
        task_id=h.format_etl_bronze_dag_task_id(dag_domain),
        name=h.format_etl_bronze_dag_task_name(dag_domain),
        conn_id="spark_cluster",
        application="local:///opt/spark/jobs/bronze/ingestion_validations_voies_ferres.py",
        properties_file="/app/spark/confs/spark-medium.conf",
        env_vars={
            "PRIM_DATASET_URI": "{{ var.value.PRIM_DATASET_URI }}",
            "PRIM_DATASET_TOKEN": "{{ var.value.PRIM_DATASET_TOKEN }}",
            **COMMON_ENV,
        },
        conf={
            **COMMON_CONF,
            "spark.openlineage.namespace": "bronze_ingestion",
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(dag_domain),
        },
        verbose=True,
    )

    silver_tasks = {}
    with TaskGroup(group_id="silver_tasks") as silver_task_group:
        for job, settings in SILVER_JOBS.items():
            job_domain = f"{dag_domain}_{job}"
            silver_tasks[job] = SparkSubmitOperator(
                task_id=h.format_etl_silver_dag_task_id(job_domain),
                name=h.format_etl_silver_dag_task_name(job_domain),
                conn_id=settings["conn_id"],
                application=f"local:///opt/spark/jobs/{settings['app']}",
                properties_file=f"/app/spark/confs/{settings['conf']}.conf",
                env_vars=COMMON_ENV,
                conf={
                    **COMMON_CONF,
                    "spark.openlineage.namespace": "silver_ingestion",
                    "spark.openlineage.appName": f"validations_silver_{job}",
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
                    "spark.openlineage.appName": f"validations_gold_{job}",
                },
                trigger_rule=TriggerRule.ALL_DONE,
                verbose=True,
            )

    bronze_task >> silver_task_group >> gold_task_group
