from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import shared_lib.helpers as h

dag_domain = "validations"

with DAG(
    dag_id=h.format_etl_dag_id(dag_domain),
    start_date=datetime(2026, 1, 1),
    schedule="@yearly",
    catchup=False,
    tags=h.generate_etl_dag_tags(dag_domain),
) as dag:
    domain_journalier = f"{dag_domain}_journalier"
    
    bronze_task = SparkSubmitOperator(
        task_id=h.format_etl_bronze_dag_task_id(domain_journalier),
        name=h.format_etl_bronze_dag_task_name(domain_journalier),
        conn_id="spark_cluster",
        application="local:///opt/spark/jobs/bronze/ingestion_validations_voies_ferres.py",
        properties_file="/app/spark/confs/spark-medium.conf",
        env_vars={
            "PRIM_DATASET_URI": "{{ var.value.PRIM_DATASET_URI }}",
            "PRIM_DATASET_TOKEN": "{{ var.value.PRIM_DATASET_TOKEN }}",
            "MINIO_URL": "{{ var.value.MINIO_URL }}",
    "MINIO_ACCESS_KEY": "{{ var.value.MINIO_ACCESS_KEY }}",
    "MINIO_SECRET_KEY": "{{ var.value.MINIO_SECRET_KEY }}",
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
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(domain_journalier)
        },
        verbose=True,
    )
