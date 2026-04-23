from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="bootstrap_nessie_catalog",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["setup", "spark", "nessie"]
) as dag:
    ensure_nessie_namespaces = SparkSubmitOperator(
        task_id="ensure_nessie_namespaces",
        conn_id="spark_local",
        application="local:///opt/spark/jobs/setup_nessie_catalog.py",
        name="ensure-nessie-namespaces",
        deploy_mode="client",
        properties_file="/app/spark/confs/spark-small.conf",
        verbose=True,
        conf={
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
        }
    )