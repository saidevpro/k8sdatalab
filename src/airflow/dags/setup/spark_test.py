from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="test_spark",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["setup", "test", "spark"]
) as dag:
    spark_test = SparkSubmitOperator(
        task_id="test_iceberg_nessie_eventlogs",
        application="local:///opt/spark/jobs/setup/test_archi.py",
        name="test-iceberg-nessie-eventlogs",
        conn_id="spark_local",
        properties_file="/app/spark/confs/spark-small.conf",
        application_args=[
            "--catalog", "nessie",
            "--namespace", "smoke_test",
            "--table", "airflow_smoke_test",
            "--rows", "10",
        ],
        conf={
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
        }
    )
