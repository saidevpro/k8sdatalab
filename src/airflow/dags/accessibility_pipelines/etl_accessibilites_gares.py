from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import shared_lib.helpers as h

dag_domain = "accessibilites_gares"

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
        application="local:///opt/spark/jobs/bronze/ingestion_prim_idfm_dataset.py",
        properties_file="/app/spark/confs/spark-small.conf",
        env_vars={
            "NESSIE_NAMESPACE": "bronze",
            "PRIM_DATASET_URI": "{{ var.value.PRIM_DATASET_URI }}",
            "DESTINATION_TABLE": "nessie.bronze.accessibility_gares",
            "PRIM_DATASET_TOKEN": "{{ var.value.PRIM_DATASET_TOKEN }}",
            "DATASET": "accessibilite-en-gare"
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
            "spark.openlineage.appName": h.format_etl_bronze_dag_task_name(dag_domain)
        },
        verbose=True,
    )

    silver_task = SparkSubmitOperator(
        task_id=h.format_etl_silver_dag_task_id(dag_domain),
        name=h.format_etl_silver_dag_task_name(dag_domain),
        conn_id="spark_cluster",
        application="local:///opt/spark/jobs/silver/accessibilites/transform_accessiblites_gares__silver.py",
        deploy_mode="cluster",
        properties_file="/app/spark/confs/spark-small.conf",
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "silver_transformation",
            "spark.openlineage.appName": h.format_etl_silver_dag_task_name(dag_domain)
        },
        verbose=True,
    )

    gold_task = SparkSubmitOperator(
        task_id=h.format_etl_gold_dag_task_id(dag_domain),
        name=h.format_etl_gold_dag_task_name(dag_domain),
        conn_id="spark_cluster",
        application="local:///opt/spark/jobs/gold/accessibility/gold_station_accessibility.py",
        deploy_mode="cluster",
        properties_file="/app/spark/confs/spark-small.conf",
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "gold_publication",
            "spark.openlineage.appName": h.format_etl_gold_dag_task_name(dag_domain)
        },
        verbose=True,
    )

    transfer_accessibility_domain = "transfer_accessibility"
    gold_transfer_accessibility_task = SparkSubmitOperator(
        task_id=h.format_etl_gold_dag_task_id(transfer_accessibility_domain),
        name=h.format_etl_gold_dag_task_name(transfer_accessibility_domain),
        conn_id="spark_cluster",
        application="local:///opt/spark/jobs/gold/accessibility/gold_transfer_accessibility.py",
        deploy_mode="cluster",
        properties_file="/app/spark/confs/spark-small.conf",
        conf={
            "spark.kubernetes.container.image": "saidsow/spark:3.5.8",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark",
            "spark.hadoop.fs.s3a.access.key": "{{ var.value.MINIO_ACCESS_KEY }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ var.value.MINIO_SECRET_KEY }}",
            "spark.sql.catalog.nessie.ref": "dev",
            "spark.sql.catalog.nessie.warehouse": "s3a://datalake/warehouse/",
            "spark.openlineage.namespace": "gold_publication",
            "spark.openlineage.appName": h.format_etl_gold_dag_task_name(transfer_accessibility_domain)
        },
        verbose=True,
    )

    bronze_task >> silver_task >> [gold_task, gold_transfer_accessibility_task]
