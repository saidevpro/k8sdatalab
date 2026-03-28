from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print("Hello from Airflow with OpenLineage!")

with DAG(
    dag_id="test_lineage",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False
) as dag:
    task = PythonOperator(
        task_id="hello",
        python_callable=my_task
    )