from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def check_dlt_installed():
    import dlt
    print(f"dlt is installed: {dlt.__version__}")


with DAG(
    dag_id="check_dlt_installed",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    check_dlt = PythonOperator(
        task_id="check_dlt_installed",
        python_callable=check_dlt_installed,
    )
