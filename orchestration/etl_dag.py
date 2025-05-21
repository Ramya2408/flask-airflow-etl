from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {"owner": "airflow", "retries": 1}

with DAG('etl_dag',
         start_date=datetime(2025, 5, 22),
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False) as dag:

    def run_etl():
        subprocess.run(['python', '../etl/main.py'], check=True)

    def run_api():
        # (optional) hit API for warm-up or tests
        pass

    t1 = PythonOperator(task_id='run_etl', python_callable=run_etl)
    # t2 = PythonOperator(task_id='run_api', python_callable=run_api)

    t1  # >> t2
