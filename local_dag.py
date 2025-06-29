from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'ipsita',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='local_dag',
    default_args=default_args,
    description='ETL for local CSVs using a single PySpark script',
    schedule_interval='@hourly',  # or '@daily' if you prefer
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'csv', 'spark']
) as dag:

    run_csv_etl = BashOperator(
        task_id='run_local_etl',
        bash_command='spark-submit /home/ipsitaroy/lirik_internship/local_transform.py'
    )

    run_csv_etl
