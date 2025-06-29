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
    dag_id='combined_dag',
    default_args=default_args,
    description='ETL for Azure, API, and MySQL sources using PySpark',
    schedule_interval='@hourly',  # Or change to '@daily' if preferred
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'spark', 'azure', 'api', 'mysql']
) as dag:

    azure_etl = BashOperator(
        task_id='run_azure_etl',
        bash_command='spark-submit /home/ipsitaroy/lirik_internship/azure_transform.py'
    )

    api_etl = BashOperator(
        task_id='run_api_etl',
        bash_command='spark-submit /home/ipsitaroy/lirik_internship/api_transform.py'
    )

    mysql_etl = BashOperator(
        task_id='run_mysql_etl',
        bash_command='spark-submit /home/ipsitaroy/lirik_internship/mysql_transform.py'
    )

    # Define order if needed (e.g. API & MySQL after Azure)
    azure_etl >> [api_etl, mysql_etl]
