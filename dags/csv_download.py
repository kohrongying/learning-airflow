"""
dag1- download csv using wget to localdisk
dag2 - poll csv file in filesystem. parse csv and print schema
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG('csv_download_dag', default_args=default_args)

output_path = "/Users/rongying/airflow/downloads/address.csv"

download_csv = f"""
    curl https://people.sc.fsu.edu/~jburkardt/data/csv/addresses.csv -o {output_path}
"""


t1 = BashOperator(
    task_id='download_task',
    bash_command=download_csv,
    dag=dag)

t2 = TriggerDagRunOperator(
    task_id='trigger_task',
    trigger_dag_id='csv_read_dag',
    dag=dag
)

t1 >> t2

# airflow tasks test csv_download_dag download_task 2021-11-01