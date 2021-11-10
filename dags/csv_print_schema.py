"""
dag1- download csv using wget to localdisk
dag2 - poll csv file in filesystem. parse csv and print schema
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
import csv 

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

dag = DAG('csv_read_dag', default_args=default_args)

output_path = "/Users/rongying/airflow/downloads/address.csv"

t1 = FileSensor(
    task_id="poll_task",
    filepath=output_path,
    timeout=300,
    poke_interval=10,
    dag=dag
)

def parse_csv(filename):
    with open(filename, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        header = next(reader)
        print(header)


t2 = PythonOperator(
    task_id="print_task",
    python_callable=parse_csv,
    op_kwargs={'filename': output_path},
    dag=dag
)

t1 >> t2

# airflow tasks test read_csv_dag poll_task 2021-11-01