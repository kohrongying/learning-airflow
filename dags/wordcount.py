from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('wordcount_dag', default_args=default_args)

path_to_wc = "/Users/rongying/Documents/data-engineering/de202/transformations"

spark_submit_wordcount_locally = f"""
    spark-submit --class thoughtworks.wordcount.WordCount --master local {path_to_wc}/target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar {path_to_wc}/src/test/resources/data/words.txt {path_to_wc}/target/wordcount-airflow
"""


t1 = BashOperator(
    task_id='count_words_task',
    bash_command=spark_submit_wordcount_locally,
    dag=dag)

# airflow tasks test wordcount_dag count_words_task 2021-11-01