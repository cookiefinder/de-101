from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'zijie',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 17, 4, 20),
    'retries': 1,
    'relay_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow_demo',
    default_args=default_args,
    schedule_interval=timedelta(days=1)
)

task1 = BashOperator(
    task_id='task1',
    bash_command='/Users/zijie.jiang/Documents/IdeaProjects/data-engineering/DE/my-venv/bin/python3 /Users/zijie.jiang/Documents/IdeaProjects/data-engineering/DE/demo.py',
    dag=dag
)