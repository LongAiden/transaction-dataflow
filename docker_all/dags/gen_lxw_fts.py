from __future__ import annotations
import pendulum
import subprocess
import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'longnv',
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'pool': 'transaction_data',
    'max_active_runs': 1,
}
def get_execution_date(**kwargs):
    execution_date = kwargs['execution_date']
    local_tz = pendulum.timezone('Asia/Bangkok')
    local_execution_date = execution_date.in_timezone(local_tz)
    return local_execution_date.strftime('%Y-%m-%d')


with DAG(
    dag_id="gen_lxw_fts",
    schedule_interval="0 23 * * *",
    start_date=datetime(2025,3,7),
    default_args=default_args,
    catchup=True,
    tags=['workflow'],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    # Task to get the execution date
    get_date = PythonOperator(
        task_id='get_date',
        python_callable=get_execution_date,
        provide_context=True
    )

    # Task to run the Python script
    calculate_fts = BashOperator(
        task_id='calculate_fts',
        bash_command=f'''
        python /opt/airflow/external_scripts/2_calculate_features.py {{{{ ti.xcom_pull(task_ids='get_date') }}}}
        '''
    )
    
    start >> get_date >> calculate_fts >> end
