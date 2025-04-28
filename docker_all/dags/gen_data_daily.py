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

current_date_str = '{{(execution_date + macros.timedelta(days=0)).in_timezone("Asia/Ho_Chi_Minh").strftime("%Y-%m-%d")}}'

default_args = {
    'owner': 'longnv',
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'pool': 'transaction_data',
    'max_active_runs': 1,
}

with DAG(
    dag_id="gen_data_daily",
    schedule_interval="0 22 * * *",
    start_date=datetime(2025,3,1),
    end_date=datetime(2025,3,20),
    default_args=default_args,
    catchup=True,
    tags=['workflow'],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    # Task to run the Python script
    gen_data_daily = BashOperator(
        task_id='gen_data_daily',
        bash_command=f'''
        python /opt/airflow/external_scripts/step_1_1_gen_transaction_data.py {current_date_str}
        '''
    )
    
    get_data_daily = BashOperator(
        task_id='get_data_daily',
        bash_command=f'''
        python /opt/airflow/external_scripts/step_1_2_gen_transaction_data.py {current_date_str}
        '''
    )

    start >> gen_data_daily >> get_data_daily >> end
