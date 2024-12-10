from __future__ import annotations
import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from plugins.common.common_func import get_sftp

# dag이름은 dag_id, 파이썬 파일명이랑 일치시켜
with DAG(
    dag_id="dag_python_operator",
    #시간임
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    #누락된거 돌릴거냐 안돌릴거냐, 차례차례 안돌고 한번에 돌게됨
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:

    task_get_sftp = PythonOperator( 
        task_id = 'dags_python_import_func',
        python_callable=get_sftp
    )

        
