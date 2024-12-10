from __future__ import annotations
import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import random

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
    def select_fruit():
        fruit = ['banana', 'orange', 'avocado']
        rand_int= random.randint(0,3)
        print(fruit[rand_int])


    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable=select_fruit
    )
    
    py_t1
        
