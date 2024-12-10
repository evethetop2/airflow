from __future__ import annotations
import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# dag이름은 dag_id, 파이썬 파일명이랑 일치시켜
with DAG(
    dag_id="dags_bash_operator",
    #시간임
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    #누락된거 돌릴거냐 안돌릴거냐, 차례차례 안돌고 한번에 돌게됨
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:

    # [START howto_operator_bash]
    bash_t1 = BashOperator(
        #그래프에 표출되는 이름
        task_id="bash_t1",
        bash_command="echo whoami",
    )    

    bash_t2 = BashOperator(
        #그래프에 표출되는 이름
        task_id="bash_t2",
        #호스트네임 출력하라
        bash_command="echo $HOSTNAME",
    ) 
    bash_t1 >> bash_t2
