from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from datetime import datetime

def read_table():
    mysql_hook = MySqlHook(mysql_conn_id='hyperconnect')
    sql = """
    select *
    from table_a
    ;
    """
    a= mysql_hook.run(sql)
    return a

with DAG(
    dag_id='read_table_using_hook',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    read_table_task = PythonOperator(
        task_id='read_table_task',
        python_callable=read_table,
        retries=1,  # Manually set retry parameters
        retry_delay=timedelta(minutes=5),
    )

    read_table_task
