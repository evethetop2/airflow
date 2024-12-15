from airflow import DAG
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
from datetime import datetime


def read_table(**kwargs):
    mysql_hook = MySqlHook(mysql_conn_id='hyperconnect')
    query = "SELECT * FROM table_a"
    result = mysql_hook.get_pandas_df(query)
    df = pd.DataFrame(result, columns=['dt','hr','value'])
    kwargs['ti'].xcom_push(key='df', value=df.to_json(orient='split'))
    print("DataFrame has been pushed to XCom")
    return df

def process_dataframe(**kwargs):
    # XCom에서 DataFrame JSON 가져오기
    df_json = kwargs['ti'].xcom_pull(key='df', task_ids='read_table_task')

    # JSON을 다시 DataFrame으로 변환
    # df = pd.read_json(df_json, orient='split')
    df_json[['id', 'user_name']] = df_json['value'].apply(lambda x: pd.Series(json.loads(x)))
    df_json.drop('value', axis=1, inplace=True)
    df_json['dt'] = pd.to_datetime(df_json['dt'], format='%Y/%m/%d')

    print("Received DataFrame:")
    print(df_json)


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

        # Task 2: DataFrame 처리
    process_dataframe_task = PythonOperator(
        task_id='process_dataframe_task',
        python_callable=process_dataframe,
        provide_context=True
    )

    read_table_task >> process_dataframe_task
