from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowSkipException
import pendulum
import pandas as pd
import json
from datetime import datetime, timezone

def convert_timestamp(ts):
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d')


def check_previous_date_exists(**kwargs):
    # MySQL 연결
    mysql_hook = MySqlHook(mysql_conn_id='hyperconnect')

    # 쿼리 실행: 2024/12/8일 이전 날짜가 존재하는지 확인
    flag = """
    SELECT COUNT(*) FROM table_a WHERE dt <= '2024-12-08';
    """
    result = mysql_hook.get_first(flag)
    
    # 결과가 0이면 이전 날짜가 존재하지 않음
    if result[0] == 0:
        print("이전 날짜가 존재하지 않습니다.")
        raise AirflowSkipException()
    else:
        print("이전 날짜가 존재합니다.")
        query = "SELECT * FROM table_a"
        result = mysql_hook.get_pandas_df(query)
        df = pd.DataFrame(result, columns=['dt','hr','value'])
        kwargs['ti'].xcom_push(key='df', value=df.to_json(orient='split'))
        print("DataFrame has been pushed to XCom")
        print(df)


def insert_data_to_table_b(**kwargs):
    # XCom에서 DataFrame JSON 가져오기
    df_json = kwargs['ti'].xcom_pull(key='df', task_ids='check_previous_date_sensor')

    # JSON을 다시 DataFrame으로 변환
    df = pd.read_json(df_json, orient='split')
    df[['id', 'user_name']] = df['value'].apply(lambda x: pd.Series(json.loads(x)))
    df.drop('value', axis=1, inplace=True)
    df['dt'] = df['dt'].apply(convert_timestamp)
    print(df)
    
    #insert를 위한 객체 생성
    mysql_hook = MySqlHook(mysql_conn_id='hyperconnect')
    conn = mysql_hook.get_conn()  # 연결 객체 가져오기
    cursor = conn.cursor()  # 커서 객체 생성
    
    # 데이터를 table B에 삽입
    for i, row in df.iterrows():
        insert_query = """
        INSERT INTO table_b (dt, hr, id, user_name)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (row['dt'], row['hr'], row['id'], row['user_name']))

    print("데이터가 정상적으로 삽입되었습니다.")

def wait_for_previous_date(**kwargs):
    try:
        check_previous_date_exists(**kwargs)
        return True
    except AirflowSkipException:
        return False

# DAG 정의
dag = DAG(
    'check_and_insert',
    start_date=pendulum.datetime(2024, 12, 1, tz="Asia/Seoul"),
    schedule="0 10 * * *",
    catchup=False
)

check_date_sensor = PythonSensor(
    task_id='check_previous_date_sensor',
    python_callable=wait_for_previous_date,
    poke_interval=10,
    timeout=60,
    mode='poke',
    dag=dag
)

insert_data = PythonOperator(
    task_id='insert_data_to_table_b',
    python_callable=insert_data_to_table_b,
    dag=dag
)

# 작업 순서: 날짜가 존재하면 insert 작업 실행, 존재하지 않으면 skip
check_date_sensor >> insert_data
