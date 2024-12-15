from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowSkipException
import pandas as pd
import json
from datetime import datetime, timezone

def convert_timestamp(ts):
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime('%Y-%m-%d')


def check_previous_date_exists(**kwargs):
    """
    이전 날짜가 존재하는지 확인하고,
    존재하지 않으면 AirflowSkipException을 발생시킴.
    """
    # 연결 정보로 MySQL 연결
    mysql_hook = MySqlHook(mysql_conn_id='hyperconnect')

    # 쿼리 실행: 2024/12/8일 이전 날짜가 존재하는지 확인
    flag = """
    SELECT COUNT(*) FROM table_a WHERE dt <= '2024-12-08';
    """
    result = mysql_hook.get_first(flag)
    
    # 결과가 0이면 이전 날짜가 존재하지 않음
    if result[0] == 0:
        print("이전 날짜가 존재하지 않습니다.")
        raise AirflowSkipException("이전 날짜가 존재하지 않으므로 작업을 건너뜁니다.")
    else:
        print("이전 날짜가 존재합니다.")
        query = "SELECT * FROM table_a"
        result = mysql_hook.get_pandas_df(query)
        df = pd.DataFrame(result, columns=['dt','hr','value'])
        kwargs['ti'].xcom_push(key='df', value=df.to_json(orient='split'))
        print("DataFrame has been pushed to XCom")


def insert_data_to_table_b(**kwargs):
    """
    table A에서 데이터를 가져와서 table B에 삽입하는 함수.
    """
    df_json = kwargs['ti'].xcom_pull(key='df', task_ids='check_previous_date_exists')

    # JSON을 다시 DataFrame으로 변환
    df = pd.read_json(df_json, orient='split')
    df[['id', 'user_name']] = df['value'].apply(lambda x: pd.Series(json.loads(x)))
    df.drop('value', axis=1, inplace=True)
    df['dt'] = df['dt'].apply(convert_timestamp)
    
    
    # 데이터를 table B에 삽입
    for row in df.iterrows():
        insert_query = """
        INSERT INTO table_b (dt, hr, id, user_name)
        VALUES (%s, %s, %s, %s)
        """
        MySqlHook.run(insert_query, parameters=(row['dt'], row['hr'], row['id'], row['user_name']))

    print("데이터가 MySQL에 삽입되었습니다.")

def wait_for_previous_date(**kwargs):
    """
    PythonSensor로 이전 날짜가 존재하는지 확인하는 함수.
    만약 날짜가 없다면 작업을 건너뛰게 함.
    """
    try:
        check_previous_date_exists(**kwargs)
        return True
    except AirflowSkipException:
        return False

# DAG 정의
dag = DAG(
    'check_and_insert_data',
    start_date=datetime(2024, 12, 8, 10, 0),  # 실행 날짜 설정 (한국시간 오전 10시)
    schedule_interval=None,  # 수동 실행
    catchup=False
)

# PythonSensor로 이전 날짜가 존재하는지 확인
check_date_sensor = PythonSensor(
    task_id='check_previous_date_sensor',
    python_callable=wait_for_previous_date,
    poke_interval=10,
    timeout=60,
    mode='poke',  # 계속해서 확인하는 방식
    provide_context=True,  # 이 옵션은 PythonSensor에서 기본적으로 True이므로 제거할 필요 없음
    dag=dag
)

# PythonOperator: 데이터를 삽입하는 작업
insert_data = PythonOperator(
    task_id='insert_data_to_table_b',
    python_callable=insert_data_to_table_b,
    provide_context=True,
    dag=dag
)

# 작업 순서: 날짜가 존재하면 insert 작업 실행, 존재하지 않으면 skip
check_date_sensor >> insert_data
