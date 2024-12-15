from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.hooks.base import BaseHook 
from sqlalchemy import create_engine
import pandas as pd
import json
from datetime import datetime, timedelta

# 한국 시간으로 오전 10시에 실행되도록 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 15, 10, 0, 0),
    'retries': 1,
}

# PythonSensor로 이전 날짜의 데이터가 있는지 체크하는 함수
def check_previous_date(**kwargs):
    connection = BaseHook.get_connection('127.0.0.1')
    engine = create_engine(f'mysql+pymysql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}')
    
    # 이전 날짜의 데이터 존재 여부를 체크하는 쿼리
    query = """
    SELECT COUNT(*) 
    FROM table_a ;
    """
    result = pd.read_sql(query, engine)
    return result # 이전 날짜의 데이터가 있으면 True 반환

# # JSON 데이터를 파싱하고 테이블 B에 삽입하는 함수
# def parse_json_and_create_table(**kwargs):
#     connection = BaseHook.get_connection('your_connection_id')
#     engine = create_engine(f'mysql+pymysql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}')
    
#     # 테이블 A에서 이전 날짜의 데이터를 가져오는 쿼리
#     query = """
#     SELECT dt, hr, value
#     FROM table_a
#     WHERE dt = CURDATE() - INTERVAL 1 DAY;
#     """
#     df = pd.read_sql(query, engine)
    
#     # JSON 데이터 파싱 및 데이터프레임 변환
#     parsed_data = []
#     for index, row in df.iterrows():
#         value_json = json.loads(row['value'])  # JSON 파싱
#         parsed_data.append({
#             'df': row['dt'], 
#             'hr': row['hr'], 
#             'id': value_json['id'], 
#             'user_name': value_json['user_name']
#         })
    
#     # 파싱된 데이터를 DataFrame으로 변환
#     df_parsed = pd.DataFrame(parsed_data)
    
#     # 테이블 B에 삽입
#     df_parsed.to_sql('table_b', engine, if_exists='replace', index=False)
#     print("Table B created with parsed data.")

# # DAG 정의
# with DAG('table_a_to_b_dag',
#          default_args=default_args,
#          schedule_interval='0 10 * * *',  # 매일 오전 10시에 실행
#          catchup=False) as dag:

#     # PythonSensor로 이전 날짜 데이터가 있는지 확인
#     check_previous_date_sensor = PythonSensor(
#         task_id='check_previous_date_sensor',
#         python_callable=check_previous_date,
#         poke_interval=60,  # 1분마다 확인
#         timeout=600,  # 최대 10분 대기
#         mode='poke'  # 'poke' 모드로 계속 체크
#     )

#     # JSON을 파싱하여 테이블 B에 저장하는 작업
#     parse_and_create_table = PythonOperator(
#         task_id='parse_json_and_create_table',
#         python_callable=parse_json_and_create_table,
#         provide_context=True
#     )

#     # Task 의존 관계 설정
#     check_previous_date_sensor >> parse_and_create_table
