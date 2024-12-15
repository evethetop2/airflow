from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
import pymysql

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 15),
    'retries': 1,
}

def read_mysql_table_with_sqlalchemy(**kwargs):
    # SQLAlchemy 연결 URL 형식
    connection_string = "mysql+pymysql://hyperconnect_1:gksdud12@183.96.150.41:3306/hyperconnect"
    
    # SQLAlchemy 엔진 생성
    engine = create_engine(connection_string)
    
    # 쿼리 작성
    query = "SELECT * FROM table_a"
    
    # 쿼리 실행 및 결과를 Pandas DataFrame으로 가져오기
    df = pd.read_sql(query, engine)
    
    # 결과 출력
    print(df.head())

# DAG 정의
with DAG('mysql_read_sqlalchemy_dag',
         default_args=default_args,
         schedule_interval='@daily',  # 매일 실행
         catchup=False) as dag:

    read_task = PythonOperator(
        task_id='read_mysql_table_with_sqlalchemy',
        python_callable=read_mysql_table_with_sqlalchemy,
        provide_context=True
    )
