# import할 패키지들 기존에 제공되는 패키지는 1번이고 2번은 새로 설치해야하는 패키지들
# 1 
from datetime import datetime, timedelta
from email.policy import default
from textwrap import dedent

# 2(MysqlOperator설치)
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator

# default_args은 dag를 실행할때 횟수나 빈도를 설정하는 변수
default_args = {
    'depends_on_past': False,
    'retires': 1,
    'retry_delay': timedelta(minutes=5)
}

# employees 테이블 생성구문 변수
sql_create_table = """
    CREATE TABLE `employees` (
        `employeeNumber` int(11) NOT NULL,
        `lastName` varchar(50) NOT NULL,
        `firstName` varchar(50) NOT NULL,
        `extension` varchar(10) NOT NULL,
        `email` varchar(100) NOT NULL,
        `officeCode` varchar(10) NOT NULL,
        `reportsTo` int(11) DEFAULT NULL,
        `jobTitle` varchar(50) NOT NULL,
    PRIMARY KEY (`employeeNumber`)
    );
"""

# employees 테이블 데이터 추가구문 변수
sql_insert_data = """
    insert into `employees`(`employeeNumber`,`lastName`,`firstName`,`extension`,`email`,`officeCode`,`reportsTo`,`jobTitle`) values
     (1002,'Murphy','Diane','x5800','dmurphy@classicmodelcars.com','1',NULL,'President'),
        (1056,'Patterson','Mary','x4611','mpatterso@classicmodelcars.com','1',1002,'VP Sales'),
        (1076,'Firrelli','Jeff','x9273','jfirrelli@classicmodelcars.com','1',1002,'VP Marketing'),
        (1088,'Patterson','William','x4871','wpatterson@classicmodelcars.com','6',1056,'Sales Manager (APAC)'),
        (1102,'Bondur','Gerard','x5408','gbondur@classicmodelcars.com','4',1056,'Sale Manager (EMEA)'),
        (1143,'Bow','Anthony','x5428','abow@classicmodelcars.com','1',1056,'Sales Manager (NA)'),
        (1165,'Jennings','Leslie','x3291','ljennings@classicmodelcars.com','1',1143,'Sales Rep'),
        (1166,'Thompson','Leslie','x4065','lthompson@classicmodelcars.com','1',1143,'Sales Rep'),
        (1188,'Firrelli','Julie','x2173','jfirrelli@classicmodelcars.com','2',1143,'Sales Rep'),
        (1216,'Patterson','Steve','x4334','spatterson@classicmodelcars.com','2',1143,'Sales Rep'),
        (1286,'Tseng','Foon Yue','x2248','ftseng@classicmodelcars.com','3',1143,'Sales Rep'),
        (1323,'Vanauf','George','x4102','gvanauf@classicmodelcars.com','3',1143,'Sales Rep'),
        (1337,'Bondur','Loui','x6493','lbondur@classicmodelcars.com','4',1102,'Sales Rep'),
        (1370,'Hernandez','Gerard','x2028','ghernande@classicmodelcars.com','4',1102,'Sales Rep'),
        (1401,'Castillo','Pamela','x2759','pcastillo@classicmodelcars.com','4',1102,'Sales Rep'),
        (1501,'Bott','Larry','x2311','lbott@classicmodelcars.com','7',1102,'Sales Rep'),
        (1504,'Jones','Barry','x102','bjones@classicmodelcars.com','7',1102,'Sales Rep'),
        (1611,'Fixter','Andy','x101','afixter@classicmodelcars.com','6',1088,'Sales Rep'),
        (1612,'Marsh','Peter','x102','pmarsh@classicmodelcars.com','6',1088,'Sales Rep'),
        (1619,'King','Tom','x103','tking@classicmodelcars.com','6',1088,'Sales Rep'),
        (1621,'Nishi','Mami','x101','mnishi@classicmodelcars.com','5',1056,'Sales Rep'),
        (1625,'Kato','Yoshimi','x102','ykato@classicmodelcars.com','5',1621,'Sales Rep'),
        (1702,'Gerard','Martin','x2312','mgerard@classicmodelcars.com','4',1102,'Sales Rep');
"""

# DAG 설정
with DAG(
    'connect_to_local_mysql',
    default_args = default_args,
    description = """
        1) create 'employees' table in local mysqld
        2) insert data to 'employees' table
    """,
    schedule_interval = '@daily',
    start_date = datetime(2022, 1, 1),
    catchup = False,
    tags = ['mysql', 'local', 'test', 'employees']
) as dag:
    t1 = MySqlOperator(
        task_id="create_employees_table",
        mysql_conn_id="mysql_local_test",
        sql=sql_create_table,
    )

    t2 = MySqlOperator(
        task_id="insert_employees_data",
        mysql_conn_id="mysql_local_test",
        sql=sql_insert_data
    )


# 순서
    t1 >> t2