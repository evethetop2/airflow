from airflow.models.dag import DAG
import pendulum
from airflow.operators.python import PythonOperator
from common.common_regist import regist2

with DAG(
    dag_id="dags_python_with_op_args",
    #시간임
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    #누락된거 돌릴거냐 안돌릴거냐, 차례차례 안돌고 한번에 돌게됨
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    regist2_t1 = PythonOperator(
        task_id = 'regist2_t1',
        python_callable=regist2,
        ##여기가 중요
        op_args=['hkpark','man','kr','seoul'],
        op_kwargs={'email':'dsad@naver.com', 'phone':'119'}
    )

    regist2_t1