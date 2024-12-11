from airflow.models.dag import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dag_python_task_decorator",
    #시간임
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    #누락된거 돌릴거냐 안돌릴거냐, 차례차례 안돌고 한번에 돌게됨
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:

   @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)


    python_task_1 = print_context("task_decorator 실행")