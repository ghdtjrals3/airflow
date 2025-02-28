import datetime
import pendulum
from airflow import DAG
from airflow.decorators import task
with DAG(
    dag_id="dag_python_with_xcom_eg2",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1,2,3])

    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    # ti 객체로부터 xcom_pull 메소드를 사용하여 key 값을 통해 가져올 수 있음
    # 그러나 같은 key 값의 ti 객체가 있다면 제일 마지막에 들어간 값을 기준으로 가져옴
    # 따라서 task_ids로 task_id를 명시하여 원하는 ti 객체의 xcom 값을 가져올 수 있음
    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key="result1")
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)

    xcom_push1() >> xcom_push2() >> xcom_pull()