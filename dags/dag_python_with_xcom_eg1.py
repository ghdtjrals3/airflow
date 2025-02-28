import datetime
import pendulum
from airflow import DAG
from airflow.decorators import task
with DAG(
    dag_id="dag_python_with_xcom_eg1",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'

    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        t1 = kwargs['ti']
        # xcom_pull 파라미터로 task_ids만 넣는다면 디폴트로 해당 task의 return 값을 가져옴 'Success'
        value1 = t1.xcom_pull(task_ids='python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + value1)

    # status는 해당 DAG의 flow를 이해해야함
    # 현재 python_xcom_push_by_return = xcom_push_result()를 통해
    # Success 값이 python_xcom_push_by_return 테스크의 return xcom 값으로 들어가 있음
    # 그 후 xcom_pull_2(python_xcom_push_by_return)는 return xcom 값을 status 인자로 호출함
    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)

    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_1()