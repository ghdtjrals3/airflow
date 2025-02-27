import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
with DAG(
    dag_id="dags_python_template.py",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id='python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date' : '{{date_interval_start | ds}}', 'end_date' : '{{date_interval_end | ds}}'}
    )

    @task(task_id='python_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts' + kwargs['ts'])

    python_t1 >> python_function2()