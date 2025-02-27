import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import get_stfp

with DAG(
    dag_id="dags_python_import_func",
    schedule="0 0 0 * *",
    start_date=pendulum.datetime(2025,3,1,tz="Asia/Seoul"),
    catchup=False
) as dag:
    task_get_stfp = PythonOperator(
        task_id="task_get_stfp",
        python_callable=get_stfp
    )