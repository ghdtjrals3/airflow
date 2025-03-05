from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2025,2,1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:
    '''감염병(코로나) 확진자 발생동향'''
    covid19_info = HttpOperator(
        task_id='covid19_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/xml/TbCorona19CountStatus/1/5/',
        method='GET',
        headers={'Content-Type': 'application/json',
                'charset': 'utf-8',
                'Accept': '*/*'
                },
        do_xcom_push=True,
        log_response=True
    )

    @task(task_id='pyhton_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='covid19_info')
        import json
        from pprint import pprint

        if not rslt:
            print("HTTP 응답이 비어 있습니다.")
            return

        try:
            pprint(json.loads(rslt))
        except json.JSONDecodeError as e:
            print(f"JSON 파싱 오류: {e}")
            print(f"응답 내용: {rslt}")
        
    covid19_info >> python_2()