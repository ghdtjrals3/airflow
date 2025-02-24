
import datetime
# pendulum은 파이썬의 datetime 포맷을 더욱 쉽게 쓸 수 있게 도와줌
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    # 웹 UI에서 표시되는 dag 이름 / 직관성을 위해 파일명과 dag_id를 일치시키는 것이 유지보수가 좋음
    dag_id="dags_bash_operator",

    # schedule은 언제 시작할지 설정함 / 분, 시, 일, 월, 요일
    schedule="0 0 * * *",

    # start_date는 dag이 언제부터 돌 것인지 설정
    # start_date와 catchup은 연관이 있음. 현재 2월 1일이라 가정할 시 start_date는 현재 1월 1일로 설정되어 있음
    # 그럼 한달의 공백기가 존재하고 한달의 누락된 작업을 돌릴려면 True, 아니면 False로 설정해야함.
    # 다만 catchup은 누락된 작업에 대해 한번에 실행하기 때문에 생성한 Dag에 따라 문제가 생길수도 있으니 유념해야함
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # [START howto_operator_bash]
    # 객체명과 task id도 역시 직관성을 위해 통일하는 것이 좋음
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )
    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )
    # [END howto_operator_bash]

    # 객체의 순서를 지정
    bash_t1 >> bash_t2
