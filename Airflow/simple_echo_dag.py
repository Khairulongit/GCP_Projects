from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="simple_echo_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual trigger
    catchup=False,
) as dag:

    echo_task1 = BashOperator(
        task_id="echo_hello1",
        bash_command="echo 'Hello from Airflow!'",
    )

    echo_task2= BashOperator(
        task_id="echo_hello2",
        bash_command="echo 'Hello from Airflow!'",
    )
    echo_task1 >> echo_task2
