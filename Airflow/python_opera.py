from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
# from airflow.operators.py import python_operator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def my_function(name,age):
    print(f"Hello from my_function! {name}, {age} years old")
    
with DAG(
    dag_id="simple_echo_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger
    schedule=None,   # manual trigger
    catchup=False,
) as dag:

    start_task = EmptyOperator(
        task_id="start_task",
        # bash_command="echo 'Hello from Airflow!'",
    )

    middle_task = PythonOperator(
        task_id="middle_task",
        python_callable=my_function ,# Replace with your actual Python function
        op_kwargs={"name": "Khairul", "age": 36},
    )
    end_task= DummyOperator(
        task_id="end_task",
        # ash_command="echo 'Hello from Airflow!'",
    )

    start_task >> middle_task >> end_task
