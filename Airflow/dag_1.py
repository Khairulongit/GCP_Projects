"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import datetime

from airflow import DAG

from airflow.providers.standard.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy import DummyOperator

# pylint: enable=g-import-not-at-top

default_args = {
    'start_date': datetime.datetime(2000, 1, 1),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'dag_dummy',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule='*/10 * * * *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id='echo',
    bash_command='echo test',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False)

t2 = DummyOperator(
    task_id='dummy',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1)

t1 >> t2