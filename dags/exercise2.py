import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG(
        dag_id="exercise2",
        default_args={
            "owner":"jonathan",
            "start_date":airflow.utils.dates.days_ago(3),
            },
        )

def print_timestamp():
    print(datetime.now())

print_timestamp = PythonOperator(
        task_id="print_timestamp",
        python_callable=print_timestamp,
        provide_context=False,
        dag=dag)

wait_1 = BashOperator(
        task_id="wait_1",
        bash_command="sleep 1",
        dag=dag
        )

wait_5 = BashOperator(
        task_id="wait_5",
        bash_command="sleep 5",
        dag=dag
        )

wait_10 = BashOperator(
        task_id="wait_10",
        bash_command="sleep 10",
        dag=dag
        )

the_end = DummyOperator(
        task_id="the_end",
        dag=dag
        )

print_timestamp >> [wait_1,wait_5,wait_10] >> the_end
