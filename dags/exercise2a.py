import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG(
            dag_id="exercise2a",
        default_args={
            "owner":"jonathan",
            "start_date":airflow.utils.dates.days_ago(5),
            "schedule_interval":"@daily",
            },
        )

def print_timestamp():
    print(str(datetime.now()))

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

print_timestamp >> wait_1
print_timestamp >> wait_5
print_timestamp >> wait_10
wait_1 >> the_end
wait_5 >> the_end
wait_10 >> the_end
