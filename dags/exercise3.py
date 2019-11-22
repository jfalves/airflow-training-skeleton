import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG(
            dag_id="exercise3",
            default_args={
                "owner":"jonathan",
                "start_date":airflow.utils.dates.days_ago(5),
                "schedule_interval":"@daily",
            },
        )

def print_weekday(**context):
    print("{{ execution_date }}")

print_weekday = PythonOperator(
        task_id="print_weekday",
        python_callable=print_weekday,
        provide_context=True,
        dag=dag)

print_weekday
