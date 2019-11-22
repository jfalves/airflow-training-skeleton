import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG(
        dag_id="bonus",
        default_args={
            "owner":"jonathan",
            "start_date":airflow.utils.dates.days_ago(3),
            },
        )

def print_something(**context):
    print('hello!')

print_something = PythonOperator(
        task_id="print_something",
        python_callable=print_something,
        provide_context=True,
        dag=dag
        )

