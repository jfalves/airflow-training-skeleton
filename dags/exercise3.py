import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import date

dag = DAG(
            dag_id="exercise3",
            default_args={
                "owner":"jonathan",
                "start_date":airflow.utils.dates.days_ago(5),
                "schedule_interval":"@daily",
            },
        )

def print_weekday(**context):
    print(context["execution_date"])

def _branching(**context):
    weekday_person_to_email = {
            0: "Bob", # Monday
            1: "Joe", # Tuesday
            2: "Alice", # Wednesday
            3: "Joe", # Thursday
            4: "Alice", # Friday
            5: "Alice", # Saturday
            6: "Alice", # Sunday
        }

    return "email_"+weekday_person_to_email[date.weekday()].lower()



print_weekday = PythonOperator(
        task_id="print_weekday",
        python_callable=print_weekday,
        provide_context=True,
        dag=dag)

branching = PythonOperator(
        task_id="branching",
        python_callable=_branching,
        provide_context=True,
        dag=dag)

print_weekday >> branching
