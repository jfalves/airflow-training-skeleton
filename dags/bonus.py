from airflow.operators.python_operator import PythonOperator

def print_something(**context):
    print('hello!')

print_something = PythonOperator(
        task_id="print_something",
        python_callable=print_something,
        provide_context=True,
        dag=dag
        )

