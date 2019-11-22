import airflow
from airflow import DAG
from airflow.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from datetime import datetime

dag = DAG(
            dag_id="exercise4",
            default_args={
                "owner":"jonathan",
                "start_date":datetime(2019,9,20),
            },
            schedule_interval="0 0 * * *"
        )

postgres_to_google_cloud = Postgresoperator(
        task_id="postgres_to_google_cloud",
        sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
        bucket="airflow-training-data-jalves",
        filename="{{ ds }}/properties_{}.json",
        postgres_conn_id="airflow-training",
        dag=dag
        )

postgres_to_google_cloud

