import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs_operator import HttpToGcsOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG(
            dag_id="exercise4",
            default_args={
                "owner":"jonathan",
                "start_date":datetime(2019,9,20),
            },
            schedule_interval="0 0 * * *"
        )

postgres_to_google_cloud = PostgresToGoogleCloudStorageOperator(
        task_id="postgres_to_google_cloud",
        sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
        bucket="airflow-training-data-jalves",
        filename="{{ ds }}/properties_{}.json",
        postgres_conn_id="airflow-training",
        dag=dag
        )

http_to_google_cloud = HttpToGcsOperator(
        task_id="http_to_google_cloud",
        method="GET",
        endpoint="https://api.exchangeratesapi.io/history?start_at=2019-09-20&end_at=2019-09-27&symbols=EUR&base=GBP",
        gcs_bucket="airflow-training-data-jalves",
        gcs_path="exchange.json",
        dag=dag
        )

dummy = DummyOperator (
        task_id="dummy",
        dag=dag
        )

postgres_to_google_cloud >> dummy
http_to_google_cloud >> dummy

