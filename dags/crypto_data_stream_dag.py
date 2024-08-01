from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scripts.crypto_data_stream import data_stream
from scripts.read_kafka_write_postgres import create_new_tables_in_postgres, insert_data_into_postgres

start_date = datetime(2018, 12, 21, 12, 12)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('crypto_data_stream', default_args=default_args, schedule_interval='*/10 * * * *', catchup=False) as dag:

    data_stream_task = PythonOperator(
        task_id='data_stream',
        python_callable=data_stream,
        dag=dag,
    )

    create_new_tables_in_postgresql_task = PythonOperator(
        task_id='create_new_tables_in_postgresql',
        python_callable=create_new_tables_in_postgres,
        dag=dag,
    )

    insert_data_into_postgresql_task = PythonOperator(
        task_id='insert_data_into_postgresql',
        python_callable=insert_data_into_postgres,
        dag=dag,
    )

    data_stream_task

    create_new_tables_in_postgresql_task >> insert_data_into_postgresql_task
