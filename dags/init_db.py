from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='initialize_postgres_db',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Run manually or only once
    catchup=False,
) as dag:

    init_db = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql='/opt/airflow/sql/init.sql',  # Corrected path
        dag=dag,
    )