from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import os
import pandas as pd
import psycopg2
from psycopg2 import sql

# Database configuration
db_config = {
    'host': 'host.docker.internal',
    'database': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'port': 5432
}

# Directory paths for data files
data_dir = "/opt/airflow/data/clean/"
table_mapping = {
    "fridges_clean.csv": "fridges",
    "laptops_clean.csv": "laptops"
}

# Table creation SQL commands
table_schemas = {
    "fridges": """
        CREATE TABLE IF NOT EXISTS fridges (
            id SERIAL PRIMARY KEY,
            name TEXT,
            brand TEXT,
            capacity_litres FLOAT,
            doors INTEGER,
            color TEXT,
            warranty_years INTEGER,
            price FLOAT,
            reviews INTEGER,
            ratings FLOAT,
            links TEXT,
            source TEXT
        );
    """,
    "laptops": """
        CREATE TABLE IF NOT EXISTS laptops (
            id SERIAL PRIMARY KEY,
            name TEXT,
            brand TEXT,
            capacity_litres INTEGER,
            doors INTEGER,
            color TEXT,
            warranty_years INTEGER,
            price FLOAT,
            reviews INTEGER,
            ratings FLOAT,
            links TEXT,
            source TEXT
        );
    """
}

# Function to load data into tables
def load_data_to_table(table_name, file_path):
    conn = psycopg2.connect(**db_config)
    df = pd.read_csv(file_path)
    
    with conn.cursor() as cursor:
        for _, row in df.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO {table} (name, brand, capacity_litres, doors, color, warranty_years, price, reviews, ratings, links, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """).format(table=sql.Identifier(table_name))
            cursor.execute(insert_query, tuple(row))
    
    conn.commit()
    conn.close()
    print(f"Data loaded into table: {table_name}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'load_data_to_postgres',
    default_args=default_args,
    description='Create tables and load data to Postgres DB',
    schedule_interval='@once',
    catchup=False
) as dag:

    # Task to create tables in Postgres
    create_tables_tasks = []
    for table_name, schema_sql in table_schemas.items():
        create_table_task = PostgresOperator(
            task_id=f'create_{table_name}_table',
            postgres_conn_id='postgres_default',
            sql=schema_sql
        )
        create_tables_tasks.append(create_table_task)

    # Task to load data for each file
    def load_file_task(table_name, file_name):
        file_path = os.path.join(data_dir, file_name)
        return PythonOperator(
            task_id=f'load_{table_name}_data',
            python_callable=load_data_to_table,
            op_kwargs={'table_name': table_name, 'file_path': file_path}
        )

    load_data_tasks = []
    for file_name, table_name in table_mapping.items():
        load_data_tasks.append(load_file_task(table_name, file_name))

    # Task dependencies
    for create_table_task in create_tables_tasks:
        create_table_task >> load_data_tasks
