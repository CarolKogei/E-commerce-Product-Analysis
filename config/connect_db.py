# import necessary modules
import csv
import pandas as pd
from sqlalchemy import create_engine
#from urllib.parse import quote_plus
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Database connection parameters
DB_HOST = os.getenv('POSTGRES_HOST')
DB_NAME = os.getenv('POSTGRES_DB')
DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_PORT = os.getenv('POSTGRES_PORT')


# print(f"DB_HOST: {DB_HOST}")
# print(f"DB_PORT: {DB_PORT}")
# print(f"DB_NAME: {DB_NAME}")
# print(f"DB_USER: {DB_USER}")
# print(f"DB_PASSWORD: {DB_PASSWORD}")

# Encode the password to handle special characters
# DB_PASSWORD = quote_plus(DB_PASSWORD)

# create postgres connection string
db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

# load data from csv file
tv_df = pd.read_csv(r'data\clean\kilimall_tvs.csv')

# table name in postgres
table_name = 'tvs'

# upload data to postgres

try:
    tv_df.to_sql(table_name, engine, if_exists='append', index=False)
    print('Data uploaded to db successfully')
except Exception as e:
    print(f'Error uploading data to db: {e}')



# Function to connect to PostgreSQL
# def connect_to_db():
#     return psycopg2.connect(
#         host=DB_HOST,
#         database=DB_NAME,
#         user=DB_USER,
#         password=DB_PASSWORD,
#         port=DB_PORT
#     )

# def ingest_data():
#     # Connect to PostgreSQL
#     conn = connect_to_db()
#     cur = conn.cursor()


        