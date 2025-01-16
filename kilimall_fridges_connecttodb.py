import psycopg2
import csv
from dotenv import load_dotenv
import os


#Database Connection Parameters
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')

# Function to connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT)



#Main function to ingest data
def ingest_data():
    #connect to postgresql
    conn = connect_to_db()
    cur = conn.cursor()

    #Open the CVS file
    with open(r"data\clean\kilimall_fridges.csv", 'r', encoding='utf-8') as file:
        data_reader = csv.reader(file)
        next(data_reader) #skip the header row

        #insert each row into the table
        for row in data_reader:
            cur.execute("INSERT INTO fridges (id,name,brand,capacity_litres,doors,price,reviews,links,source) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

    #Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully")

if __name__ == "__main__":
    ingest_data()