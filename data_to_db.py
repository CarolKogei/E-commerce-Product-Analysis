import psycopg2
import csv

#Database Connection Parameters
DB_HOST = 'localhost'
DB_NAME = 'airflow'
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_PORT = '5432'

#function to connect to postgresql
def connect_to_db():
    return psycopg2.connect(
        host = DB_HOST
        database = DB_NAME
        user = DB_USER
        password = DB_PASSWORD
        port = DB_PORT
    )

#Main function to ingest data
def ingest_data():
    #connect to postgresql
    conn = connect_to_db()
    cur = conn.cursor

    #Open the CVS file
    with open('data.csv', 'r') as file:
        data_reader = csv.reader(file)
        next(data_reader) #skip the header row

        #insert each row into the table
        for row in data_reader:
            cur.execute("INSERT INTO jumiadb (id, productname, price, old_price, ratings) VALUES (%s, %s, %s, %s, %s)", row)

    #Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully")

if __name__ == __main__:
    ingest_data()   