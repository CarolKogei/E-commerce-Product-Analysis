import psycopg2
import csv

# Database Connection Parameters
DB_HOST = 'localhost'
DB_NAME = 'airflow'
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_PORT = '5432'

# Function to connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

# Main function to ingest data
def ingest_data():
    # Connect to PostgreSQL
    conn = connect_to_db()
    cur = conn.cursor()

    # Open the CSV file
    with open(r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\clean\kilimall_clean_microwaves.csv", 'r', encoding='utf-8') as file:
        data_reader = csv.reader(file)
        next(data_reader)  # Skip the header row

        # Insert each row into the table
        for row in data_reader:
            # Clean data before inserting into the database
            brand = row[0]
            reviews = row[1]
            price = row[2]
            capacity = row[3]

            # Convert empty strings in price and capacity to None (NULL)
            if price == '':
                price = None
            else:
                # Ensure price is numeric (you can apply further cleaning here if needed)
                try:
                    price = float(price)
                except ValueError:
                    price = None  # If it can't be converted, set as NULL

            if capacity == '':
                capacity = None
            else:
                # Ensure capacity is numeric (you can apply further cleaning here if needed)
                try:
                    capacity = float(capacity)
                except ValueError:
                    capacity = None  # If it can't be converted, set as NULL

            # Insert data into the table
            cur.execute("INSERT INTO kilimall_microwave (brand, reviews, price, capacity) VALUES (%s, %s, %s, %s)", (brand, reviews, price, capacity))

    # Commit and close the connection
    conn.commit()
    cur.close()
    conn.close()
    print("Data ingested successfully")

if __name__ == "__main__":
    ingest_data()
