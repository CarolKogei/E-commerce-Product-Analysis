import requests
from bs4 import BeautifulSoup
import pandas as pd

def web_scraping_jumia_microwaves(url, baseurl):

    descriptions = []
    prices = []
    older_prices = []
    reviews_list = []
    urls = []

    r = requests.get(url)
    print(f"Fetching data from {url} Status {r}")

    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")

        # Extract product prices
        price_elements = soup.find_all("div", {"class": "prc"})
        for price_element in price_elements:
            prices.append(price_element.text.strip())

        # Extract old prices
        old_price_elements = soup.find_all("div", class_="old")
        for old_price_element in old_price_elements:
            older_prices.append(old_price_element.text.strip())

        # Extract product descriptions
        desc_elements = soup.find_all("h3", class_="name")
        for desc_element in desc_elements:
            descriptions.append(desc_element.text.strip())

        # Extract reviews
        reviews = soup.find_all("div", class_="stars _s")
        for rev in reviews:
            reviews_list.append(rev.text.strip())

        products = soup.find_all('a', class_='core')

        for product in products:
                # Extract product link
                link = product['href'] if 'href' in product.attrs else None
                # Complete the URL if the link is relative
                link = f"https://www.jumia.co.ke{link}" if link and link.startswith('/') else link
                urls.append(link)
    else:
        print(f"Failed to fetch data from {url}")
    
    for i in range(2, 51):
        page_url = f"{baseurl}{i}#catalog-listing"
        r = requests.get(page_url)
        print(f"Fetching data from: {page_url} - Status: {r.status_code}")
    
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")

        # Extract product prices
        price_elements = soup.find_all("div", {"class": "prc"})
        for price_element in price_elements:
            prices.append(price_element.text.strip())

        # Extract old prices
        old_price_elements = soup.find_all("div", class_="old")
        for old_price_element in old_price_elements:
            older_prices.append(old_price_element.text.strip())

        # Extract product descriptions
        desc_elements = soup.find_all("h3", class_="name")
        for desc_element in desc_elements:
            descriptions.append(desc_element.text.strip())

        # Extract reviews
        reviews = soup.find_all("div", class_="stars _s")
        for rev in reviews:
            reviews_list.append(rev.text.strip())
        

        
        products = soup.find_all('a', class_='core')

        for product in products:
                # Extract product link
                link = product['href'] if 'href' in product.attrs else None
                # Complete the URL if the link is relative
                link = f"https://www.jumia.co.ke{link}" if link and link.startswith('/') else link
                urls.append(link)
                
    else:
        print(f"Failed to fetch data from {page_url}")

    rows = list(zip(descriptions, prices, older_prices, reviews_list, urls))
    return rows



def collect_data_microwaves():
    data = web_scraping_jumia_microwaves(url, baseurl)

    df = pd.DataFrame(data, columns = ['descriptions', 'price', 'old_price', 'ratings', 'urls'])

    df.to_csv(r"..\data\scraped_data\jumia_scraped_microwaves.csv", index = False)


url = "https://www.jumia.co.ke/small-appliances-microwave/"
# baseurl = "https://www.jumia.co.ke/catalog/?q=microwaves&amp;page="
baseurl = "https://www.jumia.co.ke/catalog/?q=microwaves&page="


collect_data_microwaves()



def jumia_microwaves_cleaning(csv_path):
    data = pd.read_csv(csv_path)
    data.head()
    data["Price"] = data["price"].str.replace("KSh", "").str.replace(",","")
    data["Reviews"] = data["ratings"].str.replace(" out of 5", "")
    data["Old_price"] = data["old_price"].str.replace("KSh ", "").str.replace(",", "")
    data["Price"] = data["Price"].astype(int)
    data["Reviews"] = data["Reviews"].astype(float)
    data["Old_price"] = data["Old_price"].astype(int)
    pattern = r"^[a-zA-Z]+"
    data["brand"] = data["descriptions"].str.extract(f"({pattern})")
    pattern_cap = r'(\d+)\s*(?=litres|l|L)'
    result = data["descriptions"].str.extract(f"({pattern_cap})")
    data["capacity"] = result[0]
    data["capacity"] = data["capacity"].str.strip().astype(float)
    id = [i for i in range(1, len(data) + 1)]
    data["id"] = id
    data["id"] - data["id"].astype(int)
    data["source"] = ["Jumia"] * len(data)
    data = data.drop(columns = ["price", "ratings", "old_price"])
    columns = ["id", "descriptions", "brand", "Price", "Old_price", "capacity", "Reviews", "source", "urls"]
    data = data[columns]
    data.rename(columns = {"descriptions" : "description", "Price" : "price", "Old_price" : "old_price", "Reviews" :  "reviews"})
    data = data[columns]
    data.to_csv(r"..data\clean_data\jumia_clean_microwaves.csv", index=False)

    return data


csv_path = r"..data\scraped_data\jumia_scraped_microwaves.csv"


jumia_microwaves_cleaning(csv_path)


import psycopg2
import csv
import os

# Database Connection Parameters
DB_HOST = 'localhost'
DB_NAME = 'airflow'
DB_USER = 'airflow'
DB_PASSWORD = 'airflow'
DB_PORT = '5432'

# Function to connect to PostgreSQL
def connect_to_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        exit(1)

# Function to create the `jumia_microwaves` table if it does not exist
def create_table(cur):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS jumia_microwaves (
        id SERIAL PRIMARY KEY,
        descriptions TEXT,
        brand TEXT,
        price INTEGER,
        old_price INTEGER,
        capacity TEXT,
        reviews TEXT,
        source TEXT,
        urls TEXT
    );
    """
    try:
        cur.execute(create_table_query)
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        exit(1)

# Main function to ingest data
def ingest_data():
    # Connect to PostgreSQL
    conn = connect_to_db()
    cur = conn.cursor()

    # Create the table if it does not exist
    create_table(cur)

    # Define the CSV file path
    csv_file_path = r'C:\Users\charity.ngari\Desktop\e-commerce-product-analysis\data\clean_data\jumia_clean_microwaves.csv'

    # Check if the CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: File not found at {csv_file_path}")
        conn.close()
        exit(1)

    # Open the CSV file and ingest data
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            data_reader = csv.reader(file)
            next(data_reader)  # Skip the header row

            # Insert each row into the table
            for row in data_reader:
                if len(row) != 9:  # Ensure row has exactly 8 values
                    print(f"Skipping row with incorrect number of values: {row}")
                    continue
                cur.execute("""
                    INSERT INTO jumia_microwaves (id,descriptions,brand,Price,Old_price,capacity,Reviews,source,urls)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, row)
                
        # Commit the transaction
        conn.commit()
        print("Data ingested successfully")
    except Exception as e:
        print(f"Error while ingesting data: {e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cur.close()
        conn.close()

if __name__ == "__main__":
    ingest_data()
