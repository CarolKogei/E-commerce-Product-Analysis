from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from datetime import datetime
import os

# Directory paths for saving scraped and cleaned data
RAW_DATA_PATH = "/opt/airflow/data/scraped/fridges.csv"
CLEAN_DATA_PATH = "/opt/airflow/data/clean/fridges_clean.csv"

# Scraping function
def scrape_fridge_data():
    fridges = []

    # Loop through pages 1 to 30
    for page in range(1, 30):
        print(f'Scraping page {page}...')
        result = requests.get(f'https://www.jumia.co.ke/appliances-fridges-freezers/')
        content = result.text
        soup = BeautifulSoup(content, features="html.parser")

        fridges_info = soup.find_all('article', class_="prd _fb col c-prd")
        for fridge_info in fridges_info:
            try:
                fridge_name = fridge_info.find('h3', class_='name').text.strip()
                fridge_price = fridge_info.find('div', class_='prc').text.strip()
                fridge_reviews = fridge_info.find('div', class_='rev').text.strip()
                fridge_ratings = fridge_info.find('div', class_='stars _s').text.strip()
                fridge_links = fridge_info.find('a', class_='core')['href'].strip()
                fridges.append({
                    "Name": fridge_name,
                    "Price": fridge_price,
                    "Reviews": fridge_reviews,
                    "Ratings": fridge_ratings,
                    "Links": f"https://www.jumia.co.ke{fridge_links}"
                })
            except AttributeError:
                continue

    # Save scraped data
    df = pd.DataFrame(fridges)
    os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
    df.to_csv(RAW_DATA_PATH, index=False, encoding='utf-8')
    print("Scraping completed and saved.")

# Data cleaning function
def clean_fridge_data():
    def extract_brand_and_model(name):
        match = re.match(r"([A-Za-z]+(?: [A-Za-z]+)*)(?:\s[RF|REF|FM|DF|D|]{2,4}[\d]+)?", name)
        return match.group(1).strip() if match else ''

    def extract_size(name):
        match = re.search(r'(\d+)\s*Litres?', name)
        return int(match.group(1)) if match else None

    def extract_doors(name):
        match = re.search(r'(\d+)\s*Door', name)
        return int(match.group(1)) if match else None

    def extract_color(name):
        color_keywords = ['Silver', 'White', 'Black', 'Grey', 'Red', 'Blue', 'Green', 'Beige', 'Stainless', 'Chrome']
        for color in color_keywords:
            if color.lower() in name.lower():
                return color
        return None

    def extract_warranty(name):
        match = re.search(r'(\d+)\s*YRs?\s*WRTY', name)
        return int(match.group(1)) if match else None

    def extract_price(price):
        match = re.search(r'KSh\s*(\d+([,]\d{3})*)', price)
        return float(match.group(1).replace(',', '')) if match else None

    def extract_reviews(reviews):
        match = re.search(r'\((\d+)\)', reviews)
        return int(match.group(1)) if match else None

    def extract_ratings(ratings):
        match = re.search(r'(\d+\.\d+)', ratings)
        return float(match.group(1)) if match else None

    fridges_df = pd.read_csv(RAW_DATA_PATH)
    fridges_df['name'] = fridges_df['Name']
    fridges_df['brand'] = fridges_df['Name'].apply(extract_brand_and_model)
    fridges_df['capacity_litres'] = fridges_df['Name'].apply(extract_size)
    fridges_df['doors'] = fridges_df['Name'].apply(extract_doors)
    fridges_df['color'] = fridges_df['Name'].apply(extract_color)
    fridges_df['warranty_years'] = fridges_df['Name'].apply(extract_warranty)
    fridges_df['price'] = fridges_df['Price'].apply(extract_price)
    fridges_df['reviews'] = fridges_df['Reviews'].apply(extract_reviews)
    fridges_df['ratings'] = fridges_df['Ratings'].apply(extract_ratings)
    fridges_df['links'] = fridges_df['Links']
    fridges_df['source'] = 'Jumia'

    data = fridges_df[['name', 'brand', 'capacity_litres', 'doors', 'color',
                       'warranty_years', 'price', 'reviews', 'ratings', 'links', 'source']]
    os.makedirs(os.path.dirname(CLEAN_DATA_PATH), exist_ok=True)
    data.to_csv(CLEAN_DATA_PATH, index=False)
    print("Data cleaning completed and saved.")

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'scrape_and_clean_fridge_data',
    default_args=default_args,
    description='Web scrape fridge data and clean it for further processing',
    schedule_interval='@once',
    catchup=False
) as dag:

    # Task to scrape data
    scrape_data_task = PythonOperator(
        task_id='scrape_fridge_data',
        python_callable=scrape_fridge_data
    )

    # Task to clean data
    clean_data_task = PythonOperator(
        task_id='clean_fridge_data',
        python_callable=clean_fridge_data
    )

    # Task dependencies
    scrape_data_task >> clean_data_task







# # Load cleaned data to PostgreSQL
# try:
#     # Establish database connection
#     connection = psycopg2.connect(
#         host='localhost',
#         database='airflow',
#         user='airflow',
#         password='airflow',
#         port=5432

#         #host=db_params['host'],
#         #database=db_params['database'],
#         #user=db_params['user'],
#         #password=db_params['password'],
#         #port=db_params['port']
#     )
#     cursor = connection.cursor()

#     # Insert data into the table
#     for _, row in data.iterrows():
#         insert_query = '''
#         INSERT INTO laptops (name, brand, capacity_litres, doors, color, warranty_years, price, reviews, ratings, links, source)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
#         '''
#         cursor.execute(insert_query, tuple(row))

#     # Commit the changes
#     connection.commit()
#     print("Data successfully loaded into PostgreSQL.")

# except (Exception, psycopg2.DatabaseError) as error:
#     print(f"Error: {error}")

# finally:
#     if connection:
#         cursor.close()
#         connection.close()