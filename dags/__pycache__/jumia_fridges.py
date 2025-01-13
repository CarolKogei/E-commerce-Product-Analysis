# Import libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import configparser
import psycopg2

# Extracting fridges data.
# List to store all fridges data
fridges = []

# Loop through pages 1 to 30
for page in range(1, 30):
    print(f'Scraping page {page}...')

    # Send a GET request
    result =requests.get(f'https://www.jumia.co.ke/appliances-fridges-freezers/')
    content = result.text

    # Parse the HTML content
    soup = BeautifulSoup(content, features="html.parser")

    # Find all fridge elements on the current page
    fridges_info = soup.find_all('article', class_="prd _fb col c-prd")

    # Loop through each fridge and extract data
    for fridge_info in fridges_info:
        try:
            # Extract fridge details
            fridge_name = fridge_info.find('h3', class_='name').text.strip()
            fridge_price = fridge_info.find('div', class_='prc').text.strip()
            fridge_reviews = fridge_info.find('div', class_='rev').text.strip()
            fridge_ratings = fridge_info.find('div', class_='stars _s').text.strip()
            fridge_links = fridge_info.find('a', class_='core').text.strip()['href']

            # Append fridge details
            fridges.append({
                "Name" : fridge_name,
                "Price": fridge_price,
                "Reviews": fridge_reviews,
                "Ratings": fridge_ratings,
                "Links": "https://www.jumia.co.ke{fridge_links}"
            })
        
        except AttributeError:
           # Incase of missing fridge info
           continue

# Save the extracted data to a CSV file.
df = pd.DataFrame(fridges)
df.to_csv('data/scrape/fridges.csv', index=True, encoding='utf-8')

# Read raw fridges data
fridges_df = pd.read_csv("data/scrape/fridges.csv")

# Extract brand name and model.
def extract_brand_and_model(name):
    match = re.match(r"([A-Za-z]+(?: [A-Za-z]+)*)(?:\s[RF|REF|FM|DF|D|]{2,4}[\d]+)?", name)
    if match:
        return match.group(1).strip()
    return ''

# Extract size in litres
def extract_size(name):
    match = re.search(r'(\d+)\s*Litres?', name)
    if match:
        return int(match.group(1))
    return None

# Extract number of doors
def extract_doors(name):
    match = re.search(r'(\d+)\s*Door', name)
    if match:
        return int(match.group(1))
    return None

# Extract color
def extract_color(name):
    color_keywords = ['Silver', 'White', 'Black', 'Grey', 'Red', 'Blue', 'Green', 'Beige', 'Stainless', 'Chrome']
    for color in color_keywords:
        if color.lower() in name.lower():
            return color
    return None

# Extract warranty
def extract_warranty(name):
    match = re.search(r'(\d+)\s*YRs?\s*WRTY', name)
    if match:
        return int(match.group(1))
    return None

# Extract the price from the 'Price' column
def extract_price(price):
    match = re.search(r'KSh\s*(\d+([,]\d{3})*)', price)
    if match:
        return float(match.group(1).replace(',', ''))
    return None

# Extract reviews 
def extract_reviews(reviews):
    match = re.search(r'\((\d+)\)', reviews)
    if match:
        return int(match.group(1))
    return None

# Extract ratings (the number before "out of 5")
def extract_ratings(ratings):
    match = re.search(r'(\d+\.\d+)', ratings)
    if match:
        return float(match.group(1))
    return None

# Apply the extraction functions to the DataFrame
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

data = fridges_df[['name', 'brand', 'capacity_litres','doors', 'color', 'warranty_years', 'price', 'reviews', 'ratings', 'links']].copy()
data['source'] = 'Jumia'

# Save the modified DataFrame to a new CSV file
data.to_csv('data/clean/fridges_clean.csv', index=True)

# Load cleaned data to PostgreSQL
try:
    # Establish database connection
    connection = psycopg2.connect(
        host=db_params['host'],
        database=db_params['database'],
        user=db_params['user'],
        password=db_params['password'],
        port=db_params['port']
    )
    cursor = connection.cursor()

    # Insert data into the table
    for _, row in data.iterrows():
        insert_query = '''
        INSERT INTO laptops (name, brand, capacity_litres, doors, color, warranty_years, price, reviews, ratings, links, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        '''
        cursor.execute(insert_query, tuple(row))

    # Commit the changes
    connection.commit()
    print("Data successfully loaded into PostgreSQL.")

except (Exception, psycopg2.DatabaseError) as error:
    print(f"Error: {error}")

finally:
    if connection:
        cursor.close()
        connection.close()