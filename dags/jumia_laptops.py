# Import libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
import configparser
import psycopg2

# List to store all laptops' data
laptops = []

# Loop through pages 1 to 50
for page in range(1, 51):  # Pages 1 to 50
    print(f"Scraping page {page}...")

    # Send a GET request to the page, including the page number in the URL     
    result = requests.get(f'https://www.jumia.co.ke/laptops/?page=2#catalog-listing{page}')
    content = result.text

    # Parse the HTML content
    soup = BeautifulSoup(content, features="html.parser")

    # Find all laptop elements on the current page
    laptops_info = soup.find_all('article', class_="prd _fb col c-prd")

    # Loop through each laptop and extract data
    for laptop_info in laptops_info:
        try:
            # Extract laptop details
            laptop_name = laptop_info.find('h3', class_='name').text.strip()
            laptop_price = laptop_info.find('div', class_='prc').text.strip()
            laptop_reviews = laptop_info.find('div', class_='rev').text.strip()
            laptop_ratings = laptop_info.find('div', class_='stars _s').text.strip()
            laptop_links = laptop_info.find('a', class_='core')['href'].strip()

            # Append the data to the list
            laptops.append({
                "Name": laptop_name,
                "Price": laptop_price,
                "Reviews": laptop_reviews,
                "Ratings": laptop_ratings,
                "Links": f"https://www.jumia.co.ke{laptop_links}"
            })
        except AttributeError:
            # In case of missing data for any laptop, skip to the next laptop
            continue


# Save the extracted data to a CSV file
df = pd.DataFrame(laptops)
df.to_csv('data/scraped/laptops.csv', index=True, encoding='utf-8')

# Data Cleaning
laptops_df = pd.read_csv("data/scraped/laptops.csv")

# Extract brand name
def extract_brand(name):
    match = re.search(r'(HP|Lenovo|Dell|Acer|)', name, re.IGNORECASE)
    return match.group(0) if match else 'Unknown'

# Extract RAM
def extract_ram(name):
    match = re.search(r'(\d+GB)\s*RAM', name)
    return match.group(1) if match else 'Unknown'

# Extract ROM (HDD/SSD)
def extract_rom(name):
    match = re.search(r'(\d+GB|TB)\s*(HDD|SSD)', name)
    return match.group(0) if match else 'Uknown'

# Extract processor type
def extract_processor(name):
    match = re.search(r'Intel\s*(Core\s*I\d)', name)
    return match.group(1) if match else 'Unknown'
                    
def extract_screen_size(name):
    match = re.search(r'(\d+\.?\d*)"\s*', name)
    return match.group(1) if match else 'Unknown'

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

# Apply extraction functions to the DataFrame
laptops_df['name'] = laptops_df['Name']
laptops_df['brand'] = laptops_df['Name'].apply(extract_brand)
laptops_df['ram'] = laptops_df['Name'].apply(extract_ram)
laptops_df['rom'] = laptops_df['Name'].apply(extract_rom)
laptops_df['processor'] = laptops_df['Name'].apply(extract_processor)
laptops_df['screen_size'] = laptops_df['Name'].apply(extract_screen_size)
laptops_df['price'] = laptops_df['Price'].apply(extract_price)
laptops_df['reviews'] = laptops_df['Reviews'].apply(extract_reviews)
laptops_df['ratings'] = laptops_df['Ratings'].apply(extract_ratings)
laptops_df['links'] = laptops_df['Links']

# Create the new DataFrame with the extracted data
cleaned_data = laptops_df[['name','brand', 'ram', 'rom', 'processor', 'screen_size', 'price','reviews','ratings', 'links']].copy()
cleaned_data['source'] = 'Jumia'

# add id coumn based on df index
cleaned_data.insert(0, 'id', cleaned_data.index)

# Save the cleaned data to a new CSV file
cleaned_data.to_csv('data/clean/laptops_clean.csv', index=False)

# # Load cleaned data to PostgreSQL
# try:
#     # Establish database connection
#     connection = psycopg2.connect(
#         host='localhost',
#         database='airflow',
#         user='airflow',
#         password='airflow',
#         port=5432
  
#         # host=db_params['host'],
#         # database=db_params['database'],
#         # user=db_params['user'],
#         # password=db_params['password'],
#         # port=db_params['port']
#     )
#     cursor = connection.cursor()

#     # Insert data into the table
#     for _, row in cleaned_data.iterrows():
#         insert_query = '''
#         INSERT INTO laptops (name, brand, ram, rom, processor, screen_size, price, reviews, ratings, links, source)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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