# Import libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd

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
            laptop_link = laptop_info.find('a', class_='core')['href']

            # Append the data to the list
            laptops.append({
                "Name": laptop_name,
                "Price": laptop_price,
                "Reviews": laptop_reviews,
                "Ratings": laptop_ratings,
                "Link": f"https://www.jumia.co.ke{laptop_link}"
            })
        except AttributeError:
            # In case of missing data for any laptop, skip to the next laptop
            continue

# Save the extracted data to a CSV file
df = pd.DataFrame(laptops)
df.to_csv('../data/scrapped/jumia_laptops.csv', index=False, encoding='utf-8')

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
            fridge_link = fridge_info.find('a', class_='core')['href']

            # Append fridge details
            fridges.append({
                "Name" : fridge_name,
                "Price": fridge_price,
                "Reviews": fridge_reviews,
                "Ratings": fridge_ratings,
                "Link": f"https://www.jumia.co.ke{fridge_link}"
            })
        
        except AttributeError:
           # Incase of missing fridge info
           continue

# Save the extracted data to a CSV file.
df = pd.DataFrame(fridges)
df.to_csv('../data/scrapped/jumia_fridges.csv', index=False, encoding='utf-8')
