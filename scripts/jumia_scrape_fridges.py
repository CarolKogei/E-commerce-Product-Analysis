# Import libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd

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
            fridge_links = fridge_info.find('a', class_='core')['href'].text.strip()

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