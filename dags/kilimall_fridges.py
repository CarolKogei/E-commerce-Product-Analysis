#Importing libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re


# Initialize a list to store all fridges data
fridges = []

# Loop through all 16 pages
for x in range(1, 17):  # Pages 1 to 16
    print(f"Scraping page {x}...")
    
    # Send a GET request to the page
    result = requests.get(f'https://www.kilimall.co.ke/search?q=FRIDGE&page={x}&source=search|enterSearch|FRIDGE')
    
    # Check if the request was successful
    if result.status_code == 200:
        soup = BeautifulSoup(result.text, 'html.parser')  # Parse the HTML content
        
        # Extract fridge details from divs with the class "info-box".
        fridges_info = soup.find_all('div', class_="info-box")
        fridges_links = soup.find_all("div", class_="product-item")
        
        # Extract relevant details and links
        for fridge_info, fridge_link in zip(fridges_info, fridges_links):
            # Safely extract data, handle cases where tags are missing
            fridge_name = fridge_info.find('p', class_='product-title')
            fridge_price = fridge_info.find('div', class_='product-price')
            fridge_reviews = fridge_info.find('span', class_='reviews')
            
            # Extract link from product-item div
            link_tag = fridge_link.find("a", href=True)
            fridge_link_url = "https://www.kilimall.co.ke" + link_tag["href"] if link_tag else "N/A"
            
            # Clean and append extracted data
            fridges.append({
                "name": fridge_name.text.strip() if fridge_name else "N/A",
                "price": fridge_price.text.strip() if fridge_price else "N/A",
                "reviews": fridge_reviews.text.strip() if fridge_reviews else "N/A",
                "links": fridge_link_url
            })
    else:
        print(f"Failed to fetch page {x}, Status code: {result.status_code}")

# Save results as a DataFrame
df_fridges = pd.DataFrame(fridges)



#FRIDGES DATA CLEANING
# Function to clean the product name
def clean_name(name):
    # Remove words in parentheses or curly brackets if they contain "offer", "offers", "sale", or "sales"
    name = re.sub(r'\(([^)]*?(OFFER|OFFERS|SALE|SALES)[^)]*?)\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\{([^}]*?(OFFER|OFFERS|SALE|SALES)[^}]*?)\}', '', name, flags=re.IGNORECASE)
    # Remove variations of "offer" and "sale" (including "offers", "sales")
    name = re.sub(r'\b(\w+)\s+(OFFER|OFFERS|SALE|SALES)\b', '', name, flags=re.IGNORECASE)
    # Remove unnecessary marketing phrases
    name = re.sub(r'\b(BLACK FRIDAY|BLACK FRIDAY OFFERS|BEST DEALS|LIMITED|LIMITED TIME|TECH WEEK|OFFER|BEST WHOLESALE PRICE|SPECIAL OFFERS)\b', '', name, flags=re.IGNORECASE)
    # Remove all remaining parentheses, curly braces, brackets, and clean extra spaces
    name = re.sub(r'[\(\)\{\}\[\]]', '', name)  # Remove parentheses, braces, and brackets
    name = re.sub(r'\s+', ' ', name)  # Replace multiple spaces with a single space
    # Remove special characters like '!', '+' if they appear as the first word
    name = re.sub(r'^[!+\[\]]+', '', name).strip()  # Strip unwanted characters at the start
    # Remove emojis using a regex for unicode emoji ranges
    name = re.sub(r'[^\w\s,.-]', '', name)  # Remove non-alphanumeric characters (including emojis)
    # Final trim to remove leading/trailing spaces
    name = name.strip()
    return name
# Apply the cleaning function to the 'Name' column in the DataFrame
df_fridges['name'] = df_fridges['name'].apply(clean_name)


#Remove commas and any text from Price column
df_fridges["price"] = df_fridges['price'].str.replace(r'[^\d]', '', regex=True)

#Remove brackets from Reviews column
df_fridges['reviews'] = df_fridges['reviews'].str.extract(r'(\d+)')




# Extract the number of doors
def extract_doors(description):
    # Define the regex pattern to match numbers/keywords before "Door" or "Doors"
    pattern = r'\b(1|one|2|two|3|three|4|four|Single|Double)\b(?:\s*Doors)?'
    # Search for the pattern in the description
    match = re.search(pattern, description, re.IGNORECASE)
    # Map matches to corresponding numeric values
    door_mapping = {
        "1": 1,
        "one": 1,
        "single": 1,
        "2": 2,
        "two": 2,
        "double": 2,
        "4": 4,
        "four": 4}
    if match:
        door_type = match.group(1).lower()  # Convert the match to lowercase
        return door_mapping.get(door_type, "Unknown")  # Map to the number of doors
    return "Unknown"  # If no match is found



# Extract capacity in litres
def extract_capacity(description):
    # Define the regex pattern
    pattern = r'(\d+(\.\d+)?)\s*(L|litres|ltrs|lt)'
    # Search for the pattern in the description
    match = re.search(pattern, description, re.IGNORECASE)
    if match:
        return float(match.group(1))  # Return the number as float
    return None  # Return None if no match is found




#Extract brand names
brands = ['Volsmart','Hisense','Roch','Nunnix','Smartpro','Nunix','Ecomax','Ramtons','Mika','Von','Haier','Exzel','GLD','Vitron','Smartpro','Bruhm','Premier','Samsung', 'Ailyons', 'LG', 'Solstar', 'Royal','Beko','Syinix','ICECOOL','Rebune','Legacy','FK','Smart pro']
# Function to extract the brand name
def extract_brand(product_name):
    for brand in brands:
        if brand.lower() in product_name.lower():  # Case insensitive match
            return brand
    return 'Unknown'  # Return 'Unknown' if no brand is found



# Apply the extraction functions to the DataFrame
df_fridges["doors"] = df_fridges["name"].apply(extract_doors)
df_fridges['capacity_litres'] = df_fridges['name'].apply(extract_capacity)
df_fridges['brand'] = df_fridges['name'].apply(extract_brand)

# Standardize brand names (replace "Smart Pro" with "Smartpro")
df_fridges['brand'] = df_fridges['brand'].replace({'Smart pro': 'Smartpro','Nunnix': 'Nunix'})
df_fridges['brand'].value_counts()


# Add a new column named 'source' with the value 'kilimall' for all rows
df_fridges['source'] = 'Kilimall'

# Rearrange columns: specify the new order
df_fridges = df_fridges[['name', 'brand', 'capacity_litres','doors','price','reviews','links','source']]

df_fridges.to_csv(r'..\data\clean\kilimall_fridges.csv', index=True)