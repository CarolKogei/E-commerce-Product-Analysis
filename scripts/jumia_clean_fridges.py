# Import libraries
import pandas as pd
import re

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