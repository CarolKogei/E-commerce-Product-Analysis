# Import libraries
import pandas as pd
import re 

# Laptops data
laptops_df = pd.read_csv("../data/scraped/jumia_laptops.csvv")

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
laptop_df['link'] = laptop_df['Link']

# Create the new DataFrame with the extracted data
cleaned_data = laptops_df[['name','brand', 'ram', 'rom', 'processor', 'screen_size', 'price','reviews','ratings', 'link']]

# Save the cleaned data to a new CSV file
cleaned_data.to_csv('../data/clean/jumia_laptops.csv', index=False)
