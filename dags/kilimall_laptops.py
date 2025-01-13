#Importing libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re


# Initialize a list to store all laptops' data
laptops = []

# Loop through all 90 pages
for x in range(1, 91):  # Pages 1 to 90
    print(f"Scraping page {x}...")

    # Send a GET request to the page
    result = requests.get(f'https://www.kilimall.co.ke/search?q=laptop&page={x}&source=search|enterSearch|laptop')
    
    # Check if the request was successful
    if result.status_code == 200:
        soup = BeautifulSoup(result.text, 'html.parser')  # Parse the HTML content
        
        # Extract fridge details from divs with the class "info-box".
        laptops_info = soup.find_all('div', class_="info-box")
        laptops_links = soup.find_all("div", class_="product-item")
        
        # Extract relevant details and links
        for laptop_info, laptop_link in zip(laptops_info, laptops_links):
            # Safely extract data, handle cases where tags are missing
            laptop_name = laptop_info.find('p', class_='product-title')
            laptop_price = laptop_info.find('div', class_='product-price')
            laptop_reviews = laptop_info.find('span', class_='reviews')
            
            # Extract link from product-item div
            link_tag = laptop_link.find("a", href=True)
            laptop_link_url = "https://www.kilimall.co.ke" + link_tag["href"] if link_tag else "N/A"
            
            # Clean and append extracted data
            laptops.append({
                "name": laptop_name.text.strip() if laptop_name else "N/A",
                "price": laptop_price.text.strip() if laptop_price else "N/A",
                "reviews": laptop_reviews.text.strip() if laptop_reviews else "N/A",
                "links": laptop_link_url
            })
    else:
        print(f"Failed to fetch page {x}, Status code: {result.status_code}")

#Save results as a DataFrame
df_laptops = pd.DataFrame(laptops)

#LAPTOPS DATA CLEANING
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



# Extract the screen size (integer or float)
def extract_screen_size(description):
    # Regex to capture floats/integers before "inch", "inches", or `"`
    match = re.search(r"(\d+\.\d+|\d+)(?=\s*(?:''|\"|inch|inches?))", description, re.IGNORECASE)
    # If a match is found, return the captured number
    # Regex to capture floats/integers directly before `"`
    match_quote = re.search(r'(\d+\.?\d*)\s*(?=")', description)
    if match:
        return float(match.group(1))  # Return match before "inch" or "inches"
    elif match_quote:
        return match_quote.group(1)  # Return match with `"`
    else:
        return np.nan  # Return NaN if no match is found

 


# Extract RAM
def extract_ram(name):
    match = re.search(r'(\d+)\s*GB\s*RAM', name, re.IGNORECASE)  # Adjust regex to allow flexible spacing
    return f"{match.group(1)}GB" if match else 'Unknown'  # Return the RAM size with "GB" appended




# Clean and extract RAM
def clean_and_extract_ram(row):
    # Step 1: Clean the RAM value if it contains 58GB, 78GB, 116GB, 716GB, or 516GB
    row['RAM'] = re.sub(r'\b(58GB|78GB)\b', '8GB', row['RAM'])  # Replace 58 GB and 78 GB with 8GB
    row['RAM'] = re.sub(r'\b(116GB|716GB|516GB)\b', '16GB', row['RAM'])  # Replace 116gb, 716gb, 516gb with 16gb
    # Step 2: If RAM is unknown, extract the valid RAM value from the 'Name' column
    if row['RAM'] == "Unknown":  # Only process if the RAM is unknown
        # Define the RAM pattern (limited to 4GB, 8GB, 16GB, 32GB, 64GB)
        ram_pattern = r'\b(4|8|16|32|64)\s*GB'
        # Attempt to find the RAM in the 'Name' field
        match = re.findall(ram_pattern, row['Name'])
        if match:
            # Return the first match found (assuming we want just the first match)
            row['RAM'] = match[0] + "GB"  # Append 'GB' to match
    return row['RAM']


# Extract ROM with or without SSD/HDD
def extract_rom(name):
    # Define the regex pattern to capture storage sizes (with or without space) and optional SSD/HDD
    pattern = r'\b(128\s*GB|256\s*GB|250\s*GB|320\s*GB|500\s*GB|512\s*GB|750\s*GB|1000\s*GB|1\s*TB|2\s*TB)\b(?:\s*(HDD|SSD|BROM|Storage))?'
    # Search for a match
    match = re.search(pattern, name, re.IGNORECASE)
    if match:
        # Extract the size and optional type
        size = match.group(1).replace(' ', '').upper()  
        storage_type = match.group(2).upper() if match.group(2) else ""  # Get the type if it exists
        return f"{size} {storage_type}".strip()  # Combine size and type, removing extra spaces
    else:
        return 'Unknown'  # Default if no match found


#Extract processor model
def extract_processor(name):
    # Define a regex pattern to capture only the processor model (i3, i5, i7, i9)
    pattern = r'\b(i[3-9])\b'  # Match i3, i5, i7, i9 with word boundaries
    match = re.search(pattern, name, re.IGNORECASE)
    if match:
        processor_model = match.group(0).lower()  # Extract the processor model (i3, i5, i7, i9)
        return f"Intel Core {processor_model}"  # Prepend "Intel Core" to the processor model
    else:
        return 'Unknown'  # Return 'Unknown' if no valid processor is found




# Apply the cleaning function to the 'Name' column in the DataFrame
df_laptops['name'] = df_laptops['name'].apply(clean_name)
df_laptops['RAM'] = df_laptops.apply(clean_and_extract_ram, axis=1)
df_laptops['screen_size'] = df_laptops['name'].apply(extract_screen_size) 
df_laptops['RAM']=df_laptops['name'].apply(extract_ram)
df_laptops['ROM'] =df_laptops['name'].apply(extract_rom)
df_laptops['processor'] = df_laptops['name'].apply(extract_processor)


#Remove commas and any text from Price column
df_laptops["price"] = df_laptops['price'].str.replace(r'[^\d]', '', regex=True)


#Remove brackets from Reviews column
df_laptops['reviews'] = df_laptops['reviews'].str.extract(r'(\d+)')


# Add a new column named 'source' with the value 'kilimall' for all rows
df_laptops['source'] = 'Kilimall'





# Save to CSV
df_laptops.to_csv(r'..\data\clean\kilimall_laptops.csv', index=True)