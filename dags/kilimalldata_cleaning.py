import re
import pandas as pd

#Fridges data
df_fridges= pd.read_csv("csv_files/fridges.csv")
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
df_fridges['Name'] = df_fridges['Name'].apply(clean_name)


#Remove commas and any text from Price column
df_fridges["Price"] = df_fridges['Price'].str.replace(r'[^\d]', '', regex=True)
# Rename the Price column
df_fridges = df_fridges.rename(columns={'Price': 'Price(kshs)'})


#Remove brackets from Reviews column
df_fridges['Reviews'] = df_fridges['Reviews'].str.extract(r'(\d+)')




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
df_fridges["Doors"] = df_fridges["Name"].apply(extract_doors)
df_fridges['Capacity(ltrs)'] = df_fridges['Name'].apply(extract_capacity)
df_fridges['Brand'] = df_fridges['Name'].apply(extract_brand)


# Display the updated DataFrame
df_fridges 