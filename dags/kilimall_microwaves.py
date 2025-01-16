import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import time

baseurl = "https://www.kilimall.co.ke/"
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.123 Safari/537.36 AOLShield/123.0.6312.3'} 

microwaves_links =[]
for x in range(1, 150): 
        r_microwave = requests.get(f"https://www.kilimall.co.ke/search?q=microwaves&page={x}&source=search|enterSearch|microwaves", headers = headers)
        soup_microwave= BeautifulSoup(r_microwave.content, "html.parser")
        items = soup_microwave.find_all("div", class_="product-item")

        for item in items:
              for link in item.find_all("a", href = True):
                microwaves_links.append("https://www.kilimall.co.ke" + link["href"])
microwaves_list =[]
for x in range(1, 150): 
        r_microwaves = requests.get(f"https://www.kilimall.co.ke/search?q=microwaves&page={x}&source=search|enterSearch|microwaves", headers = headers)
        soup_microwaves= BeautifulSoup(r_microwaves.content, "html.parser")
        items = soup_microwaves.find_all("div", class_="info-box")
        for item in items:
                try:
                    microwaves_name = item.find("p", class_="product-title").text.strip()
                    microwaves_price = item.find("div", class_="product-price").text.strip()
                    microwaves_reviews = item.find("span", class_="reviews").text.strip() if item.find("span", class_="reviews") else "No reviews"
                    a_tag = item.find("a", href=True)
                    microwaves_url = "https://www.kilimall.co.ke" + a_tag["href"] if a_tag else "No URL available"
                
                    # Define a dictionary for each microwave
                    microwave_dict = {
                        "microwaves_name": microwaves_name,
                        "microwaves_reviews": microwaves_reviews,
                        "microwaves_price": microwaves_price,
                    }
                    microwaves_list.append(microwave_dict)

                except AttributeError:
                # Skip items with missing data
                    continue
microwaves_df = pd.DataFrame(microwaves_list)
microwaves_df['microwaves_url'] = microwaves_links
microwaves_df.to_csv(r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\scraped\kilimall_microwaves_scraped.csv")


import pandas as pd
import re

def kilimall_microwaves_clean(csv_path):
    df = pd.read_csv(csv_path)
    df_cleaned = df.dropna()
    df.drop(columns = 'Unnamed: 0', inplace = True)
    df = df.drop_duplicates()
    df["price"] = df['microwaves_price'].str.strip('KSh,').str.replace(",","")
    df['number_of_reviews'] = df["microwaves_reviews"].str.extract(r'(\d+)').astype(int)

    unwanted_words = ['clearance', 'CLEARANCE', 'SALE', 'OFFER', 'Best', 'CHOOSE', 'Offers', 'QUALITY', 'THE', 'FUTURE',
                  'EMBRACE', 'LATEST', 'TREND', 'MEGASALE', 'Buy', 'NOW', 'AND', 'ENJOY', 'UPTO', 'NEW', 'IMPROVED', 
                  'STAY', 'LOCKED','ASSURANCE ', 'WITH', 'STOCK', 'kilimall', 'special', 'MAKE', 'YOUR', 'HOUSE', 'FEEL', 'LIKE', 
                  'Super','LUXURY FOR LESS' ,'CLERAANCE','deal','BUY','OFF', 'HOME With','quality', 'RESTOCKED', 'Share', 'this', 'product', 'Best', 'ARRIVALS', 'HURRY', 
                  'AND', 'PICK', 'YOURS', 'LIMITED', 'AN', 'NO', 'OTHER', 'PRICE', 'REDUCED', 'NOWBLACK', 'Angry', 
                  'mama', 'kitchen','EXPERIENCE', 'New', 'Arrival', 'Classy', 'sale', 'offer', 'best', 'discount', 
                  'cheap', 'deal', 'SALE','Promotions','OFFER','offer','TRUSTED','SOURCE','UPGRADE','THESE','TOP','DURABLE','LISTING','ON','Cooking','End','Original','OF','ALL','affordable']


    pattern = r'\b(?:' + '|'.join(map(re.escape, unwanted_words)) + r')\b'
    df['microwaves_name'] = df["microwaves_name"].str.replace(pattern, '', regex=True).str.strip()
     # Functions to clean the data
    def remove_brackets(column):
        return column.str.replace(r'\[.*?\]', '', regex=True)

    def clean_column(column):
        return column.str.replace(pattern, '', flags=re.IGNORECASE).str.replace(r'\s+', ' ', regex=True).str.strip()

    def remove_symbols(column):
        return column.str.replace(r'[!"+]', '', regex=True)
    
    df['microwaves_name'] = remove_brackets(df['microwaves_name'])
    df['microwaves_name'] = clean_column(df['microwaves_name'])
    df['microwaves_name'] = remove_symbols(df['microwaves_name'])

    def microwaves_clean_name(name):
        name = re.sub(r'[\(\)\{\}\[\]\"!]+', '', name)  # Remove parentheses, brackets, quotes, and exclamation marks
        name = re.sub(r'^\d+\s*', '', name)  # Remove numbers at the start of the string
        return name.strip()  # Remove leading and trailing whitespace
    df["microwaves_name"] = df["microwaves_name"].apply(microwaves_clean_name)
    # Extract brand name and clean it
    def clean_text(text):
        # Remove punctuation, emojis, and parentheses
        text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
        text = re.sub(r'\s*\([^)]*\)\s*', '', text)  # Remove parentheses and their content
        text = re.sub(r'[^\x00-\x7F]+', '', text)  # Remove emojis and non-ASCII characters
        return text.strip()
    df['microwaves_name'] = df ['microwaves_name'].apply(clean_text)
    def extract_brand_name(text):
        words = clean_text(text).split()[:5]  # Get first five words
        return ' '.join(words)
    def extract_capacity(text):
        if not isinstance(text, str):
            return None
        match = re.search(r'\b(\d+\.?\d*)\s*(L|Ltrs|Liters|Litres)\b', text, re.IGNORECASE)
        return match.group(1) if match else None
    def remove_parentheses(text):
        if not isinstance(text, str):
            return text
        return re.sub(r'\s*\([^)]*\)\s*', '', text)
    def extract_brand(microwaves_name):
        # Split the name into words and take the first two or three
        words = microwaves_name.split()
        return ' '.join(words[:3])  
    df["brand"] = df["microwaves_name"].apply(extract_brand)
    df['microwaves_name'] = df['microwaves_name'].apply(extract_brand_name) 
    df['capacity'] = df['microwaves_name'].apply(extract_capacity) 
    df["description"] = df["microwaves_name"]
    df["source"] = ["kilimall"] *len(df)
    df["id"] = [i for i in range(1,len(df)+1)]
    df['urls'] = df['microwaves_url']
    df = df.drop(columns = ["microwaves_name","microwaves_reviews","microwaves_price","microwaves_url"])
    columns = ["id","description",'brand','price','number_of_reviews','capacity','source','urls']
    df = df[columns]
    df.to_csv("data/clean/kilimall_clean_microwaves.csv", index = False)
    return df