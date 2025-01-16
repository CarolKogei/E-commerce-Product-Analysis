import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
baseurl = "https://www.kilimall.co.ke/"
headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.6312.123 Safari/537.36 AOLShield/123.0.6312.3'} 


phones_links = []  # List to store phone details

for x in range(1, 150):  # Loop through the pages
    r_phone = requests.get(f"https://www.kilimall.co.ke/search?q=phones&page={x}&source=search|enterSearch|phones", headers = headers)
    soup_phone = BeautifulSoup(r_phone.content, "html.parser")
     # Find all phone product containers
    phone_items = soup_phone.find_all("div", class_="product-item")
    for item in phone_items :
        for link in item.find_all("a", href = True):
            phones_links.append("https://www.kilimall.co.ke" + link["href"])
phone_list =[]
for x in range(1, 150):  # Loop through the pages
    r_phones = requests.get(f"https://www.kilimall.co.ke/search?q=phones&page={x}&source=search|enterSearch|phones", headers = headers)
    soup_phones = BeautifulSoup(r_phones.content, "html.parser")
    phones_list = soup_phones.find_all("div", class_ = "info-box")
    for item in phones_list:
        try:
            phones_name = item.find("p", class_="product-title").text.strip()
            phones_price = item.find("div", class_="product-price").text.strip()
            phones_reviews = item.find("span", class_="reviews").text.strip() if item.find("span", class_="reviews") else "No reviews"
            a_tag = item.find("a", href=True)
            phones_url = "https://www.kilimall.co.ke" + a_tag["href"] if a_tag else "No URL available"
            
            
            # Create a dictionary for each phone
            phones_dict = {
                "phones_name": phones_name,
                "phones_reviews": phones_reviews,
                "phones_price": phones_price,
            }
            
            phone_list.append(phones_dict)
        except AttributeError:
            # Skip items with missing details
            continue
phones_df = pd.DataFrame(phone_list)
phones_df["phone_url"] = phones_links
phones_df.to_csv(r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\scraped\kilimall_phones_scraped.csv")
print(f"Total phones scraped: {len(phone_list)}")


def kilimall_phones_clean(csv_path):
    #import data
    dataframe = pd.read_csv(csv_path)
    dataframe.drop(columns = 'Unnamed: 0', inplace = True)
    dataframe = dataframe.drop_duplicates()
    dataframe['price']= dataframe['phones_price'].str.strip('KSh,').str.replace(",","")
    dataframe['number_of_reviews'] = dataframe['phones_reviews'].str.extract(r'(\d+)').astype(int)
    # List of words to remove
    words_to_remove = [
    'xmas', 'sale', 'new','New Arrival', 'Xmas','Promotion','Year','phones','arrival', 'brand', 'refurbished', 'year', 'sales', 'promotion', 
    'original', 'super', 'limited', 'offer', 'holiday', 'offers', 'black', 'friday', 'deals', 
    'arrivals', 'christmas', 'smart', 'end', 'free', 'charger', 'official', 'phones', 'special', 'Smart',
    'buy', 'big', 'launch', 'november','Limited','OFFER','Brand','New','Face','unlock','Arrival','Black','Friday','NEW','CHEAP','SMART','BLACK','NOVEMBER','OFFERS','Unlocked','Phones','BRAND','Original','Free','Charger','Exclusive','Special','Offer','Phone','LIMITED'
    'anniversary','Arrivals','Best','for','a','Refirbishe','time','BRANDNEW','CHRISTMAS','HOLIDAY','OFFEROFFER','REFURBISHED','END','YEAR','Buy','Official','Offiicial','BIG','DEALS','ARRIVALS','OFFERS','Unlock','Google','system','Techweek','mobile','Harambee','phone','Global','Version'
    ,'Refurbishe','Systems','Edition','Honor','ORIGINAL','Freeyond','machine','The','Refur','version','Latest','SOWHAT'
    ,'GRAB','QUICK','SALE','Firmware','Super','Sale','Badili','FreeYond','Smartphone','HONOR','Cell','net','Top','International','edition','high','matching','EASTER','Sharp','YOUR'
    ,'certified','Certified','DISCOUNT','SPECIAL','SPECIAL','ARRIVAL','Renovate','SEALED','OFFERSBRAND','EXPANDABLE','FRIDAY','FLASH','LIMTED'
    ]
    pattern = r'\b(?:' + '|'.join(map(re.escape, words_to_remove)) + r')\b'

    dataframe['phones_name'] = dataframe['phones_name'].str.replace(pattern, '', regex=True).str.strip()


    # Functions to clean the data
    def remove_brackets(column):
        return column.str.replace(r'\[.*?\]', '', regex=True)

    def clean_column(column):
        return column.str.replace(pattern, '', flags=re.IGNORECASE).str.replace(r'\s+', ' ', regex=True).str.strip()

    def remove_symbols(column):
        return column.str.replace(r'[!"+]', '', regex=True)

    # Apply cleaning functions
    dataframe['phones_name'] = remove_brackets(dataframe['phones_name'])  # Remove text in square brackets
    dataframe['phones_name'] = clean_column(dataframe['phones_name'])    # Remove unwanted words
    dataframe['phones_name'] = remove_symbols(dataframe['phones_name'])  # Remove special characters
    dataframe["phones_name"] = dataframe["phones_name"].str.replace("Refurbished", "")

    def clean_phones_name(name):
    # Remove unwanted characters: (), {}, !!, [], "", and numbers before the first letter
        name = re.sub(r'[\(\)\{\}\[\]\"!]+', '', name)  # Remove parentheses, brackets, quotes, and exclamation marks
        name = re.sub(r'^\d+\s*', '', name)  # Remove numbers at the start of the string
        return name.strip()  # Remove leading and trailing whitespace

    # Apply the cleaning function to the phones_name column
    dataframe["phones_name"] = dataframe["phones_name"].apply(clean_phones_name)

    def remove_non_alphabet_start(name):
        return re.sub(r'^[^a-zA-Z]+', '', name)  # Remove any non-alphabet characters from the start

    # Apply the function to the phones_name column
    dataframe["phones_name"] = dataframe["phones_name"].apply(remove_non_alphabet_start)


    ram_pattern = r'\b(2|3|4|6|8|12|24)\s*GB'

    # Function to extract RAM size
    def extract_ram(phones_name):
        ram = re.findall(ram_pattern, phones_name)  # Find all occurrences that match the pattern
        return ram[0] if ram else None  # If found, return the first match; else return None

    storage_pattern = r'\b(16|32|64|128|256)\s*GB'
    def extract_storage(phones_name):
        storage = re.findall(storage_pattern, phones_name)  # Find all occurrences that match the pattern
        return storage[0] if storage else None  # If found, return the first match; else return None
    def extract_brand(phones_name):
        # Split the name into words and take the first two or three
        words = phones_name.split()
        return ' '.join(words[:3])  # Return the first three words (or fewer if not available)
    def extract_battery(phones_name):
        # Search for a pattern that captures numbers followed by 'mAh'
        match = re.search(r'(\d+)\s*mAh', phones_name)
        if match:
            return match.group(1)  # Return the first captured group (the number)
        return None  # If no match, return None

    # Apply the function to the 'phones_name' column and create a new 'battery' column
    dataframe['battery'] = dataframe['phones_name'].apply(extract_battery)
    # Apply the function to the 'phones_name' column and create a new 'storage' column
    dataframe['ram'] = dataframe['phones_name'].apply(extract_ram)
    dataframe['storage'] = dataframe['phones_name'].apply(extract_storage)
    # Apply the function to the 'phones_name' column and create a new 'brand' column
    dataframe['brand'] = dataframe['phones_name'].apply(extract_brand)
    dataframe["description"] = dataframe['phones_name']
    dataframe['source'] = ['kilimall'] *len(dataframe)
    dataframe['id'] = [i for i in range(1,len(dataframe)+1)]
    dataframe['urls'] = dataframe['phone_url']
    dataframe = dataframe.drop(columns = ['phones_name', 'phones_reviews', 'phones_price', 'phone_url'])
    # dataframe.rename({""})
    columns = ["id", "description", "brand", "price", "number_of_reviews", "ram", "storage", "battery", "source", "urls"]
    dataframe = dataframe[columns]
    dataframe.to_csv("../data/clean/kilimall_clean_phones.csv", index = False)


    

    return dataframe





csv_path =(r"C:\Users\Vivian.Obino1\Desktop\e-commerce analysis\data\scraped\kilimall_phones_scraped.csv")
dataframe = kilimall_phones_clean(csv_path)
dataframe

