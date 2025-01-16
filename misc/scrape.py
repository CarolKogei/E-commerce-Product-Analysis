import requests
from bs4 import BeautifulSoup
import csv
import time
import random

# User-Agent headers for requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# List of URLs to scrape (only one URL for televisions now)
url1 = 'https://www.jumia.co.ke/televisions/#catalog-listing'  # Updated to televisions
url3 = 'https://www.jumia.co.ke/home-cooking-appliances-cookers/'  # Remains unchanged

urls = [url1, url3]  # Removed the duplicate url2

# Function to get the last page number
def get_last_page_number(url):
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to retrieve page. Status code: {response.status_code}")
        return 1  # Default to 1 if the request fails
    
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the 'Last Page' link using its aria-label attribute
    last_page_link = soup.find('a', attrs={'aria-label': 'Last Page'})
    
    if last_page_link and 'href' in last_page_link.attrs:
        last_page_url = last_page_link['href']
        try:
            page_number = last_page_url.split('?page=')[1].split('#')[0]
            return int(page_number)
        except Exception as e:
            print(f"Error extracting last page number: {e}")
            return 1  # Default to 1 if error occurs
    else:
        print("Last page link not found.")
        return 1  # Default to 1 page if no last page is found

# Function to scrape product details from a given URL
def scrape_product_details(url):
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to retrieve the webpage: {url}. Status code: {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    products = soup.find_all('a', class_='core')

    product_details = []

    for product in products:
        # Extract product link (relative URL)
        link = product['href'] if 'href' in product.attrs else None
        # Complete the URL if the link is relative
        link = f"https://www.jumia.co.ke{link}" if link and link.startswith('/') else link
        
        # Extract product name (from the 'name' div)
        name = product.get('data-gtm-name', "N/A")  # Extract from the 'data-gtm-name' attribute"
        
        # Extract product price (actual price) from the 'prc' div
        price = product.find('div', class_='prc').get_text(strip=True) if product.find('div', class_='prc') else "N/A"
        
        # Extract old price (if available) from the 'old' div or alternative span format
        old_price = product.find('div', class_='old').get_text(strip=True) if product.find('div', class_='old') else \
                    product.find('span', class_='-tal -gy5 -lthr -fs16 -pvxs -ubpt').get_text(strip=True) if \
                    product.find('span', class_='-tal -gy5 -lthr -fs16 -pvxs -ubpt') else "N/A"
        
        # Extract discount (if available) from the 'bdg _dsct _sm' div or span with data-disc attribute
        discount = product.find('div', class_='bdg _dsct _sm').get_text(strip=True) if product.find('div', class_='bdg _dsct _sm') else \
                  product.find('span', attrs={'data-disc': True}).get_text(strip=True) if \
                  product.find('span', attrs={'data-disc': True}) else "N/A"
        
        # Extract item ID (from data-gtm-id attribute)
        item_id = product.get('data-gtm-id', "N/A")
        
        # Extract item brand (from data-gtm-brand attribute)
        item_brand = product.get('data-gtm-brand', "N/A")
        
        # Extract the stars rating (from the 'stars _m _al' or 'stars _s' class)
        stars_rating = product.find('div', class_='stars _m _al') or product.find('div', class_='stars _s')
        if stars_rating:
            rating = stars_rating.get_text(strip=True).split(" out of ")[0]  # Extract the rating value (e.g., "3.9")
        else:
            rating = "N/A"
        
        # Extract reviews count (from the 'rev' class or 'verified ratings' link)
        reviews = product.find('div', class_='rev')
        if reviews:
            reviews_count = reviews.get_text(strip=True).split('(')[-1].replace(')', '')  # Extract the review count (e.g., "798")
        else:
            reviews_link = product.find('a', class_='-plxs _more')
            reviews_count = reviews_link.get_text(strip=True).split('(')[-1].replace(')', '') if reviews_link else "N/A"
        
        # Store all the extracted product details
        product_details.append({
            'link': link,
            'name': name,
            'discounted_price': price,
            'previous_price': old_price,
            'discount_%': discount,
            'id': item_id,
            'brand': item_brand,
            'ratings': rating,
            'reviews_count': reviews_count
        })
    
    return product_details

# Iterate over all URLs to scrape data and save to a CSV file
for url in urls:
    # Get the last page number for the current URL
    last_page = get_last_page_number(url)

    # Initialize an empty list to store all products
    products_list = []

    # Iterate through all pages from 1 to the last page
    for page_num in range(1, last_page + 1):
        page_url = f"{url}?page={page_num}#catalog-listing"
        print(f"Scraping page {page_num} from {url}...")
        
        # Scrape the products from the current page
        products = scrape_product_details(page_url)
        products_list.extend(products)  # Add the scraped products to the main list

        # Sleep for a random time between requests to avoid overwhelming the server
        time.sleep(random.uniform(1, 3))

    # Determine the output CSV file name based on the URL
    if url == url1:
        csv_filename = 'jumia_scraped_televisions.csv'  # Updated filename for televisions
    else:
        csv_filename = 'jumia_scraped_cookers.csv'

    # Save the scraped product details to a CSV file
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=[ 
            'name', 'discounted_price', 'previous_price', 'discount_%', 'id', 'brand', 'ratings', 'reviews_count', 'link'
        ])
        writer.writeheader()  # Write the header row
        
        for product in products_list:
            writer.writerow(product)

    print(f"Scraped {len(products_list)} products from {url} and saved them to '{csv_filename}'.")
