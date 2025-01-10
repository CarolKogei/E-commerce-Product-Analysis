import requests
from bs4 import BeautifulSoup
import csv
import time
import random
import sys
import os

# Add the scripts directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the improved database insertion function
from improved_db_insertion import insert_products_to_db

# URLs and headers
url1 = 'https://www.jumia.co.ke/televisions/#catalog-listing'
url2 = 'https://www.jumia.co.ke/home-cooking-appliances-cookers/#catalog-listing'

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_last_page_number(url, headers):
    """
    Get the last page number for pagination
    """
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find link for last page using its aria-label attribute
        last_page_link = soup.find('a', attrs={'aria-label': 'Last Page'})
        
        if last_page_link and 'href' in last_page_link.attrs:
            last_page_url = last_page_link['href']
            try:
                page_number = last_page_url.split('?page=')[1].split('#')[0]
                return int(page_number)
            except Exception as e:
                print(f"Error extracting last page number: {e}")
                return 1  # Default to 1 if error occurs
        
        return 1  # Default to 1 page if no last page link found
    
    except Exception as e:
        print(f"Error in get_last_page_number: {e}")
        return 1

def scrape_product_details(url, headers, max_pages=50):
    """
    Scrape product details from multiple pages
    """
    all_products = []
    
    # Get total number of pages
    last_page = min(get_last_page_number(url, headers), max_pages)
    
    for page in range(1, last_page + 1):
        # Construct paginated URL
        paginated_url = f"{url}?page={page}" if page > 1 else url
        
        print(f"Scraping page {page} from {paginated_url}...")
        
        try:
            response = requests.get(paginated_url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all product elements
            products = soup.find_all('a', class_='core')
            
            if not products:
                print(f"No products found on {paginated_url}.")
                continue
            
            for product in products:
                try:
                    # Extract product details
                    link = product['href'] if 'href' in product.attrs else None
                    link = f"https://www.jumia.co.ke{link}" if link and link.startswith('/') else link
                    
                    name = product.find('h3', class_='name').get_text(strip=True) if product.find('h3', class_='name') else "N/A"
                    price = product.find('div', class_='prc').get_text(strip=True) if product.find('div', class_='prc') else "N/A"
                    old_price = product.find('div', class_='old').get_text(strip=True) if product.find('div', class_='old') else "N/A"
                    discount = product.find('div', class_='bdg _dsct _sm').get_text(strip=True) if product.find('div', class_='bdg _dsct _sm') else "N/A"
                    
                    # Rating and reviews
                    rating_elem = product.find('div', class_='rev')
                    rating = rating_elem.find('div', class_='stars _s').get_text(strip=True) if rating_elem and rating_elem.find('div', class_='stars _s') else "N/A"
                    reviews = rating_elem.get_text(strip=True).split('(')[-1].strip(')') if rating_elem and '(' in rating_elem.get_text() else "N/A"
                    
                    # Additional product attributes
                    item_id = product.get('data-gtm-id', "N/A")
                    item_brand = product.get('data-gtm-brand', "N/A")
                    
                    # Append product details
                    all_products.append({
                        'id': item_id,
                        'name': name,
                        'discounted_price': price,
                        'previous_price': old_price,
                        'discount_%': discount,
                        'brand': item_brand,
                        'rating': rating,
                        'reviews_count': reviews,
                        'link': link
                    })
                
                except Exception as product_error:
                    print(f"Error processing individual product: {product_error}")
            
            # Random delay to mimic human behavior and avoid rate limiting
            time.sleep(random.uniform(1, 3))
        
        except Exception as page_error:
            print(f"Error scraping page {page}: {page_error}")
    
    return all_products

def main():
    # Scrape products for different categories
    categories = {
        'jumia_televisions': url1,
        'jumia_cookers': url2
    }
    
    for table_name, url in categories.items():
        print(f"Scraping products for {table_name}...")
        
        # Scrape products
        products = scrape_product_details(url, headers)
        
        # Insert products into database
        inserted_count = insert_products_to_db(products, table_name)
        
        print(f"Finished scraping and inserting products for {table_name}")

if __name__ == '__main__':
    main()
