import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime
import re
import time
import random

def clean_price(price_str):
    if not price_str:
        return None
    return float(re.sub(r'[^\d.]', '', price_str))

def extract_product_info(product):
    try:
        # Find the core link element that contains all data attributes
        core_link = product.find('a', class_='core')
        if not core_link:
            return None
            
        # Extract item_id, brand, and categories using data-ga4 attributes
        item_id = core_link.get('data-ga4-item_id', "N/A")
        item_brand = core_link.get('data-ga4-item_brand', "N/A")
        item_name = core_link.get('data-ga4-item_name', "N/A")
        
        # Categories
        category = core_link.get('data-ga4-item_category', "Electronics")
        subcategory = core_link.get('data-ga4-item_category2', "Television & Video")
        subcategory2 = core_link.get('data-ga4-item_category3', "Televisions")
        subcategory3 = core_link.get('data-ga4-item_category4', "")
        
        # Extract product URL
        product_url = core_link['href']
        
        # Price information
        price_container = product.find('div', class_='prc')
        current_price = clean_price(price_container.text.strip()) if price_container else None
        
        old_price_container = product.find('div', class_='old')
        old_price = clean_price(old_price_container.text.strip()) if old_price_container else None
        
        # Discount with updated class
        discount_container = product.find('div', class_='bdg _dsct _sm')
        discount = discount_container.text.strip() if discount_container else None
        if discount:
            discount = int(discount.replace('%', '').replace('-', ''))
        
        # Extract ratings and reviews
        rating = product.find('div', class_='rev')
        if rating:
            stars = rating.find('div', class_='stars _s').get_text(strip=True) if rating.find('div', class_='stars _s') else "N/A"
            reviews_count = rating.get_text(strip=True).split('(')[-1].strip(')') if '(' in rating.get_text() else "N/A"
        else:
            stars = "N/A"
            reviews_count = "N/A"
        
        # Additional features
        express_badge = product.find('div', class_='bdg _express')
        has_express_shipping = bool(express_badge)
        
        official_store_badge = product.find('div', class_='bdg _mall')
        is_official_store = bool(official_store_badge)
        
        return {
            'name': item_name,
            'item_id': item_id,
            'brand': item_brand,
            'price': current_price,
            'old_price': old_price,
            'discount': discount,
            'stars_rating': stars,
            'reviews_count': reviews_count,
            'category': category,
            'subcategory': subcategory,
            'subcategory2': subcategory2,
            'subcategory3': subcategory3,
            'source': 'Jumia',
            'url': f'https://www.jumia.co.ke{product_url}',
            'has_express_shipping': has_express_shipping,
            'is_official_store': is_official_store,
            'scraping_timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"Error processing product: {str(e)}")
        return None

def scrape_jumia_tvs():
    base_url = 'https://www.jumia.co.ke/televisions/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    all_products = []
    page = 1
    
    while True:
        try:
            url = f"{base_url}?page={page}#catalog-listing"
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            products = soup.find_all('article', class_='prd _fb col c-prd')
            
            if not products:
                break
                
            for product in products:
                product_info = extract_product_info(product)
                if product_info:
                    all_products.append(product_info)
            
            print(f"Scraped page {page}, found {len(products)} products")
            page += 1
            
            # Add delay to be respectful to the server
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"Error scraping page {page}: {str(e)}")
            break
    
    return all_products

def scrape_jumia_cookers():
    base_url = 'https://www.jumia.co.ke/home-cooking-appliances-cookers/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    all_products = []
    page = 1
    
    while True:
        try:
            url = f"{base_url}?page={page}#catalog-listing"
            response = requests.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            products = soup.find_all('article', class_='prd _fb col c-prd')
            
            if not products:
                break
                
            for product in products:
                product_info = extract_product_info(product)
                if product_info:
                    # Update source and category for cookers
                    product_info['source'] = 'Jumia Cookers'
                    product_info['category'] = 'Home Appliances'
                    product_info['subcategory'] = 'Cooking Appliances'
                    all_products.append(product_info)
            
            print(f"Scraped cookers page {page}, found {len(products)} products")
            page += 1
            
            # Add delay to be respectful to the server
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"Error scraping cookers page {page}: {str(e)}")
            break
    
    return all_products

def main():
    # Create data directories if they don't exist
    os.makedirs('data/scraped', exist_ok=True)
    
    # Scrape TVs
    print("Starting TV scraping process...")
    tv_products = scrape_jumia_tvs()
    
    # Save TV products to CSV
    if tv_products:
        df_tvs = pd.DataFrame(tv_products)
        tv_output_file = 'data/scraped/jumia_tvs.csv'
        df_tvs.to_csv(tv_output_file, index=False)
        print(f"Successfully scraped {len(tv_products)} TV products and saved to {tv_output_file}")
    else:
        print("No TV products were scraped")
    
    # Scrape Cookers
    print("\nStarting cookers scraping process...")
    cooker_products = scrape_jumia_cookers()
    
    # Save Cooker products to CSV
    if cooker_products:
        df_cookers = pd.DataFrame(cooker_products)
        cookers_output_file = 'data/scraped/jumia_cookers.csv'
        df_cookers.to_csv(cookers_output_file, index=False)
        print(f"Successfully scraped {len(cooker_products)} cooker products and saved to {cookers_output_file}")
    else:
        print("No cooker products were scraped")

if __name__ == "__main__":
    main()
