import requests
from bs4 import BeautifulSoup
import time
import random
import psycopg2
from psycopg2 import sql
import logging
import re

def get_last_page_number(url, headers):
    """Get the last page number with improved error handling."""
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Method 1: Try to find the last page number from the pagination section
        pagination = soup.find('div', {'class': 'pg-w'})
        if pagination:
            # Find all page links
            page_links = pagination.find_all('a')
            if page_links:
                # Filter out non-numeric values and get the maximum
                page_numbers = []
                for link in page_links:
                    text = link.get_text().strip()
                    try:
                        num = int(text)
                        page_numbers.append(num)
                    except (ValueError, TypeError):
                        continue
                
                if page_numbers:
                    return max(page_numbers)
        
        # Method 2: Try to find page numbers from the URL pattern
        page_pattern = re.compile(r'page=(\d+)')
        matches = page_pattern.findall(response.text)
        if matches:
            page_numbers = [int(num) for num in matches]
            if page_numbers:
                return max(page_numbers)
        
        # Method 3: Count product items and divide by items per page
        products = soup.find_all('article', {'class': 'prd'})
        if products:
            items_per_page = len(products)
            total_items_text = soup.find('p', {'class': '-fs14'})
            if total_items_text:
                total_match = re.search(r'of\s+(\d+)', total_items_text.text)
                if total_match:
                    total_items = int(total_match.group(1))
                    return -(-total_items // items_per_page)  # Ceiling division
        
        # Default to checking next pages until no products found
        page = 1
        while True:
            next_url = f"{url}?page={page + 1}"
            response = requests.get(next_url, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            if not soup.find_all('article', {'class': 'prd'}):
                return page
            page += 1
            if page > 50:  # Safety limit
                return 50
            time.sleep(1)  # Be nice to the server
            
    except Exception as e:
        logging.error(f"Error in get_last_page_number: {str(e)}")
        return 1  # Return 1 as fallback
    
    return 1  # Default to 1 if all methods fail

def scrape_product_details(url, headers):
    """Scrape product details from a page."""
    products = []
    try:
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all product articles
        product_articles = soup.find_all('article', {'class': 'prd'})
        
        for article in product_articles:
            try:
                # Extract product info with error handling
                name = article.find('h3', {'class': 'name'})
                name = name.text.strip() if name else 'N/A'
                
                price = article.find('div', {'class': 'prc'})
                price = price.text.strip() if price else 'N/A'
                
                brand = article.get('data-brand', 'N/A')
                product_url = article.find('a', href=True)
                product_url = product_url['href'] if product_url else 'N/A'
                
                product = {
                    'name': name,
                    'price': price,
                    'brand': brand,
                    'url': product_url
                }
                products.append(product)
                
            except Exception as e:
                logging.warning(f"Error extracting product details: {str(e)}")
                continue
                
    except Exception as e:
        logging.error(f"Error scraping page {url}: {str(e)}")
    
    return products

def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename='scraping.log'
    )
    
    # Headers for requests
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # URLs to scrape
    url1 = "https://www.jumia.co.ke/televisions/"
    url2 = "https://www.jumia.co.ke/home-cooking-appliances-cookers/"
    
    # Test pagination for both URLs
    for url in [url1, url2]:
        try:
            logging.info(f"Testing pagination for {url}")
            last_page = get_last_page_number(url, headers)
            logging.info(f"Last page number for {url}: {last_page}")
            
            # Test scraping first page
            products = scrape_product_details(url, headers)
            logging.info(f"Found {len(products)} products on first page of {url}")
            
        except Exception as e:
            logging.error(f"Error processing {url}: {str(e)}")

if __name__ == "__main__":
    main()
