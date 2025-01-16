import requests
from bs4 import BeautifulSoup
import time
import random
import psycopg2
from psycopg2 import sql
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s',
                    filename='jumia_scrape.log')

def check_response(urls, headers):
    """Check HTTP response status for given URLs."""
    for url in urls:
        try:
            response = requests.get(url, headers=headers)
            logging.info(f"URL: {response.url} - Status: {response.status_code}")

            if response.status_code != 200:
                logging.error(f"Failed to retrieve page. Status code: {response.status_code}")
                return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error: {e}")
            return False
    return True

def get_last_page_number(url, headers):
    """Determine the last page number for pagination."""
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find pagination elements and extract last page number
    pagination = soup.find('div', class_='pg-w')
    if pagination:
        pages = pagination.find_all('a', class_='pg')
        if pages:
            return int(pages[-1].text)
    return 1  # Default to 1 page if no pagination found

def scrape_product_details(url, headers):
    """Scrape product details from a single page."""
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    catalog_divs = soup.find_all('div', attrs={'data-catalog': 'true'})
    
    products = []
    for div in catalog_divs:
        try:
            # Extract product details
            product_name = div.find('h3', class_='name').text.strip()
            price_elem = div.find('div', class_='prc')
            price = price_elem.text.strip() if price_elem else 'N/A'
            
            # Add more robust error handling and default values
            product = {
                'name': product_name,
                'price': price,
                # Add other fields as needed
            }
            products.append(product)
        except Exception as e:
            logging.warning(f"Error extracting product details: {e}")
    
    return products

def save_to_postgresql(products, db_params, table_name):
    """Save products to PostgreSQL with enhanced error handling."""
    try:
        # Establish database connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Prepare SQL insert statement dynamically
        columns = ', '.join(products[0].keys())
        placeholders = ', '.join(['%s'] * len(products[0]))
        
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(columns),
            sql.SQL(placeholders)
        )
        
        # Prepare data for batch insert
        data_to_insert = [tuple(product.values()) for product in products]
        
        # Execute batch insert
        cursor.executemany(insert_query, data_to_insert)
        
        # Commit transaction
        conn.commit()
        
        logging.info(f"Successfully inserted {len(products)} products into {table_name}")
        return len(products)
    
    except (Exception, psycopg2.Error) as error:
        logging.error(f"Database insertion error: {error}")
        return 0
    
    finally:
        if conn:
            cursor.close()
            conn.close()

def scrape_and_save(url, headers, db_params, table_name):
    """Comprehensive scraping and saving function."""
    logging.info(f"Starting scrape for URL: {url}")
    
    # Get the last page number
    last_page = get_last_page_number(url, headers)
    logging.info(f"Total pages to scrape: {last_page}")
    
    # Initialize products list
    all_products = []
    
    # Scrape all pages
    for page_num in range(1, last_page + 1):
        page_url = f"{url}?page={page_num}#catalog-listing"
        logging.info(f"Scraping page {page_num}")
        
        # Scrape products from current page
        page_products = scrape_product_details(page_url, headers)
        all_products.extend(page_products)
        
        # Random delay between requests
        time.sleep(random.uniform(1, 3))
    
    # Validate and save scraped products
    if all_products:
        inserted_count = save_to_postgresql(all_products, db_params, table_name)
        
        # Log and validate insertion
        if inserted_count != len(all_products):
            logging.warning(f"Mismatch in scraping and insertion: Scraped {len(all_products)}, Inserted {inserted_count}")
        else:
            logging.info(f"Successfully scraped and inserted {inserted_count} products")
    else:
        logging.warning(f"No products found on {url}")

def main():
    # Define headers and URLs
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    url1 = "https://www.jumia.co.ke/televisions/#catalog-listing"
    url2 = "https://www.jumia.co.ke/home-cooking-appliances-cookers/#catalog-listing"
    
    # Database connection parameters
    db_params = {
        "host": "localhost",
        "database": "e-analytics_db",
        "user": "postgres",
        "password": "password"
    }
    
    # Scrape and save data
    scrape_and_save(url1, headers, db_params, "jumia_televisions")
    scrape_and_save(url2, headers, db_params, "jumia_cookers")

if __name__ == "__main__":
    main()
