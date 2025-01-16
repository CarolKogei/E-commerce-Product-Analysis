import requests
from bs4 import BeautifulSoup
import time
import random
import psycopg2
from psycopg2 import sql
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jumia_scraping.log'),
        logging.StreamHandler()  # Also show logs in console
    ]
)

def save_to_postgresql(products, db_params, table_name):
    """
    Save products to PostgreSQL with enhanced error handling and logging
    """
    conn = None
    cursor = None
    try:
        # Log the start of database operation
        logging.info(f"Attempting to insert {len(products)} products into {table_name}")
        
        # Connect to database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Create a temporary table for new products
        temp_table = f"temp_{table_name}_{int(datetime.now().timestamp())}"
        create_temp_table = f"""
        CREATE TEMP TABLE {temp_table} AS 
        SELECT * FROM {table_name} WHERE 1=0
        """
        cursor.execute(create_temp_table)
        
        # Prepare the insert statement
        columns = products[0].keys()
        placeholders = ', '.join(['%s'] * len(products[0]))
        insert_query = f"""
        INSERT INTO {temp_table} ({', '.join(columns)})
        VALUES ({placeholders})
        """
        
        # Convert products to list of tuples for batch insert
        values = [[product[column] for column in columns] for product in products]
        
        # Batch insert into temporary table
        cursor.executemany(insert_query, values)
        
        # Insert only new records from temp table to main table
        merge_query = f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        SELECT {', '.join(columns)} FROM {temp_table} t
        WHERE NOT EXISTS (
            SELECT 1 FROM {table_name} m
            WHERE m.item_id = t.item_id
        )
        """
        cursor.execute(merge_query)
        
        # Get count of new records
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        
        # Commit the transaction
        conn.commit()
        
        logging.info(f"Successfully inserted products into {table_name}. Total records: {final_count}")
        return True
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Database error: {str(e)}")
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def scrape_product_details(url, headers):
    """
    Scrape product details with enhanced error handling and logging
    """
    products = []
    try:
        logging.info(f"Fetching products from: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise exception for bad status codes
        
        soup = BeautifulSoup(response.text, 'html.parser')
        product_cards = soup.find_all('article', {'class': 'prd'})
        
        logging.info(f"Found {len(product_cards)} product cards on page")
        
        for card in product_cards:
            try:
                product = {}
                
                # Extract name
                name_elem = card.find('h3', {'class': 'name'})
                product['name'] = name_elem.text.strip() if name_elem else None
                
                # Extract price
                price_elem = card.find('div', {'class': 'prc'})
                product['price'] = price_elem.text.strip() if price_elem else None
                
                # Extract brand
                product['brand'] = card.get('data-brand', None)
                
                # Extract product URL
                link = card.find('a', href=True)
                product['url'] = link['href'] if link else None
                
                # Generate unique item ID
                product['item_id'] = f"jumia_{hash(product['url'])}"
                
                # Only add product if we have all required fields
                if all(product.values()):
                    products.append(product)
                else:
                    logging.warning(f"Skipping product due to missing data: {product}")
                
            except Exception as e:
                logging.error(f"Error extracting product details: {str(e)}")
                continue
        
        logging.info(f"Successfully extracted {len(products)} products")
        return products
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {str(e)}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error during scraping: {str(e)}")
        return []

def main():
    # Database connection parameters
    db_params = {
        "host": "localhost",
        "database": "e-analytics_db",
        "user": "postgres",
        "password": "password"
    }
    
    # URLs to scrape
    urls = {
        'televisions': 'https://www.jumia.co.ke/televisions/',
        'cookers': 'https://www.jumia.co.ke/home-cooking-appliances-cookers/'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for category, url in urls.items():
        try:
            logging.info(f"Starting scraping for category: {category}")
            
            # Scrape products
            products = scrape_product_details(url, headers)
            
            if products:
                # Save to database
                table_name = f"jumia_{category}"
                success = save_to_postgresql(products, db_params, table_name)
                
                if success:
                    logging.info(f"Successfully completed scraping and storing {category}")
                else:
                    logging.error(f"Failed to store {category} data in database")
            else:
                logging.warning(f"No products found for {category}")
                
            # Add delay between categories
            time.sleep(random.uniform(2, 5))
            
        except Exception as e:
            logging.error(f"Error processing {category}: {str(e)}")

if __name__ == "__main__":
    main()
