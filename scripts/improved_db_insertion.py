import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

def create_unique_index(conn, table_name):
    """
    Create a unique index on product_id to prevent duplicates
    """
    try:
        cur = conn.cursor()
        cur.execute(sql.SQL("""
            CREATE UNIQUE INDEX IF NOT EXISTS unique_product_id_idx 
            ON {} (product_id)
        """).format(sql.Identifier(table_name)))
        conn.commit()
        print(f"Unique index created on {table_name}")
    except Exception as e:
        print(f"Error creating unique index: {e}")
        conn.rollback()

def insert_products_to_db(products, table_name):
    """
    Improved database insertion with duplicate prevention and comprehensive error handling
    
    Args:
        products (list): List of product dictionaries
        table_name (str): Name of the database table
    
    Returns:
        int: Number of successfully inserted unique products
    """
    # Load environment variables
    load_dotenv('config/.env')
    
    try:
        # Establish database connection
        conn = psycopg2.connect(
            dbname='e-analytics_db',
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'password'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )
        
        # Create unique index to prevent duplicates
        create_unique_index(conn, table_name)
        
        cur = conn.cursor()
        
        # Prepare insert query with ON CONFLICT clause
        insert_query = sql.SQL("""
            INSERT INTO {} (
                name, 
                discounted_price, 
                previous_price, 
                discount_percentage, 
                product_id, 
                brand, 
                rating, 
                reviews_count, 
                product_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (product_id) DO NOTHING
        """).format(sql.Identifier(table_name))
        
        # Track unique products and insertion
        unique_products = set()
        inserted_count = 0
        skipped_count = 0
        error_count = 0
        
        # Batch insertion with detailed logging
        for product in products:
            # Create a unique key to identify products
            product_key = (
                product.get('id', ''),
                product.get('name', ''),
                product.get('discounted_price', '')
            )
            
            # Skip if already processed
            if product_key in unique_products:
                skipped_count += 1
                continue
            
            try:
                # Execute insert with error handling
                cur.execute(insert_query, (
                    product.get('name', 'N/A'),
                    product.get('discounted_price', 'N/A'),
                    product.get('previous_price', 'N/A'),
                    product.get('discount_%', 'N/A'),
                    product.get('id', 'N/A'),
                    product.get('brand', 'N/A'),
                    str(product.get('rating', 'N/A')),  # Convert to string to handle potential non-string types
                    str(product.get('reviews_count', 'N/A')),
                    product.get('link', 'N/A')
                ))
                
                # Track successful insertion
                unique_products.add(product_key)
                inserted_count += 1
            
            except Exception as inner_e:
                print(f"Error inserting product {product_key}: {inner_e}")
                error_count += 1
        
        # Commit the transaction
        conn.commit()
        
        # Detailed logging
        print(f"Database Insertion Summary for {table_name}:")
        print(f"Total Products Processed: {len(products)}")
        print(f"Successfully Inserted: {inserted_count}")
        print(f"Skipped (Duplicates): {skipped_count}")
        print(f"Errors: {error_count}")
        
        return inserted_count
    
    except Exception as e:
        print(f"Critical error in database insertion: {e}")
        return 0
    
    finally:
        # Ensure connection is closed
        if 'conn' in locals():
            cur.close()
            conn.close()

# Example usage
if __name__ == '__main__':
    # Sample products list for testing
    sample_products = [
        {
            'id': '1',
            'name': 'Sample TV',
            'discounted_price': '1000',
            'previous_price': '1200',
            'discount_%': '17%',
            'brand': 'Samsung',
            'rating': '4.5',
            'reviews_count': '100',
            'link': 'https://example.com'
        }
    ]
    
    # Test insertion
    insert_products_to_db(sample_products, 'jumia_televisions')
