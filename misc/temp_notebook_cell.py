def save_to_postgresql(products, db_params, table_name):
    conn = None
    cursor = None
    try:
        # Connect to PostgreSQL database
        print("Connecting to database...")
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                product_name TEXT,
                price TEXT,
                old_price TEXT,
                discount TEXT,
                rating TEXT,
                reviews_count TEXT,
                category TEXT,
                subcategory TEXT,
                is_official_store BOOLEAN,
                has_express_shipping BOOLEAN,
                item_id TEXT UNIQUE,
                item_brand TEXT,
                product_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table {table_name} created or verified")
        
        print(f"Connected successfully. Inserting {len(products)} products...")
        
        # Simple insert query
        insert_query = f"""
            INSERT INTO {table_name} 
            (product_name, price, old_price, discount, rating, reviews_count, 
             category, subcategory, is_official_store, has_express_shipping,
             item_id, item_brand, product_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (item_id) DO NOTHING
        """
        
        # Insert each product and commit after each successful insert
        for i, product in enumerate(products, 1):
            try:
                row = (
                    product["product_name"],
                    product["price"],
                    product["old_price"],
                    product["discount_percentage"],
                    product["rating"],
                    product["reviews_count"],
                    product.get("category", "N/A"),  # New fields with default values
                    product.get("subcategory", "N/A"),
                    product.get("is_official_store", False),
                    product.get("has_express_shipping", False),
                    product["item_id"],
                    product["item_brand"],
                    product["product_url"]
                )
                
                cursor.execute(insert_query, row)
                conn.commit()  # Commit after each insert
                
                if i % 100 == 0:
                    print(f"Inserted {i} products...")
                
            except Exception as e:
                print(f"Error inserting product {i}: {str(e)}")
                print(f"Problem product data: {json.dumps(product, indent=2)}")
                continue
        
        # Verify final count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        print(f"\nFinal count in {table_name}: {final_count}")
        
        # Show sample of inserted data
        print("\nVerifying inserted data...")
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        sample = cursor.fetchone()
        if sample:
            print("Sample record:", sample)
        else:
            print("No records found in database!")
        
    except Exception as e:
        print(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Database connection closed.")
