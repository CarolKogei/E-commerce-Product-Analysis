-- Create table for televisions
CREATE TABLE IF NOT EXISTS jumia_televisions (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(255) UNIQUE,
    name TEXT,
    discounted_price TEXT,
    previous_price TEXT,
    discount_percentage TEXT,
    brand TEXT,
    rating TEXT,
    reviews_count TEXT,
    product_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create table for cookers
CREATE TABLE IF NOT EXISTS jumia_cookers (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(255) UNIQUE,
    name TEXT,
    discounted_price TEXT,
    previous_price TEXT,
    discount_percentage TEXT,
    brand TEXT,
    rating TEXT,
    reviews_count TEXT,
    product_url TEXT,
    source TEXT
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
