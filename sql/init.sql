-- Check if the database exists, create if it doesn't
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'e_commerce_db'
   ) THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE e_commerce_db');
   END IF;
END
$$;

-- Connect to the database
\c e_commerce_db;

-- Create a 'laptops' table
CREATE TABLE "laptops" (
  "id" integer PRIMARY KEY,
  "name" varchar,
  "brand" varchar,
  "ram" varchar,
  "rom" varchar,
  "processor" varchar,
  "screen_size" varchar,
  "price" float,
  "reviews" float,
  "ratings" float,
  "links" varchar,
  "source" varchar,
  "created_at" timestamp
);

-- Create a 'fridges' table
CREATE TABLE "fridges" (
  "id" integer PRIMARY KEY,
  "name" varchar,
  "brand" varchar,
  "capacity_litres" varchar,
  "doors" int,
  "warranty_years" int,
  "price" float,
  "reviews" int,
  "ratings" float,
  "links" varchar,
  "source" varchar,
  "created_at" timestamp
);

-- Create a 'tvs' table
CREATE TABLE "tvs" (
  "id" integer PRIMARY KEY,
  "name" varchar,
  "product_id" varchar,
  "price" integer,
  "old_price" integer,
  "discount" float,
  "brand" varchar,
  "size" float,
  "type" varchar,
  "reviews" integer,
  "rating" float,
  "source" varchar,
  "url" varchar,
  "created_at" timestamp
);

-- Create a 'cookers' table
CREATE TABLE "cookers" (
  "id" integer PRIMARY KEY,
  "name" varchar,
  "product_id" varchar,
  "price" integer,
  "old_price" integer,
  "discount" float,
  "brand" varchar,
  "capacity" varchar,
  "type" varchar,
  "reviews" integer,
  "rating" float,
  "category" varchar,
  "source" varchar,
  "url" varchar,
  "created_at" timestamp
);

-- Create a 'phones' table
CREATE TABLE "phones" (
  "id" integer PRIMARY KEY,
  "description" varchar,
  "brand" varchar,
  "price" int,
  "old_price" int,
  "reviews" float,
  "number_of_reviews" int,
  "ram" int,
  "storage" int,
  "battery" int,
  "source" varchar,
  "url" varchar
);

-- Create a 'microwaves' table
CREATE TABLE "microwaves" (
  "id" integer PRIMARY KEY,
  "description" varchar,
  "brand" varchar,
  "price" float,
  "old_price" float,
  "number_of_reviews" int,
  "capacity" int,
  "reviews" int,
  "source" varchar,
  "url" varchar
);
