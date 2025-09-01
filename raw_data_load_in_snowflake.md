-- Set up the defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE jaffle;
USE SCHEMA raw_schema;


-- Create our input tables and import the data from S3


-- raw_customers table 
CREATE OR REPLACE TABLE raw_customers
                    (id string,
                     name string);
                    
COPY INTO raw_customers (id,
                        name)
                   from 's3://dbt-tutorial-public/long_term_dataset/raw_customers.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- raw_orders table 
CREATE OR REPLACE TABLE raw_orders
                    (id string,
                     customer string,
                     ordered_at datetime,
                     store_id string,
                     sub_total integer,
                     tax_paid integer,
                     order_total integer);
                    
COPY INTO raw_orders (id,
                      customer,
                      ordered_at,
                      store_id,
                      sub_total,
                      tax_paid,
                      order_total)
                   from 's3://dbt-tutorial-public/long_term_dataset/raw_orders.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- raw_order_items table 
CREATE OR REPLACE TABLE raw_order_items
                    (id string,
                     order_id string,
                     sku string);
                    
COPY INTO raw_order_items (id,
                          order_id,
                          sku)
                   from 's3://dbt-tutorial-public/long_term_dataset/raw_order_items.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- raw_products table 
CREATE OR REPLACE TABLE raw_products
                    (sku string,
                     name string,
                     type string,
                     price integer,
                     description text);
                    
COPY INTO raw_products (sku,
                        name,
                        type,
                        price,
                        description)
                   from 's3://dbt-tutorial-public/long_term_dataset/raw_products.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- raw_supplies table 
CREATE OR REPLACE TABLE raw_supplies
                    (id string,
                     name string,
                     cost integer,
                     perishable boolean,
                     sku text);
                    
COPY INTO raw_supplies (id,
                        name,
                        cost,
                        perishable,
                        sku)
                   from 's3://dbt-tutorial-public/long_term_dataset/raw_supplies.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');


-- raw_stores table 
CREATE OR REPLACE TABLE raw_stores
                    (id string,
                     name string,
                     opened_at  datetime,
                     tax_rate float);
                    
COPY INTO raw_stores (id,
                      name,
                      opened_at,
                       tax_rate)
                   from 's3://dbt-tutorial-public/long_term_dataset/raw_stores.csv'
                    FILE_FORMAT = (type = 'CSV' skip_header = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"');
