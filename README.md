# 🏪 Ecommerce Data Warehouse (Snowflake + dbt)

🔹 **Tech stack**: Snowflake + dbt  
🔹 **Data model**: Medallion architecture → Star schema  
🔹 **Purpose**: Analytics-ready sales data for Shop (fictitious e-commerce store)

## 📌 Project Overview

  

This project demonstrates a modern data warehouse on **Snowflake** using **dbt** for data modeling and transformation.

  

The Jaffle Shop dataset (a fictitious e-commerce store) to model sales transactions into a star schema with fact and dimension tables is used.

  
  

## 🏗️ Architecture

  

Medallion-style architecture is followed:

  

 - **Bronze (Raw / Source)** → Raw ingestion tables (`raw_customers`, `raw_orders`, etc.)
   
 -  **Silver (Staging + Intermediate)** → Cleaned staging models (`stg_*`) and business logic aggregations (`inter_*`)
     
 -  **Gold (Marts)** → Final dimensional models and fact tables (`dim_*`, `fct_*`, `*_summary`)



📊 Final Schema: Star Schema

 - Fact tables: fact_orders, fact_order_items
 
 - Dimension tables: dim_customers, dim_products, dim_stores, dim_dates



## 📂 Data Source

  

The dataset contains 6 raw tables loaded into Snowflake:

 - raw_customers
 - raw_orders    
 - raw_order_items    
 - raw_products   
 - raw_supplies   
 - raw_stores


## 🔄 Modeling Layers

### 1. Bronze (Raw Data)

Data is loaded directly into Snowflake from CSVs

### 2. Silver (Staging + Intermediate)

  

 - Staging models (stg_*) → Standardize column names, enforce data
   types, add load timestamps
   
   
  - Intermediate models (inter_*) → Business aggregations
    -  inter_orders_enriched: 1 row = 1 order with customer & store info
    -  inter_order_items_agg: 1 row = 1 customer order aggregate
    -  inter_product_sales_agg: 1 row = 1 product with sales stats
    

### 3. Gold (Fact & Dimension Tables)

   - Dimension Tables:

  - dim_customers
  -  dim_products
  -   dim_stores
  -  dim_dates


 - Fact Tables:

	 - fact_orders: One row = one customer order 
	 - fact_order_items: One row = one product sold in an order

  
Marts:

 - customer_orders_summary 
 - store_sales_summary
 - daily_sales_summary


## ⚡ How to Run

  

Clone this repo and install dependencies:

  ` cd project_folder `
  
` dbt deps `

` dbt run `

 

  
  

To view documentation:

 ` dbt docs generate `
 
 ` dbt docs serve `



Snowflake connection settings are defined in your profiles.yml.

  
  

## 📈 Business Use Cases

  

This warehouse supports analysis such as:

  

 - Total sales by store, product, or customer
   
     
   
 
 - Top-selling products and categories
      
        
     
 - Customer lifetime value (CLV)
         
           
 - Average order value (AOV)
