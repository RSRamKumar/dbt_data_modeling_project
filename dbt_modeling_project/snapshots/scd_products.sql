

{% snapshot scd_products %}

{{
   config(
       target_schema='dev_schema',
       unique_key='sku',
       strategy='check',
       check_cols= 'all'
   )
}}

select * from {{ source('jaffle', 'products') }}

{% endsnapshot %}


{# 
Tip:
1. Implement scd method
2. Execute 'dbt snapshot'  (seeds the snapshot table with initial rows.)
3. Make changes in raw table (table checked for changes)
4. Execute 'dbt snapshot' (dbt detects the change, closes the old row, adds a new one.)
5. Check for the updated result by reading the scd table. 

SELECT * FROM jaffle.raw_schema.raw_products WHERE sku='JAF-001';
UPDATE jaffle.raw_schema.raw_products SET  price=1021 WHERE  sku='JAF-001';
SELECT * FROM jaffle.dev_schema.scd_products WHERE sku='JAF-001' #}

{# Raw / Sources → Snapshots → Staging → Intermediate → Dimensions → Facts 
scd_layer acts between raw and staging layer and staging models are built on top of scd models
#}

{# 
It’s best practice to snapshot only “slowly changing” business dimensions where attribute changes must be tracked historically.
For transactional/event data (orders, order_items) → snapshots don’t make sense. #}