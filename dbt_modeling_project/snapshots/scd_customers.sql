

{% snapshot scd_customers %}

{{
   config(
       target_schema='dev_schema',
       unique_key='id',
       strategy='check',
       check_cols=['name']
   )
}}

select * from {{ source('jaffle', 'customers') }}

{% endsnapshot %}