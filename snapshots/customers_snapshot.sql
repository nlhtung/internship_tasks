{% snapshot customers_snapshot %}
{{ config(
   target_schema='snapshots',
   unique_key='customer_id',
   strategy='check',
   check_cols='all'
) }}
select
 customer_id,
 customer_name,
 signup_date,
 country
from {{ ref('stg_customers') }}
{% endsnapshot %}