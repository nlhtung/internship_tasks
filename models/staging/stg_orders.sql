{{config(materialized='view')}}

select order_id,
       customer_id,
       order_date::date as order_date,
       total_amount,
       currency
from {{source('raw','orders')}}