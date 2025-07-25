{{config(materialized='view')}}

select customer_id,
       customer_name,
       signup_date::timestamp as signup_date,
       country
from {{source('raw','customers')}}