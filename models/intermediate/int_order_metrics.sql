{{config(materialized='table')}}

with orders as (select * from {{ref('stg_orders')}}),
     customers as (select * from {{ref('stg_customers')}})

select o.order_id,
       o.order_date,
       o.customer_id,
       c.customer_name,
       c.country,
       o.total_amount,
       o.currency,
       {{to_usd('o.total_amount','o.currency')}} as total_usd
from orders o left join customers c using(customer_id)