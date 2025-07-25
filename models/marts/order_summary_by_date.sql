{{config(materialized='table')}}

select order_date,
       count(*) as total_orders,
       sum(total_usd) as revenue_usd,
       round(sum(total_usd)::numeric / nullif(count(*),0),2) as avg_order_usd
from {{ref('int_order_metrics')}}
group by order_date
order by order_date