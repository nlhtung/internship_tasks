{{ config(materialized='table') }}

select customer_id,
       customer_name,
       country,
       count(*)              as total_orders,
       sum(total_usd)        as total_revenue_usd,
       round(avg(total_usd),2) as avg_order_usd
from {{ ref('int_order_metrics') }}
group by 1,2,3
order by total_revenue_usd desc