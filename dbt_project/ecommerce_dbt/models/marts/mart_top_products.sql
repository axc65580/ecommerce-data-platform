with revenue as (
    select * from {{ ref('stg_revenue_by_product') }}
)

select
    product_name,
    category,
    price_bucket,
    total_revenue,
    total_orders,
    avg_order_value,
    unique_buyers,
    round(100.0 * total_revenue / sum(total_revenue) over (), 2) as revenue_share_pct
from revenue
order by total_revenue desc
