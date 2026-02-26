with source as (
    select * from revenue_by_product
),

renamed as (
    select
        product_id,
        product_name,
        category,
        price_bucket,
        total_revenue,
        total_orders,
        round(avg_order_value, 2) as avg_order_value,
        unique_buyers
    from source
)

select * from renamed
