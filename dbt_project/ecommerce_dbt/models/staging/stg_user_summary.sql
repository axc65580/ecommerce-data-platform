with source as (
    select * from user_summary
),

renamed as (
    select
        user_id,
        total_events,
        total_purchases,
        total_refunds,
        round(total_spent, 2) as total_spent,
        total_sessions,
        devices_used,
        case
            when total_spent >= 200 then 'high_value'
            when total_spent >= 100 then 'mid_value'
            else 'low_value'
        end as customer_segment
    from source
)

select * from renamed
