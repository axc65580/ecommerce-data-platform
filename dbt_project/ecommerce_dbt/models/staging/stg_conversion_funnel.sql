with source as (
    select * from conversion_funnel
),

renamed as (
    select
        event_type,
        event_count,
        unique_users,
        round(100.0 * event_count / sum(event_count) over (), 2) as pct_of_total
    from source
)

select * from renamed
