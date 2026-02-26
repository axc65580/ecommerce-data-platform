with users as (
    select * from {{ ref('stg_user_summary') }}
)

select
    customer_segment,
    count(user_id) as total_users,
    round(avg(total_spent), 2) as avg_spent,
    round(avg(total_purchases), 2) as avg_purchases,
    round(avg(total_sessions), 2) as avg_sessions
from users
group by customer_segment
order by avg_spent desc
