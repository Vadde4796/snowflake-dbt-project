with customers as (
    select * from {{ ref('stg_customers') }}
),

order_stats as (
    select
        customer_id,
        count(order_id)                     as total_orders,
        round(sum(order_total),  2)         as lifetime_revenue,
        round(sum(order_profit), 2)         as lifetime_profit,
        round(avg(order_total),  2)         as avg_order_value,
        min(order_date)                     as first_order_date,
        max(order_date)                     as last_order_date
    from {{ ref('int_orders_enriched') }}
    where status != 'cancelled'
    group by customer_id
)

select
    c.customer_id,
    c.full_name,
    c.email,
    c.signup_date,
    c.country,
    coalesce(os.total_orders,     0)        as total_orders,
    coalesce(os.lifetime_revenue, 0)        as lifetime_revenue,
    coalesce(os.lifetime_profit,  0)        as lifetime_profit,
    coalesce(os.avg_order_value,  0)        as avg_order_value,
    os.first_order_date,
    os.last_order_date,
    datediff('day', os.first_order_date, os.last_order_date) as customer_lifespan_days
from customers c
left join order_stats os on c.customer_id = os.customer_id
