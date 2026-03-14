with products as (
    select * from {{ ref('stg_products') }}
),

sales_stats as (
    select
        oi.product_id,
        sum(oi.quantity)                    as total_units_sold,
        round(sum(oi.line_total), 2)        as total_revenue,
        count(distinct oi.order_id)         as times_ordered
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
    where o.status != 'cancelled'
    group by oi.product_id
)

select
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.cost,
    p.margin,
    p.margin_pct,
    coalesce(ss.total_units_sold, 0)    as total_units_sold,
    coalesce(ss.total_revenue,    0)    as total_revenue,
    coalesce(ss.times_ordered,    0)    as times_ordered
from products p
left join sales_stats ss on p.product_id = ss.product_id
