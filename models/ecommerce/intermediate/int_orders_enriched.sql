with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

order_items_with_cost as (
    select
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        p.cost,
        oi.line_total,
        round(oi.quantity * p.cost, 2)  as line_cost
    from order_items oi
    left join products p on oi.product_id = p.product_id
),

order_totals as (
    select
        order_id,
        round(sum(line_total), 2)           as order_total,
        round(sum(line_cost),  2)           as order_cost,
        sum(quantity)                        as total_items,
        count(distinct product_id)           as distinct_products
    from order_items_with_cost
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.shipping_date,
    o.status,
    ot.order_total,
    ot.order_cost,
    round(ot.order_total - ot.order_cost, 2)                as order_profit,
    ot.total_items,
    ot.distinct_products,
    datediff('day', o.order_date, o.shipping_date)          as days_to_ship
from orders o
left join order_totals ot on o.order_id = ot.order_id
