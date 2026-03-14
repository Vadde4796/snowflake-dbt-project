with source as (
    select * from {{ ref('raw_orders') }}
)

select
    order_id,
    customer_id,
    cast(order_date as date)                                    as order_date,
    status,
    case
        when shipping_date is null or trim(shipping_date) = '' then null
        else try_cast(shipping_date as date)
    end                                                         as shipping_date
from source
