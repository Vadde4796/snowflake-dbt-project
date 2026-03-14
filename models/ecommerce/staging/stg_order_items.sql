with source as (
    select * from {{ ref('raw_order_items') }}
)

select
    order_item_id,
    order_id,
    product_id,
    cast(quantity   as int)                                         as quantity,
    cast(unit_price as float)                                       as unit_price,
    round(cast(quantity as int) * cast(unit_price as float), 2)    as line_total
from source
