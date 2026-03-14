with source as (
    select * from {{ ref('raw_products') }}
)

select
    product_id,
    product_name,
    category,
    cast(price as float)                                                        as price,
    cast(cost  as float)                                                        as cost,
    round(cast(price as float) - cast(cost as float), 2)                       as margin,
    round(
        (cast(price as float) - cast(cost as float)) / cast(price as float) * 100,
        2
    )                                                                           as margin_pct
from source
