-- Fact table: one row per order with all metrics
select * from {{ ref('int_orders_enriched') }}