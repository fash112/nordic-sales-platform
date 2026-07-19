{{ config(materialized='view') }}

-- Staging sits directly on the Silver output produced by PySpark.
--
-- The only thing that happens here is the quality gate: rows Silver marked as
-- failing are excluded from everything downstream. They are NOT deleted -- they
-- remain in Silver, countable, with their reasons attached. This model is the
-- single place where the exclusion happens, so there is exactly one answer to
-- "why is this row missing from the report".

with silver as (

    select * from {{ source('silver', 'sales') }}

),

gated as (

    select
        country,
        order_id,
        order_date,
        customer_no,
        customer_name,
        sku,
        product_name,
        category,
        quantity,
        unit_price_local,
        currency,
        coalesce(channel, 'unknown') as channel,
        revenue_local,
        revenue_eur,
        is_return,
        ingested_at

    from silver
    where dq_status = 'pass'

)

select * from gated
