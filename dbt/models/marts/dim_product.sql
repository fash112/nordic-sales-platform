{{ config(materialized='table') }}

-- Product dimension.
--
-- SKU is genuinely global across the four markets -- one catalogue, sold
-- everywhere -- so unlike customer_no it needs no country qualifier. That
-- asymmetry is the point: the same keying rule does not apply to every
-- dimension, and applying one out of habit is how you get either false merges
-- or needless duplication.

with source as (

    select
        sku,
        product_name,
        category,
        unit_price_local,
        currency,
        order_date
    from {{ ref('stg_sales') }}

),

named as (

    select
        sku,
        product_name,
        category,
        row_number() over (partition by sku order by order_date desc) as rn
    from source

),

price_band as (

    -- List price drifts with discounting, so the dimension carries the observed
    -- range rather than pretending a single canonical price exists.
    select
        sku,
        min(unit_price_local)    as min_observed_price,
        max(unit_price_local)    as max_observed_price,
        count(distinct currency) as currency_count
    from source
    group by 1

)

select
    md5(n.sku)             as product_key,
    n.sku,
    n.product_name,
    n.category,
    p.min_observed_price,
    p.max_observed_price,
    p.currency_count

from named n
join price_band p on n.sku = p.sku
where n.rn = 1
