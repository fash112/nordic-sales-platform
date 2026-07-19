{{ config(materialized='table') }}

-- Sales fact at ORDER LINE grain: one row per (country, order_id).
--
-- Grain is declared here and enforced by a uniqueness test in schema.yml.
-- Undeclared grain is the most common cause of double-counted revenue, and the
-- test is what stops the grain from quietly changing when someone edits an
-- upstream join.
--
-- Returns are kept as negative quantities and negative revenue rather than
-- being filtered or moved to a separate table. Any measure summing revenue is
-- therefore net of returns by default, which is the number the business
-- actually wants. Gross revenue is available by filtering is_return = false.

with sales as (

    select * from {{ ref('stg_sales') }}

)

select
    -- degenerate dimension: the order id stays on the fact, since there is no
    -- meaningful order dimension to build from a single-line-per-order source
    s.order_id,
    s.country,

    -- foreign keys
    cast(strftime(s.order_date, '%Y%m%d') as integer)  as date_key,
    md5(s.country || '|' || s.customer_no)             as customer_key,
    md5(s.sku)                                         as product_key,

    -- degenerate attributes
    s.channel,
    s.currency,

    -- measures
    s.quantity,
    s.unit_price_local,
    s.revenue_local,
    s.revenue_eur,
    s.is_return,

    s.order_date,
    s.ingested_at

from sales s
