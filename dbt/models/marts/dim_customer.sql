{{ config(materialized='table') }}

-- Customer dimension.
--
-- The natural key is (country, customer_no), NOT customer_no alone. Customer
-- numbers are unique only within a country's ERP, so C1234 in Sweden is a
-- different company from C1234 in Denmark. Keying on customer_no by itself
-- silently merges them -- an error that survives all the way to the board deck
-- because the row counts still look plausible.
--
-- SCD Type 1: names are overwritten and no history is retained. Stated
-- explicitly, because "which SCD type is this" is the first question anyone
-- asks of a dimension and "I didn't think about it" is the wrong answer.

with source as (

    select
        country,
        customer_no,
        customer_name,
        order_date
    from {{ ref('stg_sales') }}

),

latest_name as (

    -- A customer's name drifts across rows through source spelling variation.
    -- The most recent non-null spelling is treated as authoritative.
    select
        country,
        customer_no,
        customer_name,
        row_number() over (
            partition by country, customer_no
            order by order_date desc, customer_name
        ) as rn
    from source
    where customer_name is not null

),

activity as (

    select
        country,
        customer_no,
        min(order_date) as first_order_date,
        max(order_date) as latest_order_date,
        count(*)        as lifetime_order_count
    from source
    group by 1, 2

)

select
    md5(l.country || '|' || l.customer_no) as customer_key,
    l.country,
    l.customer_no,
    l.customer_name,
    a.first_order_date,
    a.latest_order_date,
    a.lifetime_order_count

from latest_name l
join activity a
  on  l.country     = a.country
  and l.customer_no = a.customer_no
where l.rn = 1
