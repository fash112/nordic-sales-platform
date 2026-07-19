{{ config(materialized='table') }}

-- Conformed date dimension, built from the observed order range rather than a
-- fixed calendar, so it never contains dates the facts cannot reach.
--
-- The surrogate key is the integer yyyymmdd. It sorts naturally, joins on an
-- int rather than a date, and stays readable in a raw query result without a
-- lookup -- which in practice matters more than surrogate-key purity.

with bounds as (

    select
        min(order_date) as min_d,
        max(order_date) as max_d
    from {{ ref('stg_sales') }}

),

spine as (

    select cast(unnest(generate_series(min_d, max_d, interval 1 day)) as date) as date_day
    from bounds

)

select
    cast(strftime(date_day, '%Y%m%d') as integer)  as date_key,
    date_day,
    extract(year    from date_day)                 as year,
    extract(quarter from date_day)                 as quarter,
    extract(month   from date_day)                 as month,
    strftime(date_day, '%B')                       as month_name,
    extract(day     from date_day)                 as day_of_month,
    extract(dayofweek from date_day)               as day_of_week,
    strftime(date_day, '%A')                       as day_name,
    case
        when extract(dayofweek from date_day) in (0, 6) then true
        else false
    end                                            as is_weekend,
    cast(strftime(date_day, '%Y%m') as integer)    as year_month

from spine
