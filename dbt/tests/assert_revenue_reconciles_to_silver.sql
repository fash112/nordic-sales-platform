-- Revenue must reconcile between the quality gate and the fact table.
--
-- This is the test that catches the failure mode nobody else catches: a join in
-- fct_sales that fans out rows. Row-count tests miss it if the fan-out is
-- uniform; grain tests catch duplication but not silent loss. Comparing the
-- summed measure on both sides catches both directions.
--
-- Tolerance is 0.01 per country to absorb decimal rounding, not to paper over
-- discrepancies. Anything larger is a real defect.

with staged as (

    select
        country,
        sum(revenue_eur) as revenue_staged
    from {{ ref('stg_sales') }}
    group by 1

),

fact as (

    select
        country,
        sum(revenue_eur) as revenue_fact
    from {{ ref('fct_sales') }}
    group by 1

),

compared as (

    select
        s.country,
        s.revenue_staged,
        f.revenue_fact,
        abs(coalesce(s.revenue_staged, 0) - coalesce(f.revenue_fact, 0)) as delta
    from staged s
    full outer join fact f on s.country = f.country

)

select *
from compared
where delta > 0.01
