-- Grain enforcement: fct_sales must hold exactly one row per (country, order_id).
--
-- Written as a singular test rather than pulling in dbt_utils, so the project
-- has no package dependencies and runs anywhere dbt runs.
--
-- If this fails, something upstream is fanning out rows -- nearly always a join
-- that gained a duplicate on its right-hand side. Catching it here means the
-- revenue numbers never get a chance to be quietly wrong.

select
    country,
    order_id,
    count(*) as row_count

from {{ ref('fct_sales') }}
group by 1, 2
having count(*) > 1
