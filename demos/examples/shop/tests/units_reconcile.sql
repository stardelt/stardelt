-- The units in the marts must equal the units in the lines they were built from. Both sides
-- are marts models, so the test is selected only in the `tag:marts` batch: in `tag:load` a
-- fresh catalog has no `gold.shop_dbt` yet, and eager indirect selection would run it there.
select s.units as line_units, g.units as mart_units
from (select sum(quantity) as units from {{ ref('order_lines_enriched') }}) s
cross join (select sum(units) as units from {{ ref('daily_revenue') }}) g
where s.units <> g.units
