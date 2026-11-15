{# Window functions: each customer's lifetime value, their rank, and the share of revenue the best customers make up. #}
with per_customer as (
    select
        l.customer_id,
        count(*) as order_lines,
        sum(l.quantity * p.unit_price) as lifetime_value,
        min(l.order_date) as first_order,
        max(l.order_date) as last_order
    from {{ ref('stg_order_lines') }} l
    join {{ ref('stg_products') }} p on p.product_id = l.product_id
    group by l.customer_id
)
select
    cast(c.customer_id as integer) as customer_id,
    cast(c.customer_name as varchar) as customer_name,
    cast(c.country as varchar) as country,
    cast(c.segment as varchar) as segment,
    cast(pc.order_lines as bigint) as order_lines,
    {{ money('pc.lifetime_value') }} as lifetime_value,
    cast(pc.first_order as date) as first_order,
    cast(pc.last_order as date) as last_order,
    cast(rank() over (order by pc.lifetime_value desc) as bigint) as value_rank,
    cast(sum(pc.lifetime_value) over (order by pc.lifetime_value desc rows between unbounded preceding and current row)
        / sum(pc.lifetime_value) over () as double) as cumulative_share
from {{ ref('stg_customers') }} c
join per_customer pc on pc.customer_id = c.customer_id
