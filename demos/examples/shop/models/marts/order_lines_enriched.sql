{# Incremental append by day: each run adds only days after the newest one already loaded. #}
{{ config(materialized='incremental', incremental_strategy='append') }}
select
    l.order_line_id,
    l.order_date,
    l.customer_id,
    c.segment,
    l.product_id,
    p.category,
    l.quantity,
    {{ money('l.quantity * p.unit_price') }} as line_revenue
from {{ ref('stg_order_lines') }} l
join {{ ref('stg_products') }} p on p.product_id = l.product_id
join {{ ref('stg_customers') }} c on c.customer_id = l.customer_id
{% if is_incremental() %}
where l.order_date > (select max(order_date) from {{ this }})
{% endif %}
