{% snapshot customers_snapshot %}
{{ config(unique_key='customer_id', strategy='check', check_cols=['segment', 'country']) }}
select customer_id, customer_name, country, segment from {{ ref('stg_customers') }}
{% endsnapshot %}
