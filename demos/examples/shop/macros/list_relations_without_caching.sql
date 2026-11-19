{# The platform denies the system catalog to everyone but administrators, and dbt-trino's
   own `trino__list_relations_without_caching` joins the adapter's materialized-view metadata
   on every run (`#338`). The shop has no materialized views, so this project version drops the
   join and leaves everything else as the adapter has it (dbt-trino 1.10.6). #}
{% macro trino__list_relations_without_caching(relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      t.table_catalog as database,
      t.table_name as name,
      t.table_schema as schema,
      case when t.table_type = 'BASE TABLE' then 'table'
           when t.table_type = 'VIEW' then 'view'
           else t.table_type
      end as table_type
    from {{ relation.information_schema() }}.tables t
    where t.table_schema = '{{ relation.schema | lower }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}
