{# Money as the marts store it: rounded to cents, a fixed decimal type (used by contracts). #}
{% macro money(expr) -%}
    cast(round({{ expr }}, 2) as decimal(18, 2))
{%- endmacro %}
