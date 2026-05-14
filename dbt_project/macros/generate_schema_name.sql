{#
  Override dbt's default schema generation so the +schema: <name> config
  produces a literal schema name (not target.schema_<name>). The blue/green
  flow depends on the schemas being exactly "blue", "green", and "raw".
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
