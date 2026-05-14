{#
  Prep the "blue" staging schema with the prior state of "green" before any
  model builds. The actual promotion happens later in swap_blue_to_green;
  this step just gives blue a starting point so incremental models, views,
  and unselected tables all reflect the live green state.

  Snowflake clones the whole schema in one zero-copy metadata operation.
  DuckDB has no schema-clone primitive, so it iterates CTAS per table.

  Wired into dbt_project.yml as on-run-start.
#}
{% macro clone_green_to_blue() %}
  {{ return(adapter.dispatch('clone_green_to_blue', 'bluegreen_analytics')()) }}
{% endmacro %}


{% macro default__clone_green_to_blue() %}
  {% do exceptions.raise_compiler_error(
        "clone_green_to_blue is not implemented for adapter "
        ~ adapter.type()
  ) %}
{% endmacro %}


{% macro snowflake__clone_green_to_blue() %}
  {% if execute %}
    {% set green = var('green_schema') %}
    {% set blue = var('blue_schema') %}
    {% set db = target.database %}

    {% set green_exists_sql %}
      select count(*)
      from {{ db }}.information_schema.schemata
      where schema_name = upper('{{ green }}')
    {% endset %}
    {% set result = run_query(green_exists_sql) %}
    {% set green_exists = result.rows[0][0] > 0 %}

    {% if not green_exists %}
      {% do log("No " ~ green ~ " schema yet; first publish run.", info=True) %}
      {% do run_query("create schema if not exists " ~ db ~ "." ~ blue) %}
    {% else %}
      {% do log("Snowflake zero-copy clone: schema "
                ~ db ~ "." ~ green ~ " -> " ~ db ~ "." ~ blue, info=True) %}
      {% do run_query(
            "create or replace schema " ~ db ~ "." ~ blue
            ~ " clone " ~ db ~ "." ~ green
      ) %}
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro duckdb__clone_green_to_blue() %}
  {% if execute %}
    {% set green = var('green_schema') %}
    {% set blue = var('blue_schema') %}

    {% do run_query("create schema if not exists " ~ green) %}
    {% do run_query("create schema if not exists " ~ blue) %}

    {% set tables_in_green %}
      select table_name
      from information_schema.tables
      where table_schema = '{{ green }}'
        and table_type in ('BASE TABLE', 'VIEW')
        and table_name not like '%__snapshot'
    {% endset %}

    {% set results = run_query(tables_in_green) %}
    {% set rows = results.rows if results is not none else [] %}

    {% if rows | length == 0 %}
      {% do log("No tables yet in " ~ green ~ "; first publish run.", info=True) %}
    {% else %}
      {% for row in rows %}
        {% set tname = row[0] %}
        {% do log("DuckDB clone: " ~ green ~ "." ~ tname ~ " -> " ~ blue ~ "." ~ tname, info=True) %}
        {% set ctas %}
          create or replace table {{ blue }}.{{ tname }} as
          select * from {{ green }}.{{ tname }}
        {% endset %}
        {% do run_query(ctas) %}
      {% endfor %}
    {% endif %}
  {% endif %}
{% endmacro %}
