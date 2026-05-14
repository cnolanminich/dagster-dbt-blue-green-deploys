{#
  Roll a green table back to its pre-swap state.

  Snowflake: real Time Travel — CREATE OR REPLACE ... CLONE AT (OFFSET => -N).
  DuckDB:    restore from the {table}__snapshot table that swap_blue_to_green
             writes on every promotion. Same orchestration shape, different
             primitives.

  Invoked as `dbt run-operation revert_via_time_travel --args '{table_name: mart_orders}'`.
#}
{% macro revert_via_time_travel(table_name, seconds_ago=300) %}
  {{ return(adapter.dispatch('revert_via_time_travel', 'bluegreen_analytics')(table_name, seconds_ago)) }}
{% endmacro %}


{% macro default__revert_via_time_travel(table_name, seconds_ago) %}
  {% do exceptions.raise_compiler_error(
        "revert_via_time_travel is not implemented for adapter " ~ adapter.type()
  ) %}
{% endmacro %}


{% macro snowflake__revert_via_time_travel(table_name, seconds_ago) %}
  {% if execute %}
    {% set green = var('green_schema') %}
    {% set db = target.database %}

    {% do log("Snowflake time-travel revert: " ~ db ~ "." ~ green ~ "." ~ table_name
              ~ " <- " ~ seconds_ago ~ "s ago", info=True) %}

    {% do run_query(
          "create or replace table " ~ db ~ "." ~ green ~ "." ~ table_name
          ~ " clone " ~ db ~ "." ~ green ~ "." ~ table_name
          ~ " at (offset => -" ~ seconds_ago ~ ")"
    ) %}
  {% endif %}
{% endmacro %}


{% macro duckdb__revert_via_time_travel(table_name, seconds_ago) %}
  {% if execute %}
    {% set green = var('green_schema') %}
    {% set snapshot = table_name ~ '__snapshot' %}

    {% set snapshot_exists_sql %}
      select count(*)
      from information_schema.tables
      where table_schema = '{{ green }}' and table_name = '{{ snapshot }}'
    {% endset %}
    {% set result = run_query(snapshot_exists_sql) %}

    {% if result.rows[0][0] == 0 %}
      {% do exceptions.raise_compiler_error(
            "No snapshot found for " ~ green ~ "." ~ table_name
            ~ " — nothing to revert to."
      ) %}
    {% endif %}

    {% do log("DuckDB revert " ~ green ~ "." ~ table_name
              ~ " <- " ~ green ~ "." ~ snapshot, info=True) %}

    {% do run_query(
          "create or replace table " ~ green ~ "." ~ table_name
          ~ " as select * from " ~ green ~ "." ~ snapshot
    ) %}
  {% endif %}
{% endmacro %}
