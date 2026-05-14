{#
  Promote a freshly-built blue table to green. Snowflake uses ALTER TABLE
  ... SWAP WITH (atomic). DuckDB takes a snapshot of the current green
  table, then CTAS's blue into green's place — the snapshot is what the
  revert macro reads from.

  Called as a per-model post-hook on every mart.
#}
{% macro swap_blue_to_green(model_relation) %}
  {{ return(adapter.dispatch('swap_blue_to_green', 'bluegreen_analytics')(model_relation)) }}
{% endmacro %}


{% macro default__swap_blue_to_green(model_relation) %}
  {% do exceptions.raise_compiler_error(
        "swap_blue_to_green is not implemented for adapter " ~ adapter.type()
  ) %}
{% endmacro %}


{% macro snowflake__swap_blue_to_green(model_relation) %}
  {% if execute %}
    {% set green = var('green_schema') %}
    {% set blue = var('blue_schema') %}
    {% set db = target.database %}
    {% set tname = model_relation.identifier %}

    {% set green_exists_sql %}
      select count(*)
      from {{ db }}.information_schema.tables
      where table_schema = upper('{{ green }}') and table_name = upper('{{ tname }}')
    {% endset %}
    {% set result = run_query(green_exists_sql) %}
    {% set green_exists = result.rows[0][0] > 0 %}

    {% if green_exists %}
      {% do log("Snowflake SWAP " ~ blue ~ "." ~ tname ~ " <-> " ~ green ~ "." ~ tname, info=True) %}
      {% do run_query(
            "alter table " ~ db ~ "." ~ blue ~ "." ~ tname
            ~ " swap with " ~ db ~ "." ~ green ~ "." ~ tname
      ) %}
    {% else %}
      {% do log("First publish of " ~ tname ~ "; promoting blue -> green", info=True) %}
      {% do run_query(
            "create or replace table " ~ db ~ "." ~ green ~ "." ~ tname
            ~ " clone " ~ db ~ "." ~ blue ~ "." ~ tname
      ) %}
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro duckdb__swap_blue_to_green(model_relation) %}
  {% if execute %}
    {% set green = var('green_schema') %}
    {% set blue = var('blue_schema') %}
    {% set tname = model_relation.identifier %}
    {% set snapshot = tname ~ '__snapshot' %}

    {% do run_query("create schema if not exists " ~ green) %}

    {% set green_exists_sql %}
      select count(*)
      from information_schema.tables
      where table_schema = '{{ green }}' and table_name = '{{ tname }}'
    {% endset %}
    {% set result = run_query(green_exists_sql) %}
    {% set green_exists = result.rows[0][0] > 0 %}

    {% if green_exists %}
      {% do log("DuckDB snapshot " ~ green ~ "." ~ tname ~ " -> " ~ green ~ "." ~ snapshot, info=True) %}
      {% do run_query(
            "create or replace table " ~ green ~ "." ~ snapshot
            ~ " as select * from " ~ green ~ "." ~ tname
      ) %}
    {% endif %}

    {% do log("DuckDB promote " ~ blue ~ "." ~ tname ~ " -> " ~ green ~ "." ~ tname, info=True) %}
    {% do run_query(
          "create or replace table " ~ green ~ "." ~ tname
          ~ " as select * from " ~ blue ~ "." ~ tname
    ) %}
  {% endif %}
{% endmacro %}
