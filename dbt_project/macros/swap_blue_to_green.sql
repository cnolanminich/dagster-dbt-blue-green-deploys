{#
  Promote a freshly-built blue table to green.

  Snowflake: ALTER TABLE ... SWAP WITH — atomic metadata rename.
    Consumers querying green either see the old table or the new one,
    never a partial state. After the swap, blue holds what used to be in
    green (kept until the next on-run-start clone overwrites it).

  DuckDB:    no SWAP primitive — CTAS green from blue.

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

    {% do run_query("create schema if not exists " ~ green) %}

    {% do log("DuckDB promote " ~ blue ~ "." ~ tname ~ " -> " ~ green ~ "." ~ tname, info=True) %}
    {% do run_query(
          "create or replace table " ~ green ~ "." ~ tname
          ~ " as select * from " ~ blue ~ "." ~ tname
    ) %}
  {% endif %}
{% endmacro %}
