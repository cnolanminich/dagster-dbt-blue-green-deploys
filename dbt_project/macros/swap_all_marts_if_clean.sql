{#
  on-run-end hook for the blue/green flow.

  Promotes every mart from blue to green ONLY if every node in the run
  finished cleanly (models built, tests passed, no skips, no errors). If
  anything went wrong, blue is left holding the bad data and green is
  untouched — re-run dbt after fixing the issue and the next clean run
  will promote.

  Why on-run-end instead of per-model post-hook: post-hook fires when the
  model SQL finishes but BEFORE dbt runs the tests attached to that model.
  An on-run-end hook is the only place where every test for every model has
  already executed and the results are available.

  dbt invokes on-run-end whenever execute_nodes() returns — failed models /
  failed tests do NOT skip it (failures live in the results array). It is
  skipped only on catastrophic errors (parse failure, on-run-start raised,
  process killed), in which case nothing was published either, so green
  stays untouched.
#}
{% macro swap_all_marts_if_clean() %}
  {% if execute %}
    {% if results is not defined or results | length == 0 %}
      {% do log("No node results available — nothing to swap.", info=True) %}
      {{ return("") }}
    {% endif %}

    {# Demo escape hatch: BLUEGREEN_SIMULATE_SWAP_FAILURE=true forces the swap
       to be skipped even on a clean build, so the failure path can be shown
       without editing models. #}
    {% if env_var('BLUEGREEN_SIMULATE_SWAP_FAILURE', 'false') | lower in ['true', '1', 'yes'] %}
      {% do log(
          "BLUEGREEN_SIMULATE_SWAP_FAILURE is set — forcing swap to be skipped. "
          ~ "Build succeeded but blue is not being promoted. Unset the env var "
          ~ "and re-run to publish.",
          info=True
      ) %}
      {{ return("") }}
    {% endif %}

    {% set failures = [] %}
    {% for r in results %}
      {% if r.status not in ['success', 'pass'] %}
        {% do failures.append(r.node.unique_id ~ " (" ~ r.status ~ ")") %}
      {% endif %}
    {% endfor %}

    {% if failures | length > 0 %}
      {% do log(
          "Build had " ~ failures | length ~ " failure(s); skipping swap. "
          ~ "Blue holds the un-promoted data. Failed nodes: "
          ~ failures | join(", "),
          info=True
      ) %}
      {{ return("") }}
    {% endif %}

    {% set marts_swapped = [] %}
    {% for r in results %}
      {% if r.node.resource_type == 'model'
            and 'marts' in r.node.fqn %}
        {% do swap_blue_to_green(r.node) %}
        {% do marts_swapped.append(r.node.name) %}
      {% endif %}
    {% endfor %}

    {% do log(
        "Swap complete; promoted " ~ marts_swapped | length ~ " mart(s) to green: "
        ~ marts_swapped | join(", "),
        info=True
    ) %}
  {% endif %}
{% endmacro %}
