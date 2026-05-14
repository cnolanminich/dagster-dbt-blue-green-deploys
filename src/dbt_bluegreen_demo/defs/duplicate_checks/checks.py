"""Custom duplicate asset check + time-travel revert flow.

After the blue/green swap promotes a fresh `mart_orders` into the green schema,
this asset check queries for duplicate `order_id` values. If duplicates exist
the check fails. A sensor watches for that failure and launches the revert
job, which calls the `revert_via_time_travel` dbt macro — Snowflake uses real
Time Travel, DuckDB restores from the snapshot table that `swap_blue_to_green`
wrote during the promotion.

In demo mode (default) both the check and the revert run against the local
DuckDB file. Set BLUEGREEN_DEMO_MODE=false to run against Snowflake.
Set BLUEGREEN_SIMULATE_DUPLICATES=true to inject a duplicate row into
green.mart_orders so the failure path can be observed end-to-end.
"""

import os
import subprocess
from pathlib import Path

import dagster as dg
import duckdb
from dagster_snowflake import SnowflakeResource


DBT_PROJECT_DIR = Path(__file__).resolve().parents[4] / "dbt_project"
DUCKDB_PATH = DBT_PROJECT_DIR / ".local" / "demo.duckdb"

# dbt's blue-schema models become assets prefixed with `blue/`.
MART_ORDERS_KEY = dg.AssetKey(["blue", "mart_orders"])
REVERT_SECONDS = 600


def _is_demo_mode() -> bool:
    return os.environ.get("BLUEGREEN_DEMO_MODE", "true").lower() != "false"


def _is_simulated_failure() -> bool:
    return os.environ.get("BLUEGREEN_SIMULATE_DUPLICATES", "false").lower() == "true"


def _count_duckdb_duplicates() -> int:
    if not DUCKDB_PATH.exists():
        return 0
    with duckdb.connect(str(DUCKDB_PATH), read_only=False) as conn:
        if _is_simulated_failure():
            # Inject a duplicate row into green.mart_orders so the check fails.
            conn.execute(
                "insert into green.mart_orders "
                "select * from green.mart_orders limit 1"
            )
        return conn.execute(
            "select count(*) from ("
            "  select order_id from green.mart_orders "
            "  group by order_id having count(*) > 1"
            ")"
        ).fetchone()[0]


def _count_snowflake_duplicates(snowflake: SnowflakeResource) -> int:
    query = """
        select count(*)
        from (
            select order_id
            from green.mart_orders
            group by order_id
            having count(*) > 1
        )
    """
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query)
        return cur.fetchone()[0]


@dg.asset_check(
    asset=MART_ORDERS_KEY,
    name="no_duplicate_order_ids",
    description=(
        "Fails if green.mart_orders has duplicate order_id values. Failure "
        "triggers a time-travel revert via the duplicate_revert_sensor."
    ),
)
def no_duplicate_order_ids(
    context: dg.AssetCheckExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.AssetCheckResult:
    if _is_demo_mode():
        duplicate_count = _count_duckdb_duplicates()
        warehouse = "duckdb"
    else:
        duplicate_count = _count_snowflake_duplicates(snowflake)
        warehouse = "snowflake"

    context.log.info(
        f"duplicate check on green.mart_orders ({warehouse}): "
        f"{duplicate_count} duplicate order_id(s)"
    )

    return dg.AssetCheckResult(
        passed=duplicate_count == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "duplicate_order_ids": dg.IntMetadataValue(duplicate_count),
            "warehouse": dg.TextMetadataValue(warehouse),
            "checked_table": dg.TextMetadataValue("green.mart_orders"),
        },
    )


@dg.op(
    description=(
        "Calls the dbt revert_via_time_travel macro to restore green.mart_orders. "
        "Snowflake uses Time Travel; DuckDB restores from the snapshot table."
    ),
)
def revert_mart_orders(context: dg.OpExecutionContext) -> None:
    table = "mart_orders"
    target = "duckdb_demo" if _is_demo_mode() else "snowflake_prod"

    cmd = [
        "dbt", "run-operation", "revert_via_time_travel",
        "--args", f"{{table_name: {table}, seconds_ago: {REVERT_SECONDS}}}",
        "--target", target,
        "--project-dir", str(DBT_PROJECT_DIR),
        "--profiles-dir", str(DBT_PROJECT_DIR),
    ]
    context.log.info("running: " + " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise dg.Failure(description=f"dbt revert failed: {result.stderr}")


@dg.job(
    name="time_travel_revert_job",
    description="Rolls back green.mart_orders when the duplicate check fails.",
)
def time_travel_revert_job() -> None:
    revert_mart_orders()


@dg.sensor(
    name="duplicate_revert_sensor",
    job=time_travel_revert_job,
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=30,
    description=(
        "Launches the time-travel revert job whenever no_duplicate_order_ids "
        "fails on mart_orders."
    ),
)
def duplicate_revert_sensor(context: dg.SensorEvaluationContext):
    records = context.instance.event_log_storage.get_event_records(
        dg.EventRecordsFilter(
            event_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION,
            asset_key=MART_ORDERS_KEY,
            after_cursor=int(context.cursor) if context.cursor else None,
        ),
        limit=20,
        ascending=True,
    )

    if not records:
        return

    new_cursor = context.cursor
    for record in records:
        new_cursor = str(record.storage_id)
        evaluation = record.asset_check_evaluation
        if evaluation is None or evaluation.check_name != "no_duplicate_order_ids":
            continue
        if evaluation.passed:
            continue
        yield dg.RunRequest(
            run_key=f"revert-{record.storage_id}",
            tags={"triggered_by": "duplicate_revert_sensor"},
        )

    context.update_cursor(new_cursor)
