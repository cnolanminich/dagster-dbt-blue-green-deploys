"""Blue/green deployment for non-dbt assets using native Dagster primitives.

Pattern:
  blue_orders  ──┐
                 ├── 4 asset checks on blue_orders ──┐
                 │   (row_count_positive,             │
                 │    no_null_order_id,               ├── green_orders
                 │    no_duplicate_order_id,          │   (eager + check-gated)
                 │    all_totals_positive)            │
                 └────────────────────────────────────┘

`green_orders` carries a Declarative Automation condition:

  eager()  AND  all_deps_match(all_checks_match(check_passed()))

* `eager()` — fires when blue_orders is freshly materialized and no run is
  in progress.
* `all_deps_match(all_checks_match(check_passed()))` — every check on every
  upstream dep currently has a passing latest evaluation.

When the user materializes blue_orders:
  1. blue_orders writes synthetic data into native_blue.orders.
  2. The 4 asset checks run in the same step (they're attached to blue_orders).
  3. After the run finishes, the daemon evaluates green_orders' condition.
     * All 4 checks passed → condition true → green_orders auto-materializes
       (copies blue → green).
     * Any check failed → condition false → green_orders stays put.

Setup
-----
* The `default_automation_condition_sensor` must be enabled in the Dagster UI
  under **Automation → Sensors**. Without that, the condition is never
  evaluated and green_orders never auto-fires.
* The first materialization of blue_orders won't trigger green_orders via the
  daemon — green is `missing()` at that point but `eager()` requires
  `newly_updated()`. Materialize blue_orders a second time (or kick off green
  manually) to bootstrap.

Failure demo
------------
Materialize blue_orders with `{"ops": {"blue_orders": {"config": {"corrupt":
true}}}}`. The asset injects a duplicate order_id so `no_duplicate_order_id`
fails. The condition on green_orders evaluates to false; green stays at its
prior good state.

The flag is a per-run Dagster Config (not an env var), so it can be flipped
at launch time from the UI launchpad without restarting the code location.
"""

from pathlib import Path

import dagster as dg
import duckdb


DUCKDB_PATH = (
    Path(__file__).resolve().parents[4]
    / "dbt_project"
    / ".local"
    / "native_demo.duckdb"
)


def _conn(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


class BlueOrdersConfig(dg.Config):
    """Per-run knobs for blue_orders.

    Set via the Dagster UI launchpad, or programmatically:
        --config-json '{"ops": {"blue_orders": {"config": {"corrupt": true}}}}'
    """

    corrupt: bool = False  # if true, inject a duplicate order_id to fail checks


# ---------------------------------------------------------------------------
# Blue (staging) asset
# ---------------------------------------------------------------------------

BLUE_ORDERS_KEY = dg.AssetKey(["native", "blue", "orders"])
GREEN_ORDERS_KEY = dg.AssetKey(["native", "green", "orders"])


@dg.asset(
    key=BLUE_ORDERS_KEY,
    description="Staging orders table. Lands data here before checks gate promotion.",
    group_name="native_blue_green",
    kinds={"duckdb"},
    tags={"layer": "blue", "deployment_strategy": "blue_green"},
)
def blue_orders(
    context: dg.AssetExecutionContext, config: BlueOrdersConfig
) -> dg.MaterializeResult:
    rows = [
        (1001, 1, 49.50, "2026-05-01 09:14:00"),
        (1002, 2, 128.00, "2026-05-01 11:02:00"),
        (1003, 1, 15.25, "2026-05-02 08:00:00"),
        (1004, 3, 210.75, "2026-05-02 14:30:00"),
        (1005, 4, 8.40, "2026-05-03 07:55:00"),
        (1006, 2, 52.10, "2026-05-03 16:45:00"),
        (1007, 5, 99.99, "2026-05-04 10:10:00"),
        (1008, 1, 310.00, "2026-05-04 19:22:00"),
        (1009, 3, 77.00, "2026-05-05 13:00:00"),
        (1010, 6, 42.00, "2026-05-05 18:08:00"),
    ]

    if config.corrupt:
        # Append a duplicate of order_id 1001 so no_duplicate_order_id fails.
        rows.append((1001, 1, 49.50, "2026-05-01 09:14:00"))
        context.log.warning(
            "config.corrupt=true — injected duplicate order_id 1001"
        )

    with _conn() as conn:
        conn.execute("create schema if not exists native_blue")
        conn.execute("drop table if exists native_blue.orders")
        conn.execute(
            "create table native_blue.orders ("
            "order_id bigint, customer_id bigint, total double, ordered_at timestamp)"
        )
        conn.executemany(
            "insert into native_blue.orders values (?, ?, ?, ?)", rows
        )
        n = conn.execute("select count(*) from native_blue.orders").fetchone()[0]

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.IntMetadataValue(n),
            "table": dg.TextMetadataValue("native_blue.orders"),
            "corrupt_mode": dg.BoolMetadataValue(config.corrupt),
        },
    )


# ---------------------------------------------------------------------------
# Asset checks 1-4 (all blocking)
# ---------------------------------------------------------------------------


@dg.asset_check(
    asset=BLUE_ORDERS_KEY,
    name="row_count_positive",
    blocking=True,
    description="Asserts native_blue.orders has at least one row.",
)
def check_row_count_positive() -> dg.AssetCheckResult:
    with _conn(read_only=True) as conn:
        n = conn.execute("select count(*) from native_blue.orders").fetchone()[0]
    return dg.AssetCheckResult(
        passed=n > 0,
        metadata={"row_count": dg.IntMetadataValue(n)},
    )


@dg.asset_check(
    asset=BLUE_ORDERS_KEY,
    name="no_null_order_id",
    blocking=True,
    description="Asserts no rows have a null order_id.",
)
def check_no_null_order_id() -> dg.AssetCheckResult:
    with _conn(read_only=True) as conn:
        n = conn.execute(
            "select count(*) from native_blue.orders where order_id is null"
        ).fetchone()[0]
    return dg.AssetCheckResult(
        passed=n == 0,
        metadata={"null_order_ids": dg.IntMetadataValue(n)},
    )


@dg.asset_check(
    asset=BLUE_ORDERS_KEY,
    name="no_duplicate_order_id",
    blocking=True,
    description="Asserts order_id is unique across the table.",
)
def check_no_duplicate_order_id() -> dg.AssetCheckResult:
    with _conn(read_only=True) as conn:
        n = conn.execute(
            "select count(*) from ("
            "  select order_id from native_blue.orders "
            "  group by order_id having count(*) > 1"
            ")"
        ).fetchone()[0]
    return dg.AssetCheckResult(
        passed=n == 0,
        metadata={"duplicate_order_ids": dg.IntMetadataValue(n)},
    )


@dg.asset_check(
    asset=BLUE_ORDERS_KEY,
    name="all_totals_positive",
    blocking=True,
    description="Asserts every order total is strictly positive.",
)
def check_all_totals_positive() -> dg.AssetCheckResult:
    with _conn(read_only=True) as conn:
        n = conn.execute(
            "select count(*) from native_blue.orders where total <= 0"
        ).fetchone()[0]
    return dg.AssetCheckResult(
        passed=n == 0,
        metadata={"non_positive_total_rows": dg.IntMetadataValue(n)},
    )


# ---------------------------------------------------------------------------
# Green (production) asset — declarative automation gated on the checks
# ---------------------------------------------------------------------------

green_orders_condition = (
    dg.AutomationCondition.eager()
    & dg.AutomationCondition.all_deps_match(
        dg.AutomationCondition.all_checks_match(
            dg.AutomationCondition.check_passed()
        )
    )
).with_label("eager_and_all_blue_checks_passed")


@dg.asset(
    key=GREEN_ORDERS_KEY,
    description=(
        "Production orders table. Auto-materializes only when blue_orders is "
        "freshly updated AND all 4 of its asset checks have passing latest "
        "evaluations."
    ),
    deps=[BLUE_ORDERS_KEY],
    automation_condition=green_orders_condition,
    group_name="native_blue_green",
    kinds={"duckdb"},
    tags={"layer": "green", "deployment_strategy": "blue_green"},
)
def green_orders(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with _conn() as conn:
        conn.execute("create schema if not exists native_green")
        conn.execute(
            "create or replace table native_green.orders as "
            "select * from native_blue.orders"
        )
        n = conn.execute("select count(*) from native_green.orders").fetchone()[0]

    context.log.info(
        f"Promoted {n} rows from native_blue.orders to native_green.orders"
    )
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.IntMetadataValue(n),
            "table": dg.TextMetadataValue("native_green.orders"),
        },
    )
