# dbt_bluegreen_demo

Two complementary deployment-safety patterns for a dbt project orchestrated by
Dagster, runnable either against **dbt Core + Snowflake** (production) or
**DuckDB** (zero-setup local demo). The same dbt repo also works unchanged
with **dbt Cloud** — see the [dbt Cloud section](#running-on-dbt-cloud) below.

The two patterns:

1. **Blue/green clone-then-swap** — keep two parallel schemas (`green` =
   live, `blue` = staging). Every run clones green into blue, builds dbt
   models into blue, then atomically swaps each completed table into green.
   Consumers reading green never see a half-built state.
2. **Asset check + time-travel revert** — after the swap, a custom Dagster
   asset check verifies an invariant on the green table (no duplicate
   `order_id`). If the check fails, a sensor launches a revert job that
   restores green to its previous state via Snowflake Time Travel (or a
   `__snapshot` table on DuckDB).

Use them together (this demo wires both) or independently. The
[when-to-prefer section](#when-to-prefer-one-over-the-other) is the part to
read carefully.

## Quickstart (no credentials needed)

```bash
uv sync
uv run dg dev
```

Open http://localhost:3000. The `BlueGreenDbtComponent` defaults to
`demo_mode: true`, which runs the real dbt project against a local DuckDB file
at `dbt_project/.local/demo.duckdb`. Seeds load automatically on first
materialization.

Materialize the dbt assets twice so you can observe both the first-publish
path and the clone-and-swap path. Then exercise the failure path:

```bash
BLUEGREEN_SIMULATE_DUPLICATES=true uv run dg dev
```

The asset check fails, `duplicate_revert_sensor` fires, `time_travel_revert_job`
runs the dbt `revert_via_time_travel` macro, and `green.mart_orders` is
restored from its snapshot.

## Running against real Snowflake

Set the Snowflake env vars and flip `demo_mode` in
[`src/dbt_bluegreen_demo/defs/dbt_blue_green/defs.yaml`](src/dbt_bluegreen_demo/defs/dbt_blue_green/defs.yaml):

```yaml
attributes:
  demo_mode: false
```

```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
export SNOWFLAKE_ROLE=...
export SNOWFLAKE_WAREHOUSE=...
export SNOWFLAKE_DATABASE=...
```

## Approach 1: blue/green clone-then-swap

The three macros all use dbt's `adapter.dispatch` so the SQL changes by
warehouse but the orchestration shape doesn't.

| Stage | Trigger | Snowflake | DuckDB |
| --- | --- | --- | --- |
| Clone green → blue | dbt `on-run-start` hook | `CREATE OR REPLACE SCHEMA blue CLONE green` (one zero-copy metadata op) | `CREATE OR REPLACE TABLE blue.x AS SELECT * FROM green.x` per table |
| Build into blue | dbt model materializations | normal `dbt build` into the `blue` schema | normal `dbt build` into the `blue` schema |
| Promote blue → green | dbt `on-run-end` hook (only if all models built and all tests passed) | `ALTER TABLE blue.x SWAP WITH green.x` per mart (atomic rename) | snapshot `green.x → green.x__snapshot`, then `CREATE OR REPLACE TABLE green.x AS SELECT * FROM blue.x` per mart |

### Why on-run-end and not per-model post-hook

dbt's `post-hook` fires after a model's SQL completes but **before** the
tests dbt scheduled on that model. Using post-hook for promotion means a bad
model is already swapped into green by the time its `unique` / `not_null`
tests fail — exactly the case the blue/green flow is supposed to prevent.

`on-run-end` is the first hook point where every model and every test has
finished, and the `results` array is populated with each node's status. The
[swap_all_marts_if_clean](dbt_project/macros/swap_all_marts_if_clean.sql)
macro walks `results`, aborts if any node has status other than `success` or
`pass`, and otherwise promotes every mart. Verified end-to-end against both
test failures and model build errors — in both cases the swap is skipped and
green retains its prior state.

dbt invokes `on-run-end` whenever `execute_nodes()` returns, **including
when models or tests failed** (failures live in the results array, not as
raised exceptions). It is skipped only on catastrophic errors — parse
failure, `on-run-start` raising, or the process being killed — in which case
nothing was published anyway, so green stays untouched either way.

The trade-off is **all-or-nothing batch promotion**: one failed test means
*no* mart promotes, even ones whose tests passed. The alternative — walking
the test results to figure out which models have all-passing tests and aren't
downstream of any failed node — is more complex and the partial-success case
is usually best handled by "rerun after fixing the broken model."

### What you get

- **Atomic cutover** on Snowflake — `SWAP WITH` is a metadata rename; consumers
  see only old-or-new, never partial.
- **Fail-forward isolation.** If `dbt build` fails halfway through, blue is
  discarded on the next run's clone. Green is untouched.
- **Cheap on Snowflake.** Zero-copy clones are metadata-only — even TB-scale
  tables clone in milliseconds.

### What you don't get

- **Cross-table consistency during the swap window.** The swap iterates marts
  inside the `on-run-end` hook, so each `ALTER TABLE SWAP WITH` (Snowflake)
  or `CREATE OR REPLACE` (DuckDB) is its own statement. There is a brief
  window where some marts have been promoted and others haven't — consumers
  reading multiple marts in that window can see mixed old/new. Closing this
  window fully requires view-aliasing (e.g., have consumers query
  `live.mart_orders` which is a view that points at either green_a or
  green_b, and flip the view in a single statement at the end).

## Approach 2: asset check + time-travel revert

A custom Dagster `@asset_check` on `blue/mart_orders` runs after the swap
promotes it into green. The check queries green for the invariant ("no
duplicate `order_id`"). If it fails:

1. The evaluation lands in the Dagster event log with `passed=False`.
2. `duplicate_revert_sensor` reads the event log, sees the failed evaluation,
   and emits a `RunRequest` for `time_travel_revert_job`.
3. The job runs `revert_mart_orders`, which (on dbt Core) shells out to
   `dbt run-operation revert_via_time_travel --args '{table_name: mart_orders, seconds_ago: 600}'`.
4. The macro restores `green.mart_orders` — from `AT (OFFSET => -600)` on
   Snowflake, or from the `green.mart_orders__snapshot` table that
   `swap_blue_to_green` wrote during the most recent promotion.

### What you get

- **Catches problems the warehouse can't.** dbt's built-in `unique` test
  catches the same case at build time, but only against blue. If a downstream
  writer corrupts green after the swap, dbt doesn't know. The Dagster check
  runs against the post-swap live table.
- **Decoupled detection and remediation.** The check records a fact. The
  sensor decides what to do with it. Swap the sensor for a PagerDuty
  integration, a Slack alert, or a `RetryRequested` and the check doesn't
  change.
- **Audit trail.** Every revert is a Dagster run with logs, run tags, and a
  cursor pointing at the asset check evaluation that triggered it.

### What you don't get

- **Atomicity with the swap.** The swap has already happened by the time the
  check runs. Consumers querying green between the swap and the revert see
  the bad data. If you want pre-swap validation, run dbt tests against blue
  before the swap macro fires — but you give up the check's main value, which
  is verifying the actually-live table.
- **Unbounded history.** Snowflake's default Time Travel retention is 1 day
  (configurable up to 90 with Enterprise). DuckDB's `__snapshot` table is
  only the *previous* state — two bad runs in a row and the snapshot is bad
  too. For longer rollback windows, materialize daily snapshots to a separate
  retention table.

## When to prefer one over the other

**Only need blue/green** when:

- Failures are loud and synchronous — `dbt build` raises, the test fails, the
  warehouse rejects the write. Bad data never reaches green; no revert needed.
- Your invariants are expressible as dbt tests and you trust them to run
  against blue before the swap.

**Also need asset check + revert** when:

- The invariant only makes sense against the live, post-swap green table.
  Duplicates can come from upstream EL writing into raw twice, from a backfill
  that overlaps a normal run, or from a join blow-up that dbt's `unique` test
  happened not to cover.
- The bad data isn't a build failure — the build succeeds, the test passes,
  and the data still violates an invariant you care about.
- You need an automatic, auditable remediation path, not "page someone and
  hope."

**Probably want both** when:

- Cost of downstream consumers seeing bad data is high (customer-facing
  dashboard, off-platform actions, reverse-ETL to another system).
- Your warehouse has cheap time-travel (Snowflake, BigQuery snapshot tables,
  Iceberg time-travel). Marginal cost of the revert path is small; the
  marginal value is large the one time it saves you.

**Probably want neither** when:

- The table is small enough that a full rebuild on detected error is faster
  than the revert flow.
- You don't have a codifiable invariant beyond "looks right."
- No downstream consumer whose harm you're trying to bound.

## Blue/green for non-dbt assets (native Dagster)

The dbt pattern above wraps the swap inside the dbt project. For pipelines
that aren't dbt — e.g. a Python asset writing to DuckDB / Snowflake / a
warehouse you don't model in dbt — the same shape works with native Dagster
primitives: an asset, asset checks attached to it, and a downstream asset
that uses Declarative Automation to gate on those checks.

See [`src/dbt_bluegreen_demo/defs/native_blue_green/assets.py`](src/dbt_bluegreen_demo/defs/native_blue_green/assets.py).

### The pattern

```
blue_orders  ──┐
                ├─ 4 asset checks on blue_orders ──┐
                │   row_count_positive             │
                │   no_null_order_id               ├─→ green_orders
                │   no_duplicate_order_id          │   (eager + check-gated)
                │   all_totals_positive            │
                └──────────────────────────────────┘
```

`green_orders` carries this `AutomationCondition`:

```python
green_orders_condition = (
    dg.AutomationCondition.eager()
    & dg.AutomationCondition.all_deps_match(
        dg.AutomationCondition.all_checks_match(
            dg.AutomationCondition.check_passed()
        )
    )
)
```

- **`eager()`** — base condition: fire when `blue_orders` is newly updated
  and no run is in progress.
- **`all_deps_match(all_checks_match(check_passed()))`** — for every upstream
  dep, every check on that dep currently has a passing latest evaluation.
  Combined with `eager()`, this means: "blue just updated, no run in flight,
  and every check on blue currently passes."

If any check fails, the condition is false; `green_orders` is not requested
and green stays at its prior state. After fixing the upstream issue and
re-materializing `blue_orders`, the next evaluation tick sees the checks
back to passing and promotes green automatically.

### Setup

The `default_automation_condition_sensor` must be enabled in the Dagster UI
under **Automation → Sensors**. Without that, the condition is never
evaluated and `green_orders` never auto-fires.

The first materialization of `blue_orders` won't trigger `green_orders` via
the daemon — `green_orders` is `missing()` at that point but `eager()`
requires `newly_updated()` (and the asset can't be newly_updated until it
has been materialized at least once). Either materialize `green_orders`
manually once to bootstrap, or replace `eager()` with `eager() |
(missing() & all_deps_match(...))` if you want bootstrap behavior baked in.

### Trying it

```bash
# Materialize blue + its 4 checks
uv run dagster asset materialize --select 'key:"native/blue/orders"' \
    -m dbt_bluegreen_demo.definitions

# In `dg dev` with the automation sensor enabled, green will fire automatically.
# For a one-shot demo without the daemon, materialize green manually:
uv run dagster asset materialize --select 'key:"native/green/orders"' \
    -m dbt_bluegreen_demo.definitions
```

To demo the failure path, pass `corrupt: true` as run config. The flag is a
per-run `dg.Config` so it can be flipped from the UI launchpad without
restarting the code location, or via CLI:

```bash
uv run dagster asset materialize \
    --select 'key:"native/blue/orders"' \
    --config-json '{"ops": {"native__blue__orders": {"config": {"corrupt": true}}}}' \
    -m dbt_bluegreen_demo.definitions
```

(The op name `native__blue__orders` is Dagster's auto-generated name from the
asset key path — each segment joined by `__`.)

The asset injects a duplicate `order_id` so `no_duplicate_order_id` fails
(verified end-to-end: check record status → `FAILED`). The condition on
`green_orders` evaluates to false; green stays put.

### Trade-offs vs the dbt path

| Aspect | dbt path | Native path |
| --- | --- | --- |
| Where the swap lives | dbt macros (`on-run-end` hook) | downstream asset materialization (`green_orders` writes the green table) |
| Failure gate | macro inspects `results`, skips swap on any non-pass | `AutomationCondition` evaluates `all_checks_match(check_passed())` |
| Atomicity (per table) | Snowflake `SWAP WITH` is atomic; DuckDB CTAS is not | depends on what `green_orders` does — could be `SWAP WITH`, `CREATE OR REPLACE`, or anything else |
| Granularity | one all-or-nothing batch at end of dbt run | per-asset; you can have multiple greens with independent conditions |
| Requires | dbt project + dbt invocation | Dagster + the automation sensor enabled |

The native path is more flexible when you have a mix of dbt and non-dbt
assets, or when greens have heterogeneous promotion rules. The dbt path is
simpler when the entire blue/green flow is contained in a dbt build.

## Running on dbt Cloud

The dbt project (macros, hooks, `dbt_project.yml`) is unchanged — dbt Cloud
runs your repo the same way dbt Core does. The `on-run-start` clone fires,
the per-model `post-hook` swap fires, `adapter.dispatch` resolves to the
Snowflake implementations. Only the Dagster pieces swap:
`DbtProjectComponent` → `DbtCloudComponent`, and the revert op stops
shelling out to local dbt.

### Approach 1 (clone-then-swap) on dbt Cloud

Drop in [`src/dbt_bluegreen_demo/defs/dbt_blue_green_cloud/defs.yaml.example`](src/dbt_bluegreen_demo/defs/dbt_blue_green_cloud/defs.yaml.example):

```yaml
type: dagster_dbt.DbtCloudComponent

attributes:
  workspace:
    account_id: "{{ env_var('DBT_CLOUD_ACCOUNT_ID') }}"
    project_id: "{{ env_var('DBT_CLOUD_PROJECT_ID') }}"
    environment_id: "{{ env_var('DBT_CLOUD_ENVIRONMENT_ID') }}"
    token: "{{ env_var('DBT_CLOUD_API_TOKEN') }}"
  cli_args: [build]
  op: { name: dbt_blue_green_cloud_build }
  translation:
    group_name: blue_green_marts
    kinds: [dbt, snowflake]
  defs_state:
    management_type: versioned_state_storage
    refresh_if_dev: true
```

Set the four `DBT_CLOUD_*` env vars, rename `defs.yaml.example` →
`defs.yaml`, and either delete the dbt Core variant or add
`translation.key_prefix: cloud` to avoid asset-key collisions.

**What's different:** credentials live in dbt Cloud; Dagster doesn't run dbt
as a subprocess (it triggers an ad-hoc job via the v2 API); the manifest is
state-backed for fast Dagster reloads.

**What's the same:** the blue/green flow itself (it's entirely in the dbt
project), the role permissions on green/blue, the schedule asset selections
(both components tag assets with `kind:dbt`).

### Approach 2 (check + revert) on dbt Cloud

The check and the sensor are pure Dagster — they don't change. The revert op
changes: there's no local dbt, so shelling out to `dbt run-operation` doesn't
work. The example file at
[`src/dbt_bluegreen_demo/defs/dbt_blue_green_cloud/checks.py.example`](src/dbt_bluegreen_demo/defs/dbt_blue_green_cloud/checks.py.example)
shows the Cloud-flavored variant. The revert op becomes:

```python
@dg.op
def revert_mart_orders(
    context: dg.OpExecutionContext,
    snowflake: SnowflakeResource,
) -> None:
    revert_sql = (
        "create or replace table green.mart_orders "
        "clone green.mart_orders at (offset => -600)"
    )
    with snowflake.get_connection() as conn:
        conn.cursor().execute(revert_sql)
```

That's the entire body of the dbt macro, issued directly through
`SnowflakeResource` instead of through `dbt run-operation`. Faster (no
warehouse spin-up, no job queue), simpler (one statement, one auth path),
and the audit trail is "Dagster issued this SQL" rather than "Dagster called
dbt Cloud, which ran a macro, which issued this SQL."

If your org's policy requires dbt Cloud to be the sole writer to the
warehouse, swap the direct Snowflake call for an ad-hoc dbt Cloud run via
the v2 API:

```python
import requests
@dg.op
def revert_mart_orders(context):
    requests.post(
        f"https://cloud.getdbt.com/api/v2/accounts/{ACCOUNT_ID}/jobs/{JOB_ID}/run/",
        headers={"Authorization": f"Token {TOKEN}"},
        json={"cause": "duplicate_revert_sensor",
              "steps_override": [
                  "dbt run-operation revert_via_time_travel "
                  "--args '{table_name: mart_orders, seconds_ago: 600}'"
              ]},
    ).raise_for_status()
```

Slower but preserves "dbt Cloud is the only thing that writes" if you need
it. For emergency rollback, direct SQL is usually the better default.

## Project layout

```
dbt_bluegreen_demo/
├── dbt_project/
│   ├── dbt_project.yml            # on-run-start clone + per-model post-hook swap
│   ├── profiles.yml               # duckdb_demo + snowflake_prod targets
│   ├── macros/
│   │   ├── clone_green_to_blue.sql       # adapter.dispatch: snowflake__/duckdb__
│   │   ├── swap_blue_to_green.sql        # adapter.dispatch: snowflake__/duckdb__
│   │   ├── swap_all_marts_if_clean.sql   # on-run-end orchestrator, walks results
│   │   ├── revert_via_time_travel.sql    # adapter.dispatch: snowflake__/duckdb__
│   │   └── generate_schema_name.sql      # +schema: blue → "blue" literally
│   ├── seeds/                            # raw_orders.csv, raw_customers.csv
│   └── models/
│       ├── staging/                      # stg_orders, stg_customers
│       └── marts/                        # mart_orders, mart_customers (promoted via on-run-end)
└── src/dbt_bluegreen_demo/
    ├── components/
    │   ├── blue_green_dbt_component.py   # DbtProjectComponent subclass + demo_mode
    │   └── scheduled_job_component.py
    └── defs/
        ├── dbt_blue_green/defs.yaml              # dbt Core instance (active)
        ├── dbt_blue_green_cloud/
        │   ├── defs.yaml.example                 # dbt Cloud variant of approach 1
        │   └── checks.py.example                 # dbt Cloud variant of approach 2
        ├── duplicate_checks/checks.py            # asset check + revert op + job + sensor
        ├── native_blue_green/assets.py           # non-dbt blue/green via AutomationCondition
        ├── resources/resources.py                # SnowflakeResource
        └── schedules/defs.yaml                   # 3 schedules
```

## Environment variables

| Var | Used by | Default |
| --- | --- | --- |
| `BLUEGREEN_DEMO_MODE` | `checks.py` to decide DuckDB vs Snowflake at check time | `true` |
| `BLUEGREEN_SIMULATE_DUPLICATES` | `checks.py` injects a duplicate row into green.mart_orders so the check fails (DuckDB only) | `false` |
| `BLUEGREEN_SIMULATE_SWAP_FAILURE` | `swap_all_marts_if_clean` macro skips the swap even when the build is clean — for demoing the failure path without editing models | `false` |
| `BLUEGREEN_DUCKDB_PATH` | `profiles.yml` for the duckdb_demo target | `dbt_project/.local/demo.duckdb` |
| `SNOWFLAKE_*` | `profiles.yml` snowflake_prod target and Dagster `SnowflakeResource` | placeholder values |
| `DBT_CLOUD_ACCOUNT_ID`, `DBT_CLOUD_PROJECT_ID`, `DBT_CLOUD_ENVIRONMENT_ID`, `DBT_CLOUD_API_TOKEN` | `dbt_blue_green_cloud/defs.yaml.example` | unset (required if activated) |

## Known constraints

- `dbt-core` and `dbt-snowflake` are pinned to `>=1.8,<1.9`. dbt 1.9+ ships a
  `function` materialization declaring `language="javascript"`, which
  dbt-core's `ModelLanguage` enum doesn't recognize — macro parsing crashes
  on load.
- The DuckDB `__snapshot` is only the *previous* state. Two bad runs in a row
  and the snapshot is bad too. Snowflake Time Travel is bounded by the
  retention window (1 day default, 90 max).
- The `on-run-end` swap iterates marts; each statement is independent. For
  fully atomic multi-mart cutover, use view aliasing (consumers query a view
  that flips between two green slots) rather than direct table swap.

## Learn more

- [Dagster docs](https://docs.dagster.io/)
- [Snowflake Time Travel](https://docs.snowflake.com/en/user-guide/data-time-travel)
- [Snowflake zero-copy clone](https://docs.snowflake.com/en/sql-reference/sql/create-clone)
- [Snowflake ALTER TABLE … SWAP WITH](https://docs.snowflake.com/en/sql-reference/sql/alter-table)
- [dagster-dbt: DbtCloudComponent](https://docs.dagster.io/integrations/libraries/dbt)
- [dbt Cloud API v2 — trigger job run](https://docs.getdbt.com/dbt-cloud/api-v2)
