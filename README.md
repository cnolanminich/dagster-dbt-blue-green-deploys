# dbt_bluegreen_demo

Blue/green deployment for a dbt project orchestrated by Dagster, runnable
either against **dbt Core + Snowflake** (production) or **DuckDB** (zero-setup
local demo). The same dbt repo also works unchanged with **dbt Cloud** — see
the [dbt Cloud section](#running-on-dbt-cloud) below.

The pattern: keep two parallel schemas — `green` (live, consumer-facing) and
`blue` (staging). Every run:

1. **Clones green into blue** as a zero-copy snapshot (`on-run-start` hook).
2. **Builds dbt models into blue.**
3. **Swaps each completed table into green** atomically (per-model `post-hook`).

Consumers reading `green` either see the old table or the new one, never a
half-built state.

## Quickstart (no credentials needed)

```bash
uv sync
uv run dg dev
```

Open http://localhost:3000. The `BlueGreenDbtComponent` defaults to
`demo_mode: true`, which runs the real dbt project against a local DuckDB file
at `dbt_project/.local/demo.duckdb`. Seeds load automatically on the first
materialization.

Materialize the dbt assets twice so you can observe both the first-publish
path (no clone needed) and the subsequent clone-and-swap path.

## Running against real Snowflake

Set the Snowflake env vars and flip `demo_mode` in
`src/dbt_bluegreen_demo/defs/dbt_blue_green/defs.yaml`:

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

## How the swap works

The three macros all use dbt's `adapter.dispatch` so the SQL changes by
warehouse but the orchestration shape doesn't.

| Stage | Trigger | Snowflake | DuckDB |
| --- | --- | --- | --- |
| Clone green → blue | dbt `on-run-start` hook | `CREATE OR REPLACE SCHEMA blue CLONE green` (one zero-copy metadata op) | `CREATE OR REPLACE TABLE blue.x AS SELECT * FROM green.x` per table |
| Build into blue | dbt model materializations | normal `dbt build` into the `blue` schema | normal `dbt build` into the `blue` schema |
| Promote blue → green | per-model dbt `post-hook` | `ALTER TABLE blue.x SWAP WITH green.x` (atomic rename) | `CREATE OR REPLACE TABLE green.x AS SELECT * FROM blue.x` |

### What you get

- **Atomic cutover.** Snowflake's `SWAP WITH` is a metadata rename — consumers
  see only old-or-new, never partial. DuckDB's CTAS is not atomic, but for
  local demo/dev that's fine.
- **Fail-forward isolation.** If `dbt build` fails halfway through, blue is
  discarded on the next run's clone. Green is untouched.
- **Cheap on Snowflake.** Zero-copy clones are metadata-only — even TB-scale
  tables clone in milliseconds.
- **Models that didn't run still resolve.** Because the clone seeds blue with
  green's prior state, unselected models, incremental upstreams, and views
  all stay queryable in blue during a partial build.

### What you don't get

- **Cross-table consistency during a build.** Per-model post-hook swap means
  consumers can briefly see a new `mart_orders` joined against an old
  `mart_customers`. If you need atomic multi-table cutover, move the swap to
  an `on-run-end` hook that swaps everything once at the end — the trade-off
  is that one bad model means the whole batch is rejected.

## Running on dbt Cloud

The dbt project (macros, hooks, `dbt_project.yml`) is unchanged — dbt Cloud
runs your repo the same way dbt Core does. The `on-run-start` clone fires,
the per-model `post-hook` swap fires, `adapter.dispatch` resolves to the
Snowflake implementations. The only thing that swaps is the Dagster
component: `DbtProjectComponent` → `DbtCloudComponent`.

An example config lives at
[`src/dbt_bluegreen_demo/defs/dbt_blue_green_cloud/defs.yaml.example`](src/dbt_bluegreen_demo/defs/dbt_blue_green_cloud/defs.yaml.example).
Outline:

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
    tags: { orchestrator: dagster, executed_by: dbt_cloud }
    kinds: [dbt, snowflake]

  defs_state:
    management_type: versioned_state_storage
    refresh_if_dev: true
```

To activate: set the four `DBT_CLOUD_*` env vars, rename
`defs.yaml.example` → `defs.yaml`, and either delete the dbt Core variant or
add `translation.key_prefix: cloud` to avoid asset-key collisions.

### What's different vs dbt Core

- **Credentials live in dbt Cloud**, not in this repo's `profiles.yml` or
  Dagster env vars. The Snowflake connection is configured on the dbt Cloud
  environment.
- **Dagster doesn't run dbt as a subprocess.** It triggers an ad-hoc job run
  via the dbt Cloud v2 API and ingests the resulting manifest.
- **Manifest is state-backed.** `DbtCloudComponent` caches the manifest
  locally so Dagster loads asset specs without an API call on every reload.
  Refresh via `dg utils refresh-defs-state` in CI/CD before deploy.

### What's the same

- **Macros, hooks, schemas, table names.** The blue/green flow is entirely
  in the dbt project, and dbt Cloud is just another way to invoke `dbt
  build` against it.
- **Permissions.** The role configured in your dbt Cloud environment must
  have `OWNERSHIP` (or sufficient grants) on both `green` and `blue` — same
  requirement as the dbt Core path, just configured in a different place.
- **Schedules + asset selection.** The schedules in `defs/schedules/defs.yaml`
  target `kind:dbt` and still work — `DbtCloudComponent` tags assets with
  `kind:dbt` the same way `DbtProjectComponent` does.

## Project layout

```
dbt_bluegreen_demo/
├── dbt_project/
│   ├── dbt_project.yml            # on-run-start clone + per-model post-hook swap
│   ├── profiles.yml               # duckdb_demo + snowflake_prod targets
│   ├── macros/
│   │   ├── clone_green_to_blue.sql       # adapter.dispatch: snowflake__/duckdb__
│   │   ├── swap_blue_to_green.sql        # adapter.dispatch: snowflake__/duckdb__
│   │   └── generate_schema_name.sql      # +schema: blue → "blue" (no env prefix)
│   ├── seeds/                            # raw_orders.csv, raw_customers.csv
│   └── models/
│       ├── staging/                      # stg_orders, stg_customers (views in blue)
│       └── marts/                        # mart_orders, mart_customers (tables, post-hook swap)
└── src/dbt_bluegreen_demo/
    ├── components/
    │   ├── blue_green_dbt_component.py   # DbtProjectComponent subclass + demo_mode toggle
    │   └── scheduled_job_component.py
    └── defs/
        ├── dbt_blue_green/defs.yaml              # dbt Core instance (active)
        ├── dbt_blue_green_cloud/defs.yaml.example # dbt Cloud instance (rename to activate)
        └── schedules/defs.yaml                   # 3 schedules
```

## Environment variables

| Var | Used by | Default |
| --- | --- | --- |
| `BLUEGREEN_DUCKDB_PATH` | `profiles.yml` for the duckdb_demo target | `dbt_project/.local/demo.duckdb` |
| `SNOWFLAKE_*` | `profiles.yml` snowflake_prod target | placeholder values |
| `DBT_CLOUD_ACCOUNT_ID`, `DBT_CLOUD_PROJECT_ID`, `DBT_CLOUD_ENVIRONMENT_ID`, `DBT_CLOUD_API_TOKEN` | `dbt_blue_green_cloud/defs.yaml.example` | unset (required if activated) |

## Known constraints

- `dbt-core` and `dbt-snowflake` are pinned to `>=1.8,<1.9`. dbt 1.9+ ships a
  `function` materialization that declares `language="javascript"`, which
  dbt-core's `ModelLanguage` enum doesn't recognize — macro parsing crashes
  on load.
- Per-model post-hook swap means cross-table inconsistency during a build is
  visible to consumers. Move to `on-run-end` for atomic multi-table cutover
  at the cost of all-or-nothing batches.

## Learn more

- [Dagster docs](https://docs.dagster.io/)
- [Snowflake zero-copy clone](https://docs.snowflake.com/en/sql-reference/sql/create-clone)
- [Snowflake ALTER TABLE … SWAP WITH](https://docs.snowflake.com/en/sql-reference/sql/alter-table)
- [dagster-dbt: DbtCloudComponent](https://docs.dagster.io/integrations/libraries/dbt)
