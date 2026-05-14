"""dbt Core component that runs a blue/green deployment.

`demo_mode: true` (default) runs the real dbt project against a local DuckDB
file (zero setup, no credentials). `demo_mode: false` runs the same project
against Snowflake using env-var-driven credentials. The dbt macros dispatch
on adapter type so the orchestration shape (on-run-start clone, per-model
post-hook swap) stays identical across both warehouses.
"""

import subprocess
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, DbtProjectComponent


@dataclass
class BlueGreenDbtComponent(DbtProjectComponent):
    """Runs `dbt build` for the blue/green project against DuckDB or Snowflake.

    The dbt project itself is adapter-aware via macro dispatch:
      * clone_green_to_blue — Snowflake zero-copy schema clone vs DuckDB CTAS
      * swap_blue_to_green  — Snowflake SWAP WITH vs DuckDB CTAS
    """

    demo_mode: bool = True

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: DbtProject | None,
    ) -> dg.AssetSpec:
        base = super().get_asset_spec(manifest, unique_id, project)
        props = self.get_resource_props(manifest, unique_id)
        name = props["name"]

        layer_tags = {"layer": "staging"} if name.startswith("stg_") else {"layer": "marts"}
        deployment_tags = {"deployment_strategy": "blue_green"} if name.startswith("mart_") else {}
        adapter_kind = "duckdb" if self.demo_mode else "snowflake"

        return base.merge_attributes(
            tags={**layer_tags, **deployment_tags, "orchestrator": "dagster"},
            kinds={"dbt", adapter_kind},
        )

    def execute(
        self, context: dg.AssetExecutionContext, dbt: DbtCliResource
    ) -> Iterator:
        target = "duckdb_demo" if self.demo_mode else "snowflake_prod"
        project_dir = Path(dbt.project_dir)

        if self.demo_mode:
            local_db = project_dir / ".local" / "demo.duckdb"
            # dbt-duckdb won't create parent directories — ensure it exists
            # on every run, not just first-publish.
            local_db.parent.mkdir(parents=True, exist_ok=True)
            if not local_db.exists():
                context.log.info("First demo run — seeding raw tables into DuckDB.")
                # Run seed as a plain subprocess so its events don't conflict
                # with the asset materialization stream below.
                seed_result = subprocess.run(
                    [
                        "dbt", "seed",
                        "--target", target,
                        "--project-dir", str(project_dir),
                        "--profiles-dir", str(project_dir),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                context.log.info(seed_result.stdout)
                if seed_result.returncode != 0:
                    context.log.error(seed_result.stderr)
                    raise dg.Failure(description="dbt seed failed")

        context.log.info(f"Running dbt build against target={target}")
        yield from dbt.cli(
            ["build", "--target", target],
            context=context,
        ).stream()
