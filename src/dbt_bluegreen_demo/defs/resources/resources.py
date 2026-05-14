"""Shared resources: SnowflakeResource for the duplicate check.

In demo mode the asset check ignores this resource, but it must still be
constructible without failing — so we read env vars with os.environ.get and
fall back to placeholder values. Real values come through the same env vars
when running in production.
"""

import os

import dagster as dg
from dagster_snowflake import SnowflakeResource


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "snowflake": SnowflakeResource(
                account=os.environ.get("SNOWFLAKE_ACCOUNT", "demo_account"),
                user=os.environ.get("SNOWFLAKE_USER", "demo_user"),
                password=os.environ.get("SNOWFLAKE_PASSWORD", "demo_password"),
                role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORMING"),
                database=os.environ.get("SNOWFLAKE_DATABASE", "ANALYTICS_DB"),
                schema=os.environ.get("SNOWFLAKE_SCHEMA", "ANALYTICS_BLUE"),
            ),
        },
    )
