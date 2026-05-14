"""Scheduled job component — schedule asset selections with cron strings."""

import dagster as dg


class ScheduledJobComponent(dg.Component, dg.Model, dg.Resolvable):
    """Adds a Dagster job + schedule for an asset selection string."""

    job_name: str
    cron_schedule: str
    asset_selection: str

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        job = dg.define_asset_job(
            name=self.job_name,
            selection=self.asset_selection,
        )
        schedule = dg.ScheduleDefinition(
            job=job,
            cron_schedule=self.cron_schedule,
            default_status=dg.DefaultScheduleStatus.RUNNING,
        )
        return dg.Definitions(jobs=[job], schedules=[schedule])
