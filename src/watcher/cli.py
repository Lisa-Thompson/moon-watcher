"""MoonWatcher CLI — cron job monitoring and heartbeat dashboard."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click

from . import __version__
from .scheduler import CronJob, CronScheduler, CronExpression
from .executor import JobExecutor
from .monitor import DriftDetector, HeartbeatMonitor


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", stream=sys.stderr)


@click.group()
@click.version_option(version=__version__, prog_name="moonwatcher")
@click.option("-v", "--verbose", is_flag=True)
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """MoonWatcher — Distributed cron job scheduler and heartbeat monitor."""
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


@main.command()
@click.option("-c", "--config", "config_path", type=click.Path(exists=True), required=True,
              help="YAML config file defining cron jobs")
def validate(config_path: str) -> None:
    """Validate a cron job configuration file."""
    import yaml
    with open(config_path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    scheduler = CronScheduler()
    errors = 0
    for entry in data.get("jobs", []):
        try:
            expr = CronExpression.parse(entry["cron"])
            job = CronJob(
                job_id=entry["id"],
                name=entry.get("name", entry["id"]),
                expression=expr,
                repository=entry.get("repository", ""),
                workflow_id=entry.get("workflow", ""),
                enabled=entry.get("enabled", True),
            )
            job.schedule_next()
            scheduler.add_job(job)
            click.echo(f"  OK  {job.job_id:20s} next={job.next_run}")
        except Exception as e:
            click.echo(f"  ERR {entry.get('id', '?'):20s} {e}", err=True)
            errors += 1
    click.echo(f"\nValidated {len(scheduler._jobs)} jobs, {errors} errors")


@main.command()
@click.option("-c", "--config", "config_path", type=click.Path(exists=True))
def heartbeat(config_path: Optional[str]) -> None:
    """Print a heartbeat status report."""
    scheduler = CronScheduler()
    if config_path:
        import yaml
        with open(config_path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        for entry in data.get("jobs", []):
            expr = CronExpression.parse(entry["cron"])
            job = CronJob(
                job_id=entry["id"],
                name=entry.get("name", entry["id"]),
                expression=expr,
                repository=entry.get("repository", ""),
                workflow_id=entry.get("workflow", ""),
            )
            job.schedule_next()
            scheduler.add_job(job)

    monitor = HeartbeatMonitor()
    status = monitor.pulse(scheduler)
    click.echo(f"MoonWatcher v{__version__}  Heartbeat: {status.checked_at}")
    click.echo(f"  Jobs: {status.total_jobs} total, "
               f"{status.healthy} healthy, "
               f"{status.unhealthy} unhealthy, "
               f"{status.unknown} unknown")
    click.echo(f"  Drift alerts: {len(status.drift_alerts)}")
    click.echo(f"  Missed jobs: {len(status.missed_jobs)}")
    if status.drift_alerts:
        click.echo("\n  Drift details:")
        for signal in status.drift_alerts[:10]:
            click.echo(f"    [{signal.severity}] {signal.job_name}: {signal.description}")


@main.command()
def about() -> None:
    """Show cron expression reference and tips."""
    click.echo("Cron Expression Reference (5-field):")
    click.echo("  minute hour dom month dow")
    click.echo("  0-59   0-23 1-31 1-12   0-6")
    click.echo("")
    click.echo("Shorthands: @hourly @daily @weekly @monthly @yearly")
    click.echo("Specials: */15 = every 15 units, 1,3,5 = at 1,3,5")


if __name__ == "__main__":
    main()
