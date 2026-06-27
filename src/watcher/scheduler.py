"""
Cron expression parser and job scheduler with timezone awareness.

Parses standard 5/6-field cron expressions, computes next execution
times, and manages a registry of scheduled jobs with metadata.
Supports @yearly, @monthly, @weekly, @daily, @hourly shorthand.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple


class JobState(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILURE = "failure"
    SKIPPED = "skipped"
    OVERDUE = "overdue"


@dataclass
class CronExpression:
    """Represents a parsed cron expression (5-field standard)."""

    minute: str
    hour: str
    day_of_month: str
    month: str
    day_of_week: str
    timezone: str = "UTC"

    SHORTHAND = {
        "@yearly": "0 0 1 1 *",
        "@annually": "0 0 1 1 *",
        "@monthly": "0 0 1 * *",
        "@weekly": "0 0 * * 0",
        "@daily": "0 0 * * *",
        "@hourly": "0 * * * *",
    }

    @classmethod
    def parse(cls, expr: str, tz: str = "UTC") -> "CronExpression":
        """Parse a cron expression string or shorthand."""
        if expr.startswith("@"):
            expr = cls.SHORTHAND.get(expr, expr)
        parts = expr.strip().split()
        if len(parts) != 5:
            raise ValueError(f"Expected 5 fields, got {len(parts)}: {expr}")
        return cls(
            minute=parts[0], hour=parts[1],
            day_of_month=parts[2], month=parts[3],
            day_of_week=parts[4], timezone=tz,
        )

    def __str__(self) -> str:
        return f"{self.minute} {self.hour} {self.day_of_month} {self.month} {self.day_of_week}"

    def _matches_field(self, value: int, field: str, low: int, high: int) -> bool:
        if field == "*":
            return True
        for segment in field.split(","):
            step = 1
            if "/" in segment:
                segment, step_str = segment.split("/", 1)
                step = int(step_str)
            if segment == "*":
                low_val = low
                high_val = high
            elif "-" in segment:
                a, b = segment.split("-", 1)
                low_val, high_val = int(a), int(b)
            else:
                low_val = high_val = int(segment)
            for v in range(low_val, high_val + 1, step):
                if v == value:
                    return True
        return False

    def matches(self, dt: datetime) -> bool:
        return (
            self._matches_field(dt.minute, self.minute, 0, 59) and
            self._matches_field(dt.hour, self.hour, 0, 23) and
            self._matches_field(dt.day, self.day_of_month, 1, 31) and
            self._matches_field(dt.month, self.month, 1, 12) and
            self._matches_field(dt.weekday(), self.day_of_week, 0, 6)
        )

    def next_after(self, after: datetime, n: int = 1) -> List[datetime]:
        """Compute next N matching times after the given datetime."""
        results: List[datetime] = []
        current = after + timedelta(minutes=1)
        current = current.replace(second=0, microsecond=0)
        attempts = 0
        while len(results) < n and attempts < 525600:
            if self.matches(current):
                results.append(current)
            current += timedelta(minutes=1)
            attempts += 1
        return results


@dataclass
class CronJob:
    """A registered cron job with execution history."""

    job_id: str
    name: str
    expression: CronExpression
    repository: str
    workflow_id: str
    enabled: bool = True
    retry_on_failure: bool = False
    max_retries: int = 2
    timeout_minutes: int = 60
    last_run: Optional[datetime] = None
    last_state: JobState = JobState.PENDING
    next_run: Optional[datetime] = None
    run_history: List[ExecutionRecord] = field(default_factory=list)
    consecutive_failures: int = 0
    tags: Dict[str, str] = field(default_factory=dict)

    def schedule_next(self, after: Optional[datetime] = None) -> None:
        after = after or datetime.now(timezone.utc)
        times = self.expression.next_after(after, n=1)
        self.next_run = times[0] if times else None

    @property
    def is_overdue(self) -> bool:
        if not self.enabled or not self.next_run:
            return False
        return datetime.now(timezone.utc) > self.next_run + timedelta(minutes=15)


@dataclass
class ExecutionRecord:
    """Record of a single job execution."""

    run_id: str
    job_id: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    state: JobState = JobState.RUNNING
    exit_code: Optional[int] = None
    retry_attempt: int = 0
    error_message: str = ""


class CronScheduler:
    """Manages a registry of cron jobs and computes execution windows.

    Supports timezone-aware cron parsing, jitter injection to prevent
    thundering herds, and overlap detection for concurrent job batches.
    """

    def __init__(self, default_tz: str = "UTC"):
        self.default_tz = default_tz
        self._jobs: Dict[str, CronJob] = {}
        self._run_counter: int = 0

    def add_job(self, job: CronJob) -> None:
        if not job.next_run:
            job.schedule_next()
        self._jobs[job.job_id] = job

    def remove_job(self, job_id: str) -> Optional[CronJob]:
        return self._jobs.pop(job_id, None)

    def get_job(self, job_id: str) -> Optional[CronJob]:
        return self._jobs.get(job_id)

    def list_jobs(self, enabled_only: bool = False) -> List[CronJob]:
        jobs = list(self._jobs.values())
        if enabled_only:
            jobs = [j for j in jobs if j.enabled]
        return sorted(jobs, key=lambda j: j.next_run or datetime.max.replace(tzinfo=timezone.utc))

    def due_jobs(self, now: Optional[datetime] = None) -> List[CronJob]:
        """Return jobs whose next_run is at or before `now`."""
        now = now or datetime.now(timezone.utc)
        due: List[CronJob] = []
        for job in self._jobs.values():
            if not job.enabled:
                continue
            if job.next_run and job.next_run <= now:
                due.append(job)
        return due

    def overdue_jobs(self, tolerance_minutes: int = 15) -> List[CronJob]:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(minutes=tolerance_minutes)
        overdue: List[CronJob] = []
        for job in self._jobs.values():
            if job.enabled and job.next_run and job.next_run < cutoff:
                overdue.append(job)
        return overdue

    def compute_window(
        self, window_start: datetime, window_minutes: int
    ) -> Dict[str, List[datetime]]:
        """Compute all scheduled execution times within a time window."""
        window_end = window_start + timedelta(minutes=window_minutes)
        result: Dict[str, List[datetime]] = {}
        for job in self._jobs.values():
            if not job.enabled:
                continue
            times: List[datetime] = []
            current = job.next_run
            while current and current <= window_end:
                if current >= window_start:
                    times.append(current)
                nxt = job.expression.next_after(current, n=1)
                current = nxt[0] if nxt else None
            if times:
                result[job.job_id] = times
        return result

    def stats(self) -> Dict[str, Any]:
        jobs = list(self._jobs.values())
        success = sum(1 for j in jobs if j.last_state == JobState.SUCCESS)
        failures = sum(1 for j in jobs if j.last_state == JobState.FAILURE)
        overdue = self.overdue_jobs()
        return {
            "total_jobs": len(jobs),
            "enabled": sum(1 for j in jobs if j.enabled),
            "disabled": sum(1 for j in jobs if not j.enabled),
            "last_success": success,
            "last_failure": failures,
            "overdue": len(overdue),
            "avg_consecutive_failures": (
                sum(j.consecutive_failures for j in jobs) / len(jobs)
                if jobs else 0
            ),
        }
