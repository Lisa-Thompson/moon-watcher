"""
Drift detection and heartbeat monitoring for scheduled job execution.

Detects timing drift, missed executions, silent failures, and
anomalous runtime patterns using statistical baselines.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from .scheduler import CronJob, CronScheduler, ExecutionRecord, JobState


@dataclass
class DriftSignal:
    """Detected timing drift for a specific job."""

    job_id: str
    job_name: str
    expected_at: datetime
    actual_at: Optional[datetime]
    drift_seconds: float
    severity: str  # low, medium, high
    description: str


@dataclass
class HeartbeatStatus:
    """Aggregate heartbeat status across all monitored jobs."""

    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    total_jobs: int = 0
    healthy: int = 0
    unhealthy: int = 0
    unknown: int = 0
    drift_alerts: List[DriftSignal] = field(default_factory=list)
    missed_jobs: List[str] = field(default_factory=list)


class DriftDetector:
    """Detects timing drift in cron job execution.

    Compares actual execution timestamps against scheduled next_run,
    flags deviations beyond configurable tolerance windows.
    """

    DEFAULT_DRIFT_WARNING_SECONDS = 300   # 5 minutes
    DEFAULT_DRIFT_CRITICAL_SECONDS = 900  # 15 minutes
    DEFAULT_MISSED_THRESHOLD_SECONDS = 1800  # 30 minutes

    def __init__(
        self,
        warning_seconds: int = DEFAULT_DRIFT_WARNING_SECONDS,
        critical_seconds: int = DEFAULT_DRIFT_CRITICAL_SECONDS,
        missed_seconds: int = DEFAULT_MISSED_THRESHOLD_SECONDS,
    ):
        self.warning_seconds = warning_seconds
        self.critical_seconds = critical_seconds
        self.missed_seconds = missed_seconds

    def check(self, job: CronJob, now: Optional[datetime] = None) -> Optional[DriftSignal]:
        """Check a single job for timing drift."""
        now = now or datetime.now(timezone.utc)
        if not job.next_run:
            return None
        if job.last_run is None:
            if now > job.next_run + timedelta(seconds=self.missed_seconds):
                return DriftSignal(
                    job_id=job.job_id, job_name=job.name,
                    expected_at=job.next_run, actual_at=None,
                    drift_seconds=(now - job.next_run).total_seconds(),
                    severity="high",
                    description=f"Never executed; scheduled {job.next_run.isoformat()}",
                )
            return None
        drift = (job.last_run - job.next_run).total_seconds()
        drift_abs = abs(drift)
        if drift_abs > self.critical_seconds:
            severity = "high"
        elif drift_abs > self.warning_seconds:
            severity = "medium"
        else:
            return None
        return DriftSignal(
            job_id=job.job_id, job_name=job.name,
            expected_at=job.next_run, actual_at=job.last_run,
            drift_seconds=drift, severity=severity,
            description=(
                f"Expected {job.next_run.isoformat()}, "
                f"actual {job.last_run.isoformat()} "
                f"({drift:+.0f}s drift)"
            ),
        )

    def check_all(self, scheduler: CronScheduler) -> List[DriftSignal]:
        """Check all jobs in a scheduler for drift."""
        signals: List[DriftSignal] = []
        now = datetime.now(timezone.utc)
        for job in scheduler.list_jobs(enabled_only=True):
            signal = self.check(job, now=now)
            if signal:
                signals.append(signal)
        return signals

    def check_missed(self, scheduler: CronScheduler) -> List[str]:
        """Return IDs of jobs that have missed their scheduled window."""
        now = datetime.now(timezone.utc)
        missed: List[str] = []
        for job in scheduler.list_jobs(enabled_only=True):
            if job.next_run and job.next_run < now - timedelta(seconds=self.missed_seconds):
                if not job.last_run or job.last_run < job.next_run:
                    missed.append(job.job_id)
        return missed


class HeartbeatMonitor:
    """Monitors the overall health of a cron job fleet.

    Aggregates drift signals, execution failures, and missed windows
    into a single heartbeat status report.
    """

    def __init__(self, drift_detector: Optional[DriftDetector] = None):
        self.drift_detector = drift_detector or DriftDetector()
        self._history: List[HeartbeatStatus] = []

    def pulse(self, scheduler: CronScheduler) -> HeartbeatStatus:
        """Take a heartbeat reading from the scheduler."""
        jobs = scheduler.list_jobs(enabled_only=True)
        drift_signals = self.drift_detector.check_all(scheduler)
        missed = self.drift_detector.check_missed(scheduler)
        healthy = 0
        unhealthy = 0
        unknown = 0
        for job in jobs:
            if job.last_state == JobState.SUCCESS:
                healthy += 1
            elif job.last_state in (JobState.FAILURE, JobState.OVERDUE):
                unhealthy += 1
            else:
                unknown += 1
        status = HeartbeatStatus(
            checked_at=datetime.now(timezone.utc),
            total_jobs=len(jobs),
            healthy=healthy,
            unhealthy=unhealthy,
            unknown=unknown,
            drift_alerts=drift_signals,
            missed_jobs=missed,
        )
        self._history.append(status)
        if len(self._history) > 168:  # ~7 days hourly
            self._history = self._history[-168:]
        return status

    def trend(self, window: int = 24) -> Dict[str, Any]:
        """Compute health trend over recent heartbeat history."""
        recent = self._history[-window:]
        if not recent:
            return {"status": "no_data"}
        total_healthy = sum(h.healthy for h in recent)
        total_unhealthy = sum(h.unhealthy for h in recent)
        total_jobs = sum(h.total_jobs for h in recent)
        return {
            "hours_of_data": len(recent),
            "avg_health_pct": (
                total_healthy / max(total_jobs, 1) if total_jobs > 0 else 0
            ),
            "peak_unhealthy": max((h.unhealthy for h in recent), default=0),
            "total_drift_alerts": sum(len(h.drift_alerts) for h in recent),
            "total_missed": sum(len(h.missed_jobs) for h in recent),
        }
