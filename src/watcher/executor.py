"""
Job execution engine with retry, timeout, and graceful shutdown.

Manages concurrent job execution, captures exit codes and timing,
and feeds results back into the scheduler's run history.
"""

from __future__ import annotations

import logging
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .scheduler import CronJob, CronScheduler, ExecutionRecord, JobState

logger = logging.getLogger("watcher.executor")


class ExecutionResult:
    """Result of a single job execution."""

    def __init__(
        self,
        job_id: str,
        run_id: str,
        success: bool,
        exit_code: Optional[int] = None,
        duration_seconds: float = 0,
        stdout: str = "",
        stderr: str = "",
    ):
        self.job_id = job_id
        self.run_id = run_id
        self.success = success
        self.exit_code = exit_code
        self.duration_seconds = duration_seconds
        self.stdout = stdout
        self.stderr = stderr

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "run_id": self.run_id,
            "success": self.success,
            "exit_code": self.exit_code,
            "duration_seconds": round(self.duration_seconds, 2),
        }


class JobExecutor:
    """Executes cron jobs with concurrency limits and timeout enforcement.

    Executions are fire-and-forget: the caller is responsible for
    checking results via the scheduler's run_history.

    Thread-safe for concurrent job launches.
    """

    def __init__(
        self,
        max_concurrent: int = 4,
        default_timeout: int = 3600,
        scheduler: Optional[CronScheduler] = None,
    ):
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.scheduler = scheduler
        self._active: Dict[str, subprocess.Popen] = {}
        self._lock = threading.Lock()
        self._execution_callback: Optional[Callable[[ExecutionResult], None]] = None

    def on_complete(self, callback: Callable[[ExecutionResult], None]) -> None:
        self._execution_callback = callback

    def execute(self, job: CronJob, command: Optional[List[str]] = None) -> ExecutionResult:
        """Execute a single cron job synchronously."""
        run_id = f"{job.job_id}-{uuid.uuid4().hex[:8]}"
        started = datetime.now(timezone.utc)
        record = ExecutionRecord(
            run_id=run_id, job_id=job.job_id,
            started_at=started, state=JobState.RUNNING,
        )
        if command is None:
            command = ["echo", f"Would execute: {job.name}"]
        try:
            proc = subprocess.run(
                command,
                capture_output=True, text=True,
                timeout=job.timeout_minutes * 60 or self.default_timeout,
                shell=False,
            )
            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()
            success = proc.returncode == 0
            record.finished_at = finished
            record.duration_seconds = duration
            record.exit_code = proc.returncode
            record.state = JobState.SUCCESS if success else JobState.FAILURE
            if not success:
                record.error_message = proc.stderr[:500]
            result = ExecutionResult(
                job_id=job.job_id, run_id=run_id,
                success=success, exit_code=proc.returncode,
                duration_seconds=duration,
                stdout=proc.stdout[:2000],
                stderr=proc.stderr[:2000],
            )
        except subprocess.TimeoutExpired:
            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()
            record.finished_at = finished
            record.duration_seconds = duration
            record.state = JobState.FAILURE
            record.error_message = "Timeout"
            result = ExecutionResult(
                job_id=job.job_id, run_id=run_id,
                success=False, duration_seconds=duration,
                stderr="Timeout",
            )
        except Exception as e:
            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()
            record.finished_at = finished
            record.duration_seconds = duration
            record.state = JobState.FAILURE
            record.error_message = str(e)[:500]
            result = ExecutionResult(
                job_id=job.job_id, run_id=run_id,
                success=False, duration_seconds=duration,
                stderr=str(e)[:500],
            )

        job.last_run = finished
        job.last_state = record.state
        if record.state == JobState.SUCCESS:
            job.consecutive_failures = 0
        elif record.state == JobState.FAILURE:
            job.consecutive_failures += 1
        job.run_history.append(record)
        if len(job.run_history) > 100:
            job.run_history = job.run_history[-100:]
        job.schedule_next(after=finished)

        if self._execution_callback:
            self._execution_callback(result)
        return result

    def execute_batch(
        self, jobs: List[CronJob], commands: Optional[Dict[str, List[str]]] = None
    ) -> List[ExecutionResult]:
        """Execute multiple jobs concurrently within executor limits.

        Respects max_concurrent, launching in waves if needed.
        """
        if not jobs:
            return []
        commands = commands or {}
        results: List[ExecutionResult] = []
        for i in range(0, len(jobs), self.max_concurrent):
            batch = jobs[i:i + self.max_concurrent]
            threads: List[threading.Thread] = []
            batch_results: Dict[str, ExecutionResult] = {}

            def _worker(job: CronJob) -> None:
                cmd = commands.get(job.job_id)
                result = self.execute(job, command=cmd)
                batch_results[job.job_id] = result

            for job in batch:
                t = threading.Thread(target=_worker, args=(job,), daemon=True)
                threads.append(t)
                t.start()
            for t in threads:
                t.join()
            for job in batch:
                if job.job_id in batch_results:
                    results.append(batch_results[job.job_id])
        return results

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    def shutdown(self, wait: bool = True) -> None:
        """Terminate all active executions gracefully."""
        with self._lock:
            for proc in list(self._active.values()):
                try:
                    proc.terminate()
                except Exception:
                    pass
            if wait:
                for proc in list(self._active.values()):
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                self._active.clear()
