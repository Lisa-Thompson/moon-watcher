"""Tests for MoonWatcher scheduler, executor, and drift detector."""

from datetime import datetime, timezone, timedelta

from watcher.scheduler import CronScheduler, CronJob, CronExpression, JobState
from watcher.executor import JobExecutor, ExecutionResult
from watcher.monitor import DriftDetector, HeartbeatMonitor, DriftSignal, HeartbeatStatus


class TestCronExpression:
    def test_parse_basic(self):
        expr = CronExpression.parse("0 9 * * 1-5")
        assert expr.minute == "0"
        assert expr.hour == "9"
        assert expr.day_of_week == "1-5"

    def test_parse_shorthand(self):
        expr = CronExpression.parse("@daily")
        assert str(expr) == "0 0 * * *"

    def test_matches(self):
        expr = CronExpression.parse("30 14 * * *")
        dt = datetime(2026, 6, 27, 14, 30, 0, tzinfo=timezone.utc)
        assert expr.matches(dt)
        assert not expr.matches(dt.replace(minute=31))

    def test_next_after(self):
        expr = CronExpression.parse("0 9 * * *")
        after = datetime(2026, 6, 27, 8, 0, 0, tzinfo=timezone.utc)
        times = expr.next_after(after, n=2)
        assert len(times) == 2
        assert times[0].hour == 9
        assert times[0].minute == 0

    def test_parse_invalid_fields(self):
        try:
            CronExpression.parse("0 9 *")
            assert False, "should have raised"
        except ValueError:
            pass


class TestCronJob:
    def test_schedule_next(self):
        expr = CronExpression.parse("@daily")
        job = CronJob(
            job_id="test-1", name="Daily Job",
            expression=expr, repository="o/r", workflow_id="ci",
        )
        job.schedule_next()
        assert job.next_run is not None
        assert job.next_run.hour == 0

    def test_consecutive_failures(self):
        expr = CronExpression.parse("@hourly")
        job = CronJob(
            job_id="fail-job", name="Failing Job",
            expression=expr, repository="o/r", workflow_id="ci",
        )
        job.last_state = JobState.FAILURE
        job.consecutive_failures = 3
        assert job.consecutive_failures == 3
        job.last_state = JobState.SUCCESS
        job.consecutive_failures = 0
        assert job.consecutive_failures == 0


class TestCronScheduler:
    def test_add_and_list(self):
        sched = CronScheduler()
        expr = CronExpression.parse("@daily")
        job = CronJob(
            job_id="j1", name="Test", expression=expr,
            repository="o/r", workflow_id="ci",
        )
        sched.add_job(job)
        assert len(sched.list_jobs()) == 1
        assert sched.get_job("j1") is job

    def test_due_jobs(self):
        sched = CronScheduler()
        expr = CronExpression.parse("* * * * *")
        job = CronJob(
            job_id="j1", name="Every Min", expression=expr,
            repository="o/r", workflow_id="ci",
        )
        job.schedule_next()
        sched.add_job(job)
        now = datetime.now(timezone.utc) + timedelta(minutes=5)
        due = sched.due_jobs(now=now)
        assert len(due) == 1

    def test_overdue_jobs(self):
        sched = CronScheduler()
        expr = CronExpression.parse("0 0 1 1 *")
        job = CronJob(
            job_id="j1", name="Yearly", expression=expr,
            repository="o/r", workflow_id="ci",
        )
        past = datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
        job.next_run = past
        sched.add_job(job)
        overdue = sched.overdue_jobs(tolerance_minutes=0)
        assert len(overdue) == 1

    def test_stats(self):
        sched = CronScheduler()
        expr = CronExpression.parse("@daily")
        for i in range(3):
            job = CronJob(
                job_id=f"j{i}", name=f"Job {i}", expression=expr,
                repository="o/r", workflow_id="ci",
            )
            sched.add_job(job)
        sched._jobs["j0"].last_state = JobState.SUCCESS
        sched._jobs["j1"].last_state = JobState.FAILURE
        stats = sched.stats()
        assert stats["total_jobs"] == 3
        assert stats["last_success"] == 1
        assert stats["last_failure"] == 1


class TestJobExecutor:
    def test_execute_echo(self):
        executor = JobExecutor(max_concurrent=2)
        expr = CronExpression.parse("@daily")
        job = CronJob(
            job_id="test-echo", name="Echo Test",
            expression=expr, repository="o/r", workflow_id="ci",
        )
        job.schedule_next()
        result = executor.execute(job, command=["echo", "hello"])
        assert result.success
        assert "hello" in result.stdout
        assert job.last_run is not None

    def test_execute_failure(self):
        executor = JobExecutor()
        expr = CronExpression.parse("@daily")
        job = CronJob(
            job_id="test-fail", name="Fail Test",
            expression=expr, repository="o/r", workflow_id="ci",
        )
        result = executor.execute(job, command=["python", "-c", "exit(1)"])
        assert not result.success


class TestDriftDetector:
    def test_no_drift_when_on_time(self):
        detector = DriftDetector()
        expr = CronExpression.parse("@daily")
        job = CronJob(
            job_id="j1", name="On Time", expression=expr,
            repository="o/r", workflow_id="ci",
        )
        now = datetime.now(timezone.utc)
        job.next_run = now
        job.last_run = now
        signal = detector.check(job, now=now)
        assert signal is None

    def test_drift_detected(self):
        detector = DriftDetector(warning_seconds=60)
        expr = CronExpression.parse("@daily")
        job = CronJob(
            job_id="j1", name="Drifting", expression=expr,
            repository="o/r", workflow_id="ci",
        )
        now = datetime.now(timezone.utc)
        job.next_run = now - timedelta(seconds=600)
        job.last_run = now
        signal = detector.check(job, now=now)
        assert signal is not None
        assert signal.drift_seconds >= 500


class TestHeartbeatMonitor:
    def test_pulse(self):
        sched = CronScheduler()
        expr = CronExpression.parse("@daily")
        job = CronJob(
            job_id="j1", name="Test", expression=expr,
            repository="o/r", workflow_id="ci",
        )
        job.last_state = JobState.SUCCESS
        sched.add_job(job)
        monitor = HeartbeatMonitor()
        status = monitor.pulse(sched)
        assert status.total_jobs == 1
        assert status.healthy == 1

    def test_trend(self):
        monitor = HeartbeatMonitor()
        trend = monitor.trend(window=1)
        assert trend["status"] == "no_data"
