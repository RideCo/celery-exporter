import logging
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import NamedTuple

import pytest
from celery.contrib.testing.worker import start_worker  # type: ignore
from celery.utils.time import adjust_timestamp, utcoffset  # type: ignore

from src.exporter import Exporter, reverse_adjust_timestamp


# pylint: disable=too-many-lines
def track_event(exporter, event_type, task):
    exporter.state = SimpleNamespace(
        event=lambda _event: None,
        tasks=SimpleNamespace(get=lambda _uuid: task),
    )
    exporter.track_task_event({"type": event_type, "uuid": "task-id"})


def track_task_sent(exporter, task):
    track_event(exporter, "task-sent", task)


@pytest.fixture
def assert_exporter_metric_called(mocker, celery_app, hostname):
    def fn(metric):
        labels = mocker.patch.object(metric, "labels")

        @celery_app.task
        def slow_task():
            logging.info("Started the slow task")
            time.sleep(3)
            logging.info("Finished the slow task")

        # Use start_worker context manager to ensure worker is available
        with start_worker(celery_app, without_heartbeat=False):
            time.sleep(1)
            slow_task.delay().get()
            assert labels.call_count >= 1
            labels.assert_called_with(hostname=hostname)
            labels.return_value.set.assert_any_call(1)

    return fn


@pytest.mark.celery()
def test_worker_tasks_active(broker, threaded_exporter, assert_exporter_metric_called):
    if broker != "memory":
        pytest.skip(
            reason="test_worker_tasks_active can only be tested for the in-memory broker"
        )

    assert_exporter_metric_called(threaded_exporter.worker_tasks_active)


@pytest.mark.celery()
def test_worker_heartbeat_status(
    broker, threaded_exporter, assert_exporter_metric_called
):
    if broker != "memory":
        pytest.skip(
            reason="test_worker_tasks_active can only be tested for the in-memory broker"
        )

    assert_exporter_metric_called(threaded_exporter.celery_worker_up)


@pytest.mark.celery()
def test_worker_status(threaded_exporter, celery_app, hostname):
    time.sleep(5)

    with start_worker(celery_app, without_heartbeat=False):
        time.sleep(2)
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_worker_up", labels={"hostname": hostname}
            )
            == 1.0
        )

    time.sleep(2)
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_worker_up", labels={"hostname": hostname}
        )
        == 0.0
    )


@pytest.mark.parametrize(
    "input_utcoffset, sleep_seconds, expected_metric_value",
    [
        (None, 5, 0.0),
        (0, 5, 0.0),
        (7, 5, 0.0),
        (7, 0, 1.0),
    ],  # Eg: PST (America/Los_Angeles)
)
def test_worker_timeout_status(
    input_utcoffset, sleep_seconds, expected_metric_value, threaded_exporter, hostname
):
    ts = adjust_timestamp(time.time(), (input_utcoffset or 0))
    threaded_exporter.track_worker_status(
        {"hostname": hostname, "timestamp": ts, "utcoffset": input_utcoffset}, True
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_worker_up", labels={"hostname": hostname}
        )
        == 1.0
    )
    assert threaded_exporter.worker_last_seen[hostname] == {
        "forgotten": False,
        "ts": reverse_adjust_timestamp(ts, input_utcoffset),
    }

    time.sleep(sleep_seconds)
    threaded_exporter.scrape()
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_worker_up", labels={"hostname": hostname}
        )
        == expected_metric_value
    )


@pytest.mark.parametrize(
    "input_utcoffset, sleep_seconds, expected_metric_value",
    [
        (None, 15, None),
        (0, 15, None),
        (7, 15, None),
        (7, 0, 1.0),
    ],  # Eg: PST (America/Los_Angeles)
)
def test_purge_offline_worker_metrics(
    input_utcoffset, sleep_seconds, expected_metric_value, threaded_exporter, hostname
):
    ts = adjust_timestamp(time.time(), (input_utcoffset or 0))
    threaded_exporter.track_worker_status(
        {"hostname": hostname, "timestamp": ts, "utcoffset": input_utcoffset}, True
    )
    threaded_exporter.worker_tasks_active.labels(hostname=hostname).inc()
    threaded_exporter.celery_task_runtime.labels(
        name="boosh", hostname=hostname, queue_name="test"
    ).observe(1.0)
    threaded_exporter.celery_task_queue_wait_time.labels(
        name="boosh", hostname=hostname, queue_name="test"
    ).observe(1.0)
    threaded_exporter.state_counters["task-sent"].labels(
        name="boosh", hostname=hostname, queue_name="test"
    ).inc()

    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_worker_up", labels={"hostname": hostname}
        )
        == 1.0
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_worker_tasks_active", labels={"hostname": hostname}
        )
        == 1.0
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_task_runtime_count",
            labels={"hostname": hostname, "queue_name": "test", "name": "boosh"},
        )
        == 1.0
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_task_queue_wait_time_count",
            labels={"hostname": hostname, "queue_name": "test", "name": "boosh"},
        )
        == 1.0
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_task_sent_total",
            labels={"hostname": hostname, "queue_name": "test", "name": "boosh"},
        )
        == 1.0
    )

    assert threaded_exporter.worker_last_seen[hostname] == {
        "forgotten": False,
        "ts": reverse_adjust_timestamp(ts, input_utcoffset),
    }

    time.sleep(sleep_seconds)
    threaded_exporter.scrape()
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_worker_up", labels={"hostname": hostname}
        )
        == expected_metric_value
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_worker_tasks_active", labels={"hostname": hostname}
        )
        == expected_metric_value
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_task_runtime_count",
            labels={"hostname": hostname, "queue_name": "test", "name": "boosh"},
        )
        == expected_metric_value
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_task_queue_wait_time_count",
            labels={"hostname": hostname, "queue_name": "test", "name": "boosh"},
        )
        == expected_metric_value
    )
    assert (
        threaded_exporter.registry.get_sample_value(
            "celery_task_sent_total",
            labels={"hostname": hostname, "queue_name": "test", "name": "boosh"},
        )
        == expected_metric_value
    )


def test_worker_offline_event_does_not_recreate_purged_metric(hostname):
    exporter = Exporter()
    exporter.track_worker_status(
        {"hostname": hostname, "timestamp": time.time(), "utcoffset": 0}, True
    )
    exporter.purge_worker_metrics(hostname)

    assert (
        exporter.registry.get_sample_value(
            "celery_worker_up", labels={"hostname": hostname}
        )
        is None
    )

    exporter.track_worker_status(
        {"hostname": hostname, "timestamp": time.time(), "utcoffset": 0}, False
    )

    assert (
        exporter.registry.get_sample_value(
            "celery_worker_up", labels={"hostname": hostname}
        )
        is None
    )


def test_purge_stale_generic_task_sent_metrics(mocker):
    exporter = Exporter(
        purge_offline_worker_metrics_seconds=10,
        generic_hostname_task_sent_metric=True,
        static_label={"cluster": "test"},
    )
    now = mocker.patch("src.exporter.time.time")

    now.return_value = 100
    track_task_sent(
        exporter,
        SimpleNamespace(name="old-task", hostname="client@one", queue="first"),
    )
    now.return_value = 105
    track_task_sent(
        exporter,
        SimpleNamespace(name="active-task", hostname="client@two", queue="second"),
    )

    now.return_value = 111
    exporter.track_timed_out_workers()

    assert (
        exporter.registry.get_sample_value(
            "celery_task_sent_total",
            labels={
                "hostname": "generic",
                "name": "old-task",
                "queue_name": "first",
                "cluster": "test",
            },
        )
        is None
    )
    assert (
        exporter.registry.get_sample_value(
            "celery_task_sent_total",
            labels={
                "hostname": "generic",
                "name": "active-task",
                "queue_name": "second",
                "cluster": "test",
            },
        )
        == 1.0
    )


def test_generic_task_sent_last_seen_is_refreshed(mocker):
    exporter = Exporter(
        purge_offline_worker_metrics_seconds=10,
        generic_hostname_task_sent_metric=True,
    )
    task = SimpleNamespace(name="active-task", hostname="client@one", queue="celery")
    now = mocker.patch("src.exporter.time.time")

    now.return_value = 100
    track_task_sent(exporter, task)
    now.return_value = 109
    track_task_sent(exporter, task)
    now.return_value = 111
    exporter.track_timed_out_workers()

    assert (
        exporter.registry.get_sample_value(
            "celery_task_sent_total",
            labels={
                "hostname": "generic",
                "name": "active-task",
                "queue_name": "celery",
            },
        )
        == 2.0
    )


def test_generic_task_sent_is_not_tracked_when_purging_is_disabled(mocker):
    exporter = Exporter(
        purge_offline_worker_metrics_seconds=0,
        generic_hostname_task_sent_metric=True,
    )
    mocker.patch("src.exporter.time.time", return_value=100)

    track_task_sent(
        exporter,
        SimpleNamespace(name="task", hostname="client@one", queue="celery"),
    )

    assert not exporter.generic_last_seen
    assert (
        exporter.registry.get_sample_value(
            "celery_task_sent_total",
            labels={"hostname": "generic", "name": "task", "queue_name": "celery"},
        )
        == 1.0
    )


def test_generic_metrics_are_not_tracked_when_flags_are_disabled(mocker):
    exporter = Exporter(purge_offline_worker_metrics_seconds=10)
    mocker.patch("src.exporter.time.time", return_value=100)

    track_task_sent(
        exporter,
        SimpleNamespace(name="task", hostname="client@one", queue="celery"),
    )

    # Real-hostname series are the worker lifecycle's responsibility, so they must not
    # end up on the generic purge timer.
    assert not exporter.generic_last_seen
    assert (
        exporter.registry.get_sample_value(
            "celery_task_sent_total",
            labels={"hostname": "one", "name": "task", "queue_name": "celery"},
        )
        == 1.0
    )


def test_generic_task_sent_metric_is_recreated_after_purge(mocker):
    exporter = Exporter(
        purge_offline_worker_metrics_seconds=10,
        generic_hostname_task_sent_metric=True,
    )
    task = SimpleNamespace(name="task", hostname="client@one", queue="celery")
    labels = {"hostname": "generic", "name": "task", "queue_name": "celery"}
    now = mocker.patch("src.exporter.time.time")

    now.return_value = 100
    track_task_sent(exporter, task)
    now.return_value = 111
    exporter.track_timed_out_workers()

    assert exporter.registry.get_sample_value("celery_task_sent_total", labels) is None

    now.return_value = 112
    track_task_sent(exporter, task)

    # The counter restarts from zero rather than resuming the pre-purge total.
    assert exporter.registry.get_sample_value("celery_task_sent_total", labels) == 1.0

    now.return_value = 123
    exporter.track_timed_out_workers()

    assert exporter.registry.get_sample_value("celery_task_sent_total", labels) is None


def test_purge_stale_generic_worker_task_metrics(mocker):
    exporter = Exporter(
        purge_offline_worker_metrics_seconds=10,
        generic_hostname_worker_task_metric=True,
    )
    labels = {"hostname": "generic", "name": "task", "queue_name": "celery"}
    now = mocker.patch("src.exporter.time.time")

    now.return_value = 100
    track_event(
        exporter,
        "task-succeeded",
        SimpleNamespace(
            name="task", hostname="worker@one", queue="celery", runtime=1.5
        ),
    )

    # The event increments task-succeeded and zero-instantiates every sibling counter,
    # all at hostname="generic". None of them are reachable by worker purging.
    assert (
        exporter.registry.get_sample_value("celery_task_succeeded_total", labels) == 1.0
    )
    assert (
        exporter.registry.get_sample_value("celery_task_started_total", labels) == 0.0
    )
    assert (
        exporter.registry.get_sample_value("celery_task_runtime_count", labels) == 1.0
    )

    now.return_value = 111
    exporter.track_timed_out_workers()

    assert (
        exporter.registry.get_sample_value("celery_task_succeeded_total", labels)
        is None
    )
    assert (
        exporter.registry.get_sample_value("celery_task_started_total", labels) is None
    )
    assert (
        exporter.registry.get_sample_value("celery_task_runtime_count", labels) is None
    )
    assert not exporter.generic_last_seen


def test_purge_stale_generic_worker_task_metrics_with_exception_label(mocker):
    exporter = Exporter(
        purge_offline_worker_metrics_seconds=10,
        generic_hostname_worker_task_metric=True,
    )
    labels = {
        "hostname": "generic",
        "name": "task",
        "queue_name": "celery",
        "exception": "ValueError",
    }
    now = mocker.patch("src.exporter.time.time")

    now.return_value = 100
    track_event(
        exporter,
        "task-failed",
        SimpleNamespace(
            name="task",
            hostname="worker@one",
            queue="celery",
            exception="ValueError('boom')",
        ),
    )

    # task-failed carries an extra label, so purging must key off each metric's own
    # label names rather than a shared labelset.
    assert exporter.registry.get_sample_value("celery_task_failed_total", labels) == 1.0

    now.return_value = 111
    exporter.track_timed_out_workers()

    assert (
        exporter.registry.get_sample_value("celery_task_failed_total", labels) is None
    )
    assert not exporter.generic_last_seen


def test_generic_worker_task_metrics_do_not_purge_real_hostname_series(mocker):
    exporter = Exporter(purge_offline_worker_metrics_seconds=10)
    now = mocker.patch("src.exporter.time.time")

    now.return_value = 100
    track_event(
        exporter,
        "task-succeeded",
        SimpleNamespace(
            name="task", hostname="worker@one", queue="celery", runtime=1.5
        ),
    )

    now.return_value = 111
    exporter.track_timed_out_workers()

    # Without a heartbeat there is no worker_last_seen entry, so worker purging leaves
    # these alone; the generic timer must not reach them either.
    assert (
        exporter.registry.get_sample_value(
            "celery_task_succeeded_total",
            labels={"hostname": "one", "name": "task", "queue_name": "celery"},
        )
        == 1.0
    )


def test_worker_generic_task_sent_hostname(threaded_exporter, celery_app, hostname):
    threaded_exporter.generic_hostname_task_sent_metric = True
    time.sleep(5)

    @celery_app.task
    def succeed():
        pass

    succeed.apply_async()

    with start_worker(celery_app, without_heartbeat=False):
        time.sleep(5)
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_task_sent_total",
                labels={
                    "hostname": "generic",
                    "name": "src.test_metrics.succeed",
                    "queue_name": "celery",
                },
            )
            == 1.0
        )

        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_task_sent_total",
                labels={
                    "hostname": hostname,
                    "name": "src.test_metrics.succeed",
                    "queue_name": "celery",
                },
            )
            is None
        )


def test_worker_generic_task_hostname(threaded_exporter, celery_app, hostname):
    threaded_exporter.generic_hostname_worker_task_metric = True
    time.sleep(5)

    @celery_app.task
    def succeed():
        pass

    succeed.apply_async()

    with start_worker(celery_app, without_heartbeat=False):
        time.sleep(5)

        # The worker-executed counter and the runtime histogram carry the generic
        # hostname, not the executing worker's.
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_task_succeeded_total",
                labels={
                    "hostname": "generic",
                    "name": "src.test_metrics.succeed",
                    "queue_name": "celery",
                },
            )
            == 1.0
        )
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_task_runtime_count",
                labels={
                    "hostname": "generic",
                    "name": "src.test_metrics.succeed",
                    "queue_name": "celery",
                },
            )
            == 1.0
        )
        # The runtime histogram is only ever touched by observe() on the collapsed
        # worker event, so no series exists under the real worker hostname.
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_task_runtime_count",
                labels={
                    "hostname": hostname,
                    "name": "src.test_metrics.succeed",
                    "queue_name": "celery",
                },
            )
            is None
        )

        # celery_task_sent is client-side and governed by its own flag, so this flag
        # leaves it labeled with the real hostname.
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_task_sent_total",
                labels={
                    "hostname": hostname,
                    "name": "src.test_metrics.succeed",
                    "queue_name": "celery",
                },
            )
            == 1.0
        )

        # celery_worker_up does not pass through track_task_event, so it keeps the real
        # per-worker hostname that KEDA's scaler depends on.
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_worker_up", labels={"hostname": hostname}
            )
            == 1.0
        )
        assert (
            threaded_exporter.registry.get_sample_value(
                "celery_worker_up", labels={"hostname": "generic"}
            )
            is None
        )


# ---------------------------------------------------------------------------
# Tests for celery_idle_process_count
# ---------------------------------------------------------------------------


def _make_heartbeat_event(hostname, active_tasks):
    """Build a worker-heartbeat event dict.

    Mirrors the fields a real worker sends. Note there is no "pool" key: pool
    sizes are only ever known from inspect().stats(), never from an event.
    """
    now = time.time()
    return {
        "type": "worker-heartbeat",
        "hostname": hostname,
        "timestamp": now,
        "local_received": now,
        "utcoffset": 0,
        "active": active_tasks,
        "processed": 0,
        "loadavg": [0.0, 0.0, 0.0],
        "freq": 2.0,
        "sw_ident": "py-celery",
        "sw_ver": "5.0",
        "sw_sys": "Linux",
        "clock": 1,
        "pid": 1234,
    }


class WorkerSpec(NamedTuple):
    """A worker as reported by inspect(), plus the state of its last heartbeat."""

    name: str = "celery@host-1"
    #: Pool processes in inspect().stats(). None for pools that report none.
    processes: int | None = 4
    #: Reported by gevent/eventlet/threads pools in place of "processes".
    max_concurrency: int | None = None
    #: Active tasks in the worker's last heartbeat. None if it never sent one.
    active_tasks: int | None = 0
    queues: tuple[str, ...] = ("default",)

    @property
    def pool_stats(self):
        if self.processes is not None:
            return {"processes": list(range(self.processes))}
        return {
            "implementation": "celery.concurrency.gevent:TaskPool",
            "max-concurrency": self.max_concurrency,
        }


@pytest.fixture
def scrape_queue_metrics(mocker, celery_app):
    """Run track_queue_metrics() for the given workers against a stubbed broker.

    Workers listed in ``offline`` send a heartbeat and then go offline, so they
    are forgotten and no longer answer inspect().
    """

    def scrape(*workers, offline=()):
        exporter = Exporter()
        exporter.app = celery_app
        exporter.state = celery_app.events.State()

        for worker in (*offline, *workers):
            if worker.active_tasks is not None:
                exporter.track_worker_heartbeat(
                    _make_heartbeat_event(worker.name, worker.active_tasks)
                )
        for worker in offline:
            exporter.track_worker_status(
                _make_heartbeat_event(worker.name, worker.active_tasks), is_online=False
            )

        # Stub the inspect calls so track_queue_metrics needs no live workers
        mocker.patch.object(
            exporter.app.control,
            "inspect",
            return_value=mocker.MagicMock(
                stats=mocker.MagicMock(
                    return_value={w.name: {"pool": w.pool_stats} for w in workers}
                ),
                active_queues=mocker.MagicMock(
                    return_value={
                        w.name: [{"name": q} for q in w.queues] for w in workers
                    }
                ),
            ),
        )
        mocker.patch("src.exporter.queue_length", return_value=0)
        mocker.patch("src.exporter.rabbitmq_queue_consumer_count", return_value=1)

        with celery_app.connection() as conn:
            mocker.patch.object(exporter.app, "connection", return_value=conn)
            mocker.patch.object(conn, "info", return_value={"transport": "memory"})
            exporter.track_queue_metrics()

        return exporter

    return scrape


def _queue_gauge(exporter, metric, queue, **extra_labels):
    return exporter.registry.get_sample_value(
        metric, labels={"queue_name": queue, **extra_labels}
    )


@pytest.mark.parametrize(
    "workers,expected_idle",
    [
        pytest.param(
            (WorkerSpec(processes=4, active_tasks=2),),
            {"default": 2.0},
            id="single-worker",
        ),
        pytest.param(
            (WorkerSpec(processes=4, active_tasks=0),),
            {"default": 4.0},
            id="no-active-tasks",
        ),
        pytest.param(
            (WorkerSpec(processes=4, active_tasks=4),),
            {"default": 0.0},
            id="fully-busy",
        ),
        # Active tasks can outnumber the pool size the scrape sees, e.g. after a
        # worker shrinks its pool. Idle is clamped at 0 rather than going negative.
        pytest.param(
            (WorkerSpec(processes=4, active_tasks=6),),
            {"default": 0.0},
            id="more-active-tasks-than-processes",
        ),
        # No heartbeat means the worker is absent from the event state, so its
        # active count reads 0, matching the worker_tasks_active default.
        pytest.param(
            (WorkerSpec(processes=3, active_tasks=None),),
            {"default": 3.0},
            id="no-heartbeat-yet",
        ),
        pytest.param(
            (
                WorkerSpec("celery@host-a", processes=4, active_tasks=1),
                WorkerSpec("celery@host-b", processes=2, active_tasks=2),
            ),
            {"default": 3.0},
            id="two-workers-one-queue",
        ),
        # celery@host-1 and gen2@host-1 collapse into a single worker_tasks_active
        # series because the hostname label is stripped by get_hostname(). Active
        # counts must therefore be read per worker from the event state, giving
        # 3 + 1 idle rather than the same value counted twice.
        pytest.param(
            (
                WorkerSpec("celery@host-1", processes=4, active_tasks=1),
                WorkerSpec("gen2@host-1", processes=4, active_tasks=3),
            ),
            {"default": 4.0},
            id="two-workers-same-host",
        ),
        pytest.param(
            (WorkerSpec(processes=4, active_tasks=1, queues=("default", "priority")),),
            {"default": 3.0, "priority": 3.0},
            id="one-worker-two-queues",
        ),
        # gevent/eventlet/threads pools report no processes, so their configured
        # max-concurrency stands in for the pool size.
        pytest.param(
            (WorkerSpec(processes=None, max_concurrency=100, active_tasks=30),),
            {"default": 70.0},
            id="greenlet-pool-uses-max-concurrency",
        ),
    ],
)
def test_idle_process_count(scrape_queue_metrics, workers, expected_idle):
    exporter = scrape_queue_metrics(*workers)

    for queue, expected in expected_idle.items():
        idle = _queue_gauge(
            exporter, "celery_idle_process_count", queue, autoscaling="yes"
        )
        total = _queue_gauge(exporter, "celery_active_process_count", queue)
        assert idle == expected
        # Both gauges come from the same inspect().stats() snapshot, so the idle
        # count can never exceed the queue's total process count.
        assert idle <= total


def test_idle_process_count_sibling_worker_offline(scrape_queue_metrics):
    """A worker going offline must not affect a live sibling on the same host.

    ``worker_last_seen`` is keyed by the bare hostname, so forgetting one worker
    forgets the whole host. A busy worker that is still consuming must keep
    reporting 0 idle processes rather than falling back to "everything idle".
    """
    exporter = scrape_queue_metrics(
        WorkerSpec("gen2@host-1", processes=4, active_tasks=4),
        offline=(WorkerSpec("celery@host-1", processes=4, active_tasks=4),),
    )

    assert (
        _queue_gauge(
            exporter, "celery_idle_process_count", "default", autoscaling="yes"
        )
        == 0.0
    )


def test_idle_process_count_autoscaling_label(scrape_queue_metrics):
    """The autoscaling label lets downstream processors filter on this metric alone."""
    exporter = scrape_queue_metrics(
        WorkerSpec("gen2@host-1", processes=4, active_tasks=3)
    )

    # The autoscaling label is set on the celery_idle_process_count metric.
    assert (
        exporter.registry.get_sample_value(
            "celery_idle_process_count",
            labels={"queue_name": "default", "autoscaling": "yes"},
        )
        == 1.0
    )
    # The autoscaling label is not set on the celery_active_process_count metric.
    assert (
        exporter.registry.get_sample_value(
            "celery_active_process_count",
            labels={"queue_name": "default", "autoscaling": "yes"},
        )
        is None
    )
    # The celery_active_process_count metric has a value.
    assert (
        exporter.registry.get_sample_value(
            "celery_active_process_count",
            labels={"queue_name": "default"},
        )
        == 4.0
    )


QUEUE_WAIT_TASK_NAME = "src.test_metrics.waiting_task"
QUEUE_WAIT_BASE_TIME = 1_600_000_000.0


def make_task_event(event_type, timestamp, utcoffset=None):
    return {
        "type": event_type,
        "uuid": "7d9b0b6c-6b1e-4c1e-a0f5-000000000001",
        "timestamp": timestamp,
        "local_received": timestamp,
        "hostname": "worker@wait-test-host",
        "clock": 1,
        "utcoffset": utcoffset,
    }


def make_task_sent_event(timestamp, eta=None, retries=0, utcoffset=None):
    event = make_task_event("task-sent", timestamp, utcoffset=utcoffset)
    event.update(name=QUEUE_WAIT_TASK_NAME, queue="celery", eta=eta, retries=retries)
    return event


def isoformat_eta(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def get_queue_wait_sample(exporter, suffix):
    return exporter.registry.get_sample_value(
        f"celery_task_queue_wait_time_{suffix}",
        labels={
            "name": QUEUE_WAIT_TASK_NAME,
            "hostname": "wait-test-host",
            "queue_name": "celery",
        },
    )


@pytest.mark.parametrize(
    "eta,started_offset,expected_wait",
    [
        pytest.param(None, 5, 5.0, id="plain-task"),
        pytest.param(
            isoformat_eta(QUEUE_WAIT_BASE_TIME + 120),
            123,
            3.0,
            id="future-eta-excluded",
        ),
        pytest.param(
            isoformat_eta(QUEUE_WAIT_BASE_TIME - 60),
            5,
            5.0,
            id="past-eta-ignored",
        ),
        pytest.param(None, -2, 0.0, id="clamped-to-zero-on-clock-skew"),
    ],
)
def test_queue_wait_time(event_exporter, eta, started_offset, expected_wait):
    """A task is sent at the base time (optionally with an ETA) and started
    `started_offset` seconds later; the wait is measured from max(sent, eta)
    and clamped at zero."""
    event_exporter.track_task_event(make_task_sent_event(QUEUE_WAIT_BASE_TIME, eta=eta))
    event_exporter.track_task_event(
        make_task_event("task-started", QUEUE_WAIT_BASE_TIME + started_offset)
    )

    assert get_queue_wait_sample(event_exporter, "count") == 1.0
    assert get_queue_wait_sample(event_exporter, "sum") == pytest.approx(expected_wait)


def test_queue_wait_time_measures_each_retry_delivery_without_backoff(event_exporter):
    # first delivery: waits 1s
    event_exporter.track_task_event(make_task_sent_event(QUEUE_WAIT_BASE_TIME))
    event_exporter.track_task_event(
        make_task_event("task-started", QUEUE_WAIT_BASE_TIME + 1)
    )
    event_exporter.track_task_event(
        make_task_event("task-retried", QUEUE_WAIT_BASE_TIME + 2)
    )
    # retry delivery: republished with a 30s backoff ETA, waits 3s past it
    event_exporter.track_task_event(
        make_task_sent_event(
            QUEUE_WAIT_BASE_TIME + 2,
            eta=isoformat_eta(QUEUE_WAIT_BASE_TIME + 32),
            retries=1,
        )
    )
    event_exporter.track_task_event(
        make_task_event("task-started", QUEUE_WAIT_BASE_TIME + 35)
    )

    # 1s for the first delivery plus 3s for the retry delivery
    assert get_queue_wait_sample(event_exporter, "count") == 2.0
    assert get_queue_wait_sample(event_exporter, "sum") == pytest.approx(4.0)


def test_queue_wait_time_not_observed_when_sent_event_missed(event_exporter):
    event_exporter.track_task_event(
        make_task_event("task-started", QUEUE_WAIT_BASE_TIME)
    )

    assert get_queue_wait_sample(event_exporter, "count") is None


def test_queue_wait_time_buckets_independent_of_runtime_buckets():
    exporter = Exporter(buckets=[1.0, 5.0], queue_wait_buckets=[2.0, 4.0])

    # pylint: disable=protected-access
    assert exporter.celery_task_queue_wait_time._upper_bounds == [
        2.0,
        4.0,
        float("inf"),
    ]
    assert exporter.celery_task_runtime._upper_bounds == [1.0, 5.0, float("inf")]


def test_queue_wait_time_excludes_eta_when_events_from_other_timezone(event_exporter):
    """The event receiver localises event timestamps based on the sender's
    utcoffset, while the ETA stays an absolute datetime. The ETA must be
    localised the same way, or the exclusion silently never applies when the
    producer/worker timezone differs from the exporter's."""
    source_utcoffset = utcoffset() + 4

    def localise(timestamp):
        # what Receiver.event_from_message does to event timestamps
        return adjust_timestamp(timestamp, source_utcoffset)

    event_exporter.track_task_event(
        make_task_sent_event(
            localise(QUEUE_WAIT_BASE_TIME),
            eta=isoformat_eta(QUEUE_WAIT_BASE_TIME + 120),
            utcoffset=source_utcoffset,
        )
    )
    event_exporter.track_task_event(
        make_task_event(
            "task-started",
            localise(QUEUE_WAIT_BASE_TIME + 123),
            utcoffset=source_utcoffset,
        )
    )

    assert get_queue_wait_sample(event_exporter, "count") == 1.0
    assert get_queue_wait_sample(event_exporter, "sum") == pytest.approx(3.0)
