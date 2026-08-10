import logging
import time
from typing import NamedTuple

import pytest
from celery.contrib.testing.worker import start_worker  # type: ignore
from celery.utils.time import adjust_timestamp  # type: ignore

from src.exporter import Exporter, reverse_adjust_timestamp


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
    active: int | None = 0
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
            if worker.active is not None:
                exporter.track_worker_heartbeat(
                    _make_heartbeat_event(worker.name, worker.active)
                )
        for worker in offline:
            exporter.track_worker_status(
                _make_heartbeat_event(worker.name, worker.active), is_online=False
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


def _queue_gauge(exporter, metric, queue):
    return exporter.registry.get_sample_value(metric, labels={"queue_name": queue})


@pytest.mark.parametrize(
    "workers,expected_idle",
    [
        pytest.param(
            (WorkerSpec(processes=4, active=2),),
            {"default": 2.0},
            id="single-worker",
        ),
        pytest.param(
            (WorkerSpec(processes=4, active=0),),
            {"default": 4.0},
            id="no-active-tasks",
        ),
        pytest.param(
            (WorkerSpec(processes=4, active=4),),
            {"default": 0.0},
            id="fully-busy",
        ),
        # Active tasks can outnumber the pool size the scrape sees, e.g. after a
        # worker shrinks its pool. Idle is clamped at 0 rather than going negative.
        pytest.param(
            (WorkerSpec(processes=4, active=6),),
            {"default": 0.0},
            id="more-active-tasks-than-processes",
        ),
        # No heartbeat means the worker is absent from the event state, so its
        # active count reads 0, matching the worker_tasks_active default.
        pytest.param(
            (WorkerSpec(processes=3, active=None),),
            {"default": 3.0},
            id="no-heartbeat-yet",
        ),
        pytest.param(
            (
                WorkerSpec("celery@host-a", processes=4, active=1),
                WorkerSpec("celery@host-b", processes=2, active=2),
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
                WorkerSpec("celery@host-1", processes=4, active=1),
                WorkerSpec("gen2@host-1", processes=4, active=3),
            ),
            {"default": 4.0},
            id="two-workers-same-host",
        ),
        pytest.param(
            (WorkerSpec(processes=4, active=1, queues=("default", "priority")),),
            {"default": 3.0, "priority": 3.0},
            id="one-worker-two-queues",
        ),
        # gevent/eventlet/threads pools report no processes, so their configured
        # max-concurrency stands in for the pool size.
        pytest.param(
            (WorkerSpec(processes=None, max_concurrency=100, active=30),),
            {"default": 70.0},
            id="greenlet-pool-uses-max-concurrency",
        ),
    ],
)
def test_idle_process_count(scrape_queue_metrics, workers, expected_idle):
    exporter = scrape_queue_metrics(*workers)

    for queue, expected in expected_idle.items():
        idle = _queue_gauge(exporter, "celery_idle_process_count", queue)
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
        WorkerSpec("gen2@host-1", processes=4, active=4),
        offline=(WorkerSpec("celery@host-1", processes=4, active=4),),
    )

    assert _queue_gauge(exporter, "celery_idle_process_count", "default") == 0.0
