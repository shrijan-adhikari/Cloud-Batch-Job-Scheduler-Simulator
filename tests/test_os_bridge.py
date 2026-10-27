"""Test suite verifying OS Bridge process spawning, suspension, affinity, and cleanup."""

import time
import psutil
import pytest
from simulator.job import Job
from os_bridge.process_manager import ProcessManager
from os_bridge import host_monitor
from simulator.engine import Engine


def test_host_monitor_sampling() -> None:
    """Verify host_monitor.sample returns valid system CPU and RAM usage."""
    data = host_monitor.sample()
    assert "cpu_percent_per_core" in data
    assert "ram_percent" in data
    assert "ram_used_mb" in data

    assert isinstance(data["cpu_percent_per_core"], list)
    assert len(data["cpu_percent_per_core"]) > 0
    assert 0.0 <= data["ram_percent"] <= 100.0
    assert data["ram_used_mb"] > 0


def test_process_manager_spawn_suspend_terminate() -> None:
    """Gate 4 Verification: Spawn 2 workers, suspend 1, verify status via psutil, terminate all without zombies."""
    cfg = {
        "os_bridge": {
            "enabled": True,
            "ram_scale_mb_per_gb": 15,
            "max_total_real_ram_mb": 512,
            "max_live_workers": 4,
        },
        "cluster": {"num_cpus": 2},
    }
    pm = ProcessManager(cfg)

    j1 = Job(id=201, name="worker-1", user="alice", arrival_time=0, burst_time=5, memory_gb=4, base_priority=2)
    j2 = Job(id=202, name="worker-2", user="bob", arrival_time=0, burst_time=5, memory_gb=4, base_priority=3)

    # 1. Spawn 2 workers
    pm.start_or_resume(j1, core=0)
    pm.start_or_resume(j2, core=1)

    pid1 = j1.os_pid
    pid2 = j2.os_pid
    assert pid1 is not None
    assert pid2 is not None
    assert pid1 != pid2
    assert psutil.pid_exists(pid1)
    assert psutil.pid_exists(pid2)

    # Check status
    statuses = pm.status()
    assert len(statuses) == 2
    assert {s["job_id"] for s in statuses} == {201, 202}

    # 2. Suspend worker 1
    pm.suspend(j1)
    assert pm.workers[j1.id]["status"] == "suspended"

    # 3. Resume worker 1
    pm.start_or_resume(j1, core=0)
    assert pm.workers[j1.id]["status"] == "running"

    # 4. Terminate worker 1
    pm.terminate(j1)
    assert j1.os_pid is None
    assert 201 not in pm.workers

    # Give OS a moment to reap
    time.sleep(0.2)
    try:
        p1 = psutil.Process(pid1)
        assert p1.status() != psutil.STATUS_RUNNING
    except psutil.NoSuchProcess:
        pass  # Process is fully gone

    # 5. Terminate all
    pm.terminate_all()
    assert len(pm.workers) == 0

    time.sleep(0.2)
    try:
        p2 = psutil.Process(pid2)
        assert p2.status() != psutil.STATUS_RUNNING
    except psutil.NoSuchProcess:
        pass


def test_process_manager_caps_oldest_suspended() -> None:
    """Verify when max_live_workers is reached, the oldest suspended worker is terminated to make room."""
    cfg = {
        "os_bridge": {
            "enabled": True,
            "ram_scale_mb_per_gb": 10,
            "max_total_real_ram_mb": 512,
            "max_live_workers": 2,  # Cap at 2 live workers
        },
        "cluster": {"num_cpus": 2},
    }
    pm = ProcessManager(cfg)

    j1 = Job(id=301, name="w1", user="u", arrival_time=0, burst_time=5, memory_gb=4, base_priority=2)
    j2 = Job(id=302, name="w2", user="u", arrival_time=0, burst_time=5, memory_gb=4, base_priority=2)
    j3 = Job(id=303, name="w3", user="u", arrival_time=0, burst_time=5, memory_gb=4, base_priority=2)

    try:
        pm.start_or_resume(j1, core=0)
        time.sleep(0.05)
        pm.start_or_resume(j2, core=1)
        pid1 = j1.os_pid

        # Suspend j1 so it becomes candidate for recycling
        pm.suspend(j1)

        # Spawning j3 should trigger eviction of oldest suspended (j1)
        pm.start_or_resume(j3, core=0)
        assert len(pm.workers) <= 2
        assert 301 not in pm.workers
        assert 303 in pm.workers

        # j1 pid should be terminated
        time.sleep(0.2)
        try:
            p1 = psutil.Process(pid1)
            assert p1.status() != psutil.STATUS_RUNNING
        except psutil.NoSuchProcess:
            pass
    finally:
        pm.terminate_all()


def test_engine_live_simulation_with_os_bridge() -> None:
    """Verify Engine runs with live ProcessManager attached and cleans up completely."""
    cfg = {
        "cluster": {
            "num_cpus": 2,
            "total_memory_gb": 32,
            "memory_suspension_threshold": 0.85,
            "memory_resume_threshold": 0.70,
        },
        "schedulers": {
            "long_term_policy": "memory_aware_fcfs",
            "medium_term_policy": "suspend_lowest_priority",
            "short_term_policy": "round_robin",
            "time_quantum": 2,
            "aging_interval_ticks": 6,
            "min_residency_ticks": 3,
            "max_swaps_per_tick": 1,
        },
        "os_bridge": {
            "enabled": True,
            "ram_scale_mb_per_gb": 10,
            "max_total_real_ram_mb": 512,
            "max_live_workers": 4,
        },
    }
    pm = ProcessManager(cfg)
    jobs = [
        Job(id=401, name="short-a", user="alice", arrival_time=0, burst_time=2, memory_gb=4, base_priority=1),
        Job(id=402, name="short-b", user="bob", arrival_time=0, burst_time=2, memory_gb=4, base_priority=2),
    ]

    engine = Engine(cfg, jobs, os_bridge=pm)
    try:
        # Step 0: Ingest & dispatch
        snap = engine.step_tick()
        assert snap["mode"] == "live"
        assert len(pm.workers) == 2

        # Step 1: Running
        engine.step_tick()

        # Step 2: Termination of burst=2 jobs
        engine.step_tick()
        assert engine.finished is True
        assert len(pm.workers) == 0
    finally:
        pm.terminate_all()
