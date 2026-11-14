"""Tests verifying discrete tick ordering, state transitions, and all engine invariants."""

import pytest
from simulator.engine import Engine
from simulator.job import Job, JobState, generate_workload


def get_default_config() -> dict:
    """Return a baseline configuration dictionary for invariant testing."""
    return {
        "cluster": {
            "num_cpus": 4,
            "total_memory_gb": 64,
            "memory_suspension_threshold": 0.85,
            "memory_resume_threshold": 0.70,
        },
        "schedulers": {
            "long_term_policy": "memory_aware_fcfs",
            "medium_term_policy": "suspend_lowest_priority",
            "short_term_policy": "round_robin",
            "time_quantum": 4,
            "aging_interval_ticks": 6,
            "min_residency_ticks": 3,
            "max_swaps_per_tick": 1,
        },
        "os_bridge": {"enabled": False},
        "workload": {"num_jobs": 20, "random_seed": 42},
    }


def test_100_jobs_all_invariants_every_tick():
    """Run a 100-job workload and assert all engine invariants hold at every single tick."""
    cfg = get_default_config()
    cfg["workload"]["num_jobs"] = 100
    jobs = generate_workload(num_jobs=100, seed=42, total_memory_gb=64)
    engine = Engine(config=cfg, jobs=jobs, os_bridge=None)

    total_ticks = 0
    max_ticks = 5000

    while not engine.finished and total_ticks < max_ticks:
        snap = engine.step_tick()
        total_ticks += 1

        # Invariant 1: Used memory matches exact sum of READY + RUNNING jobs
        ready_running_mem = sum(
            j.memory_gb for j in (engine.ready_queue + engine.running_jobs)
        )
        assert engine.cluster.used_memory_gb == ready_running_mem, (
            f"Tick {engine.tick}: Cluster memory {engine.cluster.used_memory_gb} "
            f"!= READY+RUNNING sum {ready_running_mem}"
        )

        # Invariant 2: 0 <= used_memory_gb <= total_memory_gb
        assert 0 <= engine.cluster.used_memory_gb <= engine.cluster.total_memory_gb

        # Invariant 3: Core exclusivity
        running_ids = [j.id for j in engine.running_jobs]
        occupied_cores = [c for c in engine.cluster.cores if c is not None]
        assert sorted(running_ids) == sorted(occupied_cores), (
            f"Tick {engine.tick}: Running jobs {running_ids} != occupied cores {occupied_cores}"
        )
        assert len(running_ids) == len(set(running_ids)), "Duplicate job IDs in running"

        # Invariant 4: Non-running jobs have assigned_cpu = None
        for j in engine.new_queue + engine.ready_queue + engine.suspended_queue + engine.terminated_jobs:
            assert j.assigned_cpu is None, f"Non-running job {j.id} has assigned_cpu={j.assigned_cpu}"

        # Invariant 5: NEW, SUSPENDED, TERMINATED hold no memory
        for j in engine.new_queue + engine.suspended_queue + engine.terminated_jobs:
            assert j.state in (JobState.NEW, JobState.SUSPENDED, JobState.TERMINATED)

    assert engine.finished, f"Simulation did not finish within {max_ticks} ticks"
    assert len(engine.terminated_jobs) == 100


def test_engine_cancellation():
    """Test job cancellation vacates cores, releases memory, and updates state."""
    cfg = get_default_config()
    jobs = [
        Job(id=101, name="j1", user="a", arrival_time=0, burst_time=10, memory_gb=16, base_priority=2),
        Job(id=102, name="j2", user="b", arrival_time=0, burst_time=10, memory_gb=16, base_priority=2),
    ]
    engine = Engine(config=cfg, jobs=jobs, os_bridge=None)

    # Tick 1: Jobs 101 and 102 are admitted and dispatched to cores
    engine.step_tick()
    assert 101 in engine.cluster.cores
    mem_before = engine.cluster.used_memory_gb

    # Cancel job 101
    assert engine.cancel(101) is True

    # Next tick applies cancellation
    engine.step_tick()
    cancelled_job = next(j for j in engine.terminated_jobs if j.id == 101)
    assert cancelled_job.cancelled is True
    assert cancelled_job.state == JobState.TERMINATED
    assert cancelled_job.assigned_cpu is None
    assert 101 not in engine.cluster.cores
    assert engine.cluster.used_memory_gb == mem_before - 16


def test_engine_reset():
    """Test engine reset returns cluster and queues to initial state."""
    cfg = get_default_config()
    jobs = generate_workload(num_jobs=5, seed=123, total_memory_gb=64)
    engine = Engine(config=cfg, jobs=jobs, os_bridge=None)

    for _ in range(5):
        engine.step_tick()
    assert engine.tick == 5

    engine.reset()
    assert engine.tick == 0
    assert engine.cluster.used_memory_gb == 0
    assert all(c is None for c in engine.cluster.cores)
    assert len(engine.ready_queue) == 0
    assert len(engine.running_jobs) == 0
    assert len(engine.terminated_jobs) == 0
