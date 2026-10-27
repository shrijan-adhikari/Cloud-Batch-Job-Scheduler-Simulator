"""Engine invariant tests verifying Memory Conservation, Bounds, Core Exclusivity, and Queue Partitioning."""

import pytest
from simulator.job import Job, JobState, generate_workload
from simulator.engine import Engine


def assert_engine_invariants(engine: Engine) -> None:
    """Assert all 5 system invariants at the current tick."""
    cluster = engine.cluster
    all_jobs = engine.all_jobs

    # Invariant 1: Memory Conservation
    # cluster.used_memory_gb == sum(j.memory_gb for j in all_jobs if j.state in (JobState.READY, JobState.RUNNING))
    expected_memory = sum(
        j.memory_gb for j in all_jobs if j.state in (JobState.READY, JobState.RUNNING)
    )
    assert cluster.used_memory_gb == expected_memory, (
        f"Memory conservation violated at tick {engine.tick}: "
        f"cluster used={cluster.used_memory_gb}, expected={expected_memory}"
    )

    # Invariant 2: Memory Bounds
    # 0 <= cluster.used_memory_gb <= cluster.total_memory_gb
    assert 0 <= cluster.used_memory_gb <= cluster.total_memory_gb, (
        f"Memory bounds violated at tick {engine.tick}: {cluster.used_memory_gb} GB used "
        f"out of {cluster.total_memory_gb} GB total."
    )

    # Invariant 3: Core Exclusivity
    # Every non-None job ID in cluster.cores is unique, has job.state == JobState.RUNNING,
    # and job.assigned_cpu == core_index.
    busy_cores = [jid for jid in cluster.cores if jid is not None]
    assert len(busy_cores) == len(set(busy_cores)), (
        f"Duplicate job assigned to cores at tick {engine.tick}: {cluster.cores}"
    )

    for core_idx, jid in enumerate(cluster.cores):
        if jid is not None:
            job = next((j for j in all_jobs if j.id == jid), None)
            assert job is not None, f"Job {jid} on core {core_idx} not found in all_jobs"
            assert job.state == JobState.RUNNING, (
                f"Job {jid} on core {core_idx} has state {job.state}, expected RUNNING"
            )
            assert job.assigned_cpu == core_idx, (
                f"Job {jid} on core {core_idx} has assigned_cpu {job.assigned_cpu}"
            )

    # Invariant 4: Queue Partitioning
    # No job appears in more than one queue, and job.state matches the queue it resides in.
    new_ids = [j.id for j in engine.new_queue]
    ready_ids = [j.id for j in engine.ready_queue]
    running_ids = [j.id for j in engine.running_jobs]
    suspended_ids = [j.id for j in engine.suspended_queue]
    terminated_ids = [j.id for j in engine.terminated_jobs]

    all_queued = new_ids + ready_ids + running_ids + suspended_ids + terminated_ids
    assert len(all_queued) == len(set(all_queued)), (
        f"Queue partitioning violated: duplicate job IDs across queues at tick {engine.tick}"
    )

    for j in engine.new_queue:
        assert j.state == JobState.NEW
    for j in engine.ready_queue:
        assert j.state == JobState.READY
    for j in engine.running_jobs:
        assert j.state == JobState.RUNNING
    for j in engine.suspended_queue:
        assert j.state == JobState.SUSPENDED
    for j in engine.terminated_jobs:
        assert j.state == JobState.TERMINATED


@pytest.mark.parametrize(
    "lt_policy,mt_policy,st_policy",
    [
        ("memory_aware_fcfs", "suspend_lowest_priority", "round_robin"),
        ("memory_aware_fcfs", "suspend_largest_memory", "srtf"),
        ("priority_admission", "suspend_lowest_priority", "priority"),
        ("priority_admission", "suspend_largest_memory", "fcfs"),
        ("memory_aware_fcfs", "suspend_lowest_priority", "sjf"),
    ],
)
def test_engine_invariants_per_tick_50_jobs(lt_policy: str, mt_policy: str, st_policy: str) -> None:
    """Verify invariants at every single tick for a 50-job workload across policy combinations."""
    jobs = generate_workload(num_jobs=50, seed=42, total_memory_gb=64)
    cfg = {
        "cluster": {
            "num_cpus": 4,
            "total_memory_gb": 64,
            "memory_suspension_threshold": 0.85,
            "memory_resume_threshold": 0.70,
        },
        "schedulers": {
            "long_term_policy": lt_policy,
            "medium_term_policy": mt_policy,
            "short_term_policy": st_policy,
            "time_quantum": 4,
            "aging_interval_ticks": 6,
            "min_residency_ticks": 3,
            "max_swaps_per_tick": 1,
        },
    }
    engine = Engine(cfg, jobs)

    while not engine.finished and engine.tick < 3000:
        engine.step_tick()
        assert_engine_invariants(engine)

    # Invariant 5: No Deadlocks / All jobs terminate
    assert engine.finished is True
    assert all(j.state == JobState.TERMINATED for j in engine.all_jobs)


def test_engine_invariants_100_jobs_large_run() -> None:
    """Verify 100-job engine run invariants and completion without deadlocks."""
    jobs = generate_workload(num_jobs=100, seed=123, total_memory_gb=64)
    cfg = {
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
    }
    engine = Engine(cfg, jobs)

    while not engine.finished and engine.tick < 5000:
        engine.step_tick()
        assert_engine_invariants(engine)

    assert engine.finished is True
    assert len(engine.terminated_jobs) == 100


def test_dynamic_submit_and_cancel_invariants() -> None:
    """Verify invariants hold when jobs are submitted and cancelled dynamically during simulation."""
    jobs = generate_workload(num_jobs=20, seed=77, total_memory_gb=64)
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
            "short_term_policy": "priority",
            "time_quantum": 3,
            "aging_interval_ticks": 5,
            "min_residency_ticks": 2,
            "max_swaps_per_tick": 1,
        },
    }
    engine = Engine(cfg, jobs)

    # Run 5 ticks
    for _ in range(5):
        engine.step_tick()
        assert_engine_invariants(engine)

    # Dynamically submit a job
    new_job = engine.submit("urgent-render", "alice", burst_time=6, memory_gb=8, priority=1)
    assert new_job.id == engine.next_job_id - 1

    engine.step_tick()
    assert_engine_invariants(engine)

    # Cancel a job that might be in ready or running
    if engine.ready_queue:
        victim_id = engine.ready_queue[0].id
        cancelled = engine.cancel(victim_id)
        assert cancelled is True

    engine.run_to_completion(max_ticks=2000)
    assert engine.finished is True
    assert_engine_invariants(engine)
