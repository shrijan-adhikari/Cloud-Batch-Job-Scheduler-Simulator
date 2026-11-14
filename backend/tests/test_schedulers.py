"""Tests for Long-Term, Medium-Term, and Short-Term schedulers."""

import pytest
from simulator.cluster import Cluster
from simulator.job import Job, JobState
from simulator.long_term import admit
from simulator.medium_term import swap_in, swap_out
from simulator.short_term import age, quantum_preempt, pick_next, find_preemption


def make_job(
    job_id: int,
    burst: int = 5,
    mem: int = 8,
    prio: int = 3,
    arrival: int = 0,
    user: str = "alice",
    name: str = "job",
) -> Job:
    """Helper to construct a Job with preset attributes."""
    return Job(
        id=job_id,
        name=f"{name}-{job_id}",
        user=user,
        arrival_time=arrival,
        burst_time=burst,
        memory_gb=mem,
        base_priority=prio,
    )


# Long-Term Admission Tests
def test_long_term_fcfs_admission():
    """Test memory-aware FCFS admission adheres to arrival order and memory limits."""
    cluster = Cluster(num_cpus=2, total_memory_gb=20)
    jobs = [
        make_job(101, mem=12, arrival=0),
        make_job(102, mem=10, arrival=1),
        make_job(103, mem=6, arrival=2),
    ]

    admitted = admit(jobs, cluster, policy="memory_aware_fcfs", now=0)
    # 101 fits (12 <= 20). 102 (10 GB) cannot fit (12+10 > 20). 103 (6 GB) fits (12+6 <= 20).
    assert [j.id for j in admitted] == [101, 103]
    assert cluster.used_memory_gb == 18
    assert len(jobs) == 1
    assert jobs[0].id == 102
    assert jobs[0].state == JobState.NEW


def test_long_term_priority_admission():
    """Test priority admission prioritizes higher priority jobs (lower number)."""
    cluster = Cluster(num_cpus=2, total_memory_gb=20)
    jobs = [
        make_job(101, mem=10, prio=4, arrival=0),
        make_job(102, mem=10, prio=1, arrival=1),
        make_job(103, mem=8, prio=2, arrival=2),
    ]

    admitted = admit(jobs, cluster, policy="priority_admission", now=0)
    # Priority order: 102 (prio 1), 103 (prio 2), 101 (prio 4)
    # 102 fits (10 GB), 103 fits (10+8=18 GB), 101 cannot fit (18+10 > 20)
    assert [j.id for j in admitted] == [102, 103]
    assert cluster.used_memory_gb == 18
    assert [j.id for j in jobs] == [101]


# Medium-Term Scheduler Tests
def test_medium_term_swap_in():
    """Test swap-in resumes highest priority suspended job when utilization allows."""
    cluster = Cluster(num_cpus=2, total_memory_gb=64, suspension_threshold=0.85, resume_threshold=0.70)
    # cluster.memory_utilization = 0.0 < 0.70
    suspended = [
        make_job(101, mem=16, prio=3, arrival=0),
        make_job(102, mem=16, prio=1, arrival=1),
    ]
    for j in suspended:
        j.state = JobState.SUSPENDED

    resumed = swap_in(suspended, cluster, now=5)
    # 102 has highest priority (1 < 3) and should be swapped in first
    assert len(resumed) >= 1
    assert resumed[0].id == 102
    assert resumed[0].state == JobState.READY
    assert resumed[0].last_resumed_time == 5


def test_medium_term_swap_out_trigger_a_memory_pressure():
    """Test swap-out under Trigger A memory pressure."""
    cluster = Cluster(num_cpus=2, total_memory_gb=100, suspension_threshold=0.85, resume_threshold=0.70)
    # Allocate 90 GB (90% > 85% threshold)
    j_alloc = make_job(999, mem=90)
    cluster.allocate(j_alloc)

    ready = [
        make_job(101, mem=20, prio=4, arrival=0),
        make_job(102, mem=30, prio=5, arrival=1),
    ]
    for j in ready:
        j.state = JobState.READY
        j.last_resumed_time = 0

    running = []
    # Blocked job needing 20 GB when only 10 GB free
    new_queue = [make_job(201, mem=20, prio=2, arrival=5)]

    cfg = {"max_swaps_per_tick": 1, "min_residency_ticks": 3, "long_term_policy": "memory_aware_fcfs"}
    victims = swap_out(ready, running, new_queue, cluster, "suspend_lowest_priority", now=5, cfg=cfg)

    assert len(victims) == 1
    # suspend_lowest_priority maximizes (priority, memory, id) -> 102 has prio 5 > prio 4
    assert victims[0].id == 102
    assert victims[0].state == JobState.SUSPENDED
    assert victims[0].suspension_count == 1


def test_medium_term_swap_out_anti_thrashing_residency():
    """Test that jobs with residency below min_residency_ticks are not swapped out."""
    cluster = Cluster(num_cpus=2, total_memory_gb=100, suspension_threshold=0.85)
    j_alloc = make_job(999, mem=90)
    cluster.allocate(j_alloc)

    ready = [make_job(101, mem=20, prio=5, arrival=0)]
    ready[0].last_resumed_time = 4  # Resumed at tick 4, current is 5 -> delta 1 < min_residency 3

    new_queue = [make_job(201, mem=20, prio=2, arrival=5)]
    cfg = {"max_swaps_per_tick": 1, "min_residency_ticks": 3}
    victims = swap_out(ready, [], new_queue, cluster, "suspend_lowest_priority", now=5, cfg=cfg)

    # Protected by anti-thrashing min residency rule
    assert len(victims) == 0


# Short-Term Dispatcher Tests
def test_short_term_fcfs_and_sjf():
    """Test FCFS and SJF pick_next selection keys."""
    jobs = [
        make_job(101, burst=8, arrival=0),
        make_job(102, burst=3, arrival=1),
        make_job(103, burst=5, arrival=2),
    ]
    # FCFS picks first in ready_queue
    assert pick_next(jobs, "fcfs").id == 101

    # SJF picks smallest burst time (102 has burst 3)
    assert pick_next(jobs, "sjf").id == 102


def test_short_term_srtf_preemption():
    """Test SRTF preemption condition and tie-breaking."""
    ready = [make_job(101, burst=2, arrival=3)]
    running = [
        make_job(102, burst=10, arrival=0),
        make_job(103, burst=6, arrival=1),
    ]
    # Assign CPUs to running jobs
    running[0].assigned_cpu = 0
    running[1].assigned_cpu = 1
    running[0].remaining_time = 7
    running[1].remaining_time = 4

    pair = find_preemption(ready, running, "srtf")
    assert pair is not None
    better, worst = pair
    assert better.id == 101  # remaining 2
    assert worst.id == 102   # worst running has remaining 7 > 4


def test_short_term_priority_aging():
    """Test priority aging decreases priority number after interval ticks."""
    ready = [make_job(101, prio=4), make_job(102, prio=1)]
    # Job 102 is already prio 1 (highest), cannot be boosted further

    # Age 6 ticks
    for _ in range(5):
        age(ready, interval=6)
    assert ready[0].priority == 4

    # 6th tick triggers boost
    boosted = age(ready, interval=6)
    assert len(boosted) == 1
    assert boosted[0].id == 101
    assert ready[0].priority == 3
    assert ready[0].age_counter == 0
    assert ready[1].priority == 1  # Cannot boost past 1


def test_short_term_round_robin_quantum():
    """Test Round Robin quantum preemption detection."""
    running = [
        make_job(101, burst=10),
        make_job(102, burst=10),
    ]
    running[0].quantum_used = 4
    running[1].quantum_used = 2

    preempted = quantum_preempt(running, quantum=4)
    assert [j.id for j in preempted] == [101]
