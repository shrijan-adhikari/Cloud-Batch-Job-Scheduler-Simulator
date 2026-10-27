"""Unit tests for Core Data Models, Admission, Medium-Term, and Short-Term Schedulers."""

import pytest
from simulator.job import Job, JobState, generate_workload, load_workload, clone_jobs
from simulator.cluster import Cluster
from simulator import long_term, medium_term, short_term
from simulator.engine import Engine


def test_job_dataclass_fields() -> None:
    """Verify Job dataclass initialization, post_init, and to_dict."""
    job = Job(
        id=101,
        name="test-job",
        user="alice",
        arrival_time=0,
        burst_time=5,
        memory_gb=8,
        base_priority=3,
    )
    assert job.priority == 3
    assert job.remaining_time == 5
    assert job.state == JobState.NEW
    assert job.assigned_cpu is None
    assert job.os_pid is None
    assert job.admitted_time is None
    assert job.first_run_time is None
    assert job.finish_time is None
    assert job.quantum_used == 0
    assert job.ready_wait_ticks == 0
    assert job.age_counter == 0
    assert job.suspension_count == 0
    assert job.cancelled is False
    assert job.last_resumed_time is None

    d = job.to_dict()
    assert d["id"] == 101
    assert d["state"] == "NEW"
    assert d["priority"] == 3


def test_workload_generator_determinism() -> None:
    """Verify generate_workload is deterministic and respects constraints."""
    w1 = generate_workload(num_jobs=25, seed=42, total_memory_gb=64)
    w2 = generate_workload(num_jobs=25, seed=42, total_memory_gb=64)
    w3 = generate_workload(num_jobs=25, seed=99, total_memory_gb=64)

    assert len(w1) == 25
    for j1, j2 in zip(w1, w2):
        assert j1.id == j2.id
        assert j1.name == j2.name
        assert j1.user == j2.user
        assert j1.arrival_time == j2.arrival_time
        assert j1.burst_time == j2.burst_time
        assert j1.memory_gb == j2.memory_gb
        assert j1.base_priority == j2.base_priority

    # Different seeds should yield different burst or arrival sequences
    assert [j.burst_time for j in w1] != [j.burst_time for j in w3]

    # Memory constraint: memory_gb <= total_memory_gb // 2
    for j in w1:
        assert 1 <= j.memory_gb <= 32
        assert 1 <= j.base_priority <= 5
        assert j.burst_time >= 1


def test_load_and_clone_jobs() -> None:
    """Verify load_workload from JSON and clone_jobs state reset."""
    golden = load_workload("tests/golden_workload.json")
    assert len(golden) == 3
    assert golden[0].id == 101
    assert golden[1].id == 102
    assert golden[2].id == 103

    golden[0].state = JobState.RUNNING
    golden[0].remaining_time = 1
    golden[0].assigned_cpu = 0

    cloned = clone_jobs(golden)
    assert cloned[0].state == JobState.NEW
    assert cloned[0].remaining_time == golden[0].burst_time
    assert cloned[0].assigned_cpu is None


def test_cluster_resource_tracking() -> None:
    """Verify Cluster RAM and core operations."""
    cluster = Cluster(num_cpus=2, total_memory_gb=32, suspension_threshold=0.85, resume_threshold=0.70)
    assert cluster.available_memory_gb == 32
    assert cluster.memory_utilization == 0.0
    assert cluster.free_cores() == [0, 1]

    j1 = Job(id=1, name="j1", user="u", arrival_time=0, burst_time=4, memory_gb=16, base_priority=2)
    assert cluster.can_fit(j1)
    cluster.allocate(j1)
    assert cluster.used_memory_gb == 16
    assert cluster.available_memory_gb == 16
    assert cluster.memory_utilization == 0.5

    cluster.assign(j1, 0)
    assert cluster.cores == [1, None]
    assert cluster.free_cores() == [1]

    # Assigning to occupied core must raise ValueError
    with pytest.raises(ValueError):
        cluster.assign(j1, 0)

    # Exceeding memory must raise ValueError
    j_too_big = Job(id=2, name="big", user="u", arrival_time=0, burst_time=4, memory_gb=20, base_priority=2)
    with pytest.raises(ValueError):
        cluster.allocate(j_too_big)

    cluster.vacate(0)
    assert cluster.cores == [None, None]
    cluster.release(j1)
    assert cluster.used_memory_gb == 0


def test_long_term_admission_policies() -> None:
    """Test memory_aware_fcfs and priority_admission."""
    cluster = Cluster(num_cpus=2, total_memory_gb=20)
    j1 = Job(id=101, name="j1", user="u", arrival_time=0, burst_time=4, memory_gb=12, base_priority=4)
    j2 = Job(id=102, name="j2", user="u", arrival_time=1, burst_time=4, memory_gb=8, base_priority=1)

    # Test priority_admission: j2 (prio=1) should be admitted before j1 (prio=4)
    new_q = [j1, j2]
    admitted = long_term.admit(new_q, cluster, "priority_admission", now=0)
    assert [j.id for j in admitted] == [102, 101]
    assert cluster.used_memory_gb == 20
    assert len(new_q) == 0

    # Reset
    cluster.release(j1)
    cluster.release(j2)
    j1.state = JobState.NEW
    j2.state = JobState.NEW

    # Test memory_aware_fcfs: j1 arrives at t=0, j2 at t=1
    new_q = [j1, j2]
    admitted = long_term.admit(new_q, cluster, "memory_aware_fcfs", now=0)
    assert [j.id for j in admitted] == [101, 102]
    assert cluster.used_memory_gb == 20


def test_medium_term_hysteresis_and_anti_thrash() -> None:
    """Test swap_in / swap_out hysteresis and min_residency_ticks."""
    cluster = Cluster(num_cpus=2, total_memory_gb=100, suspension_threshold=0.85, resume_threshold=0.70)
    # Cluster has 80 GB used
    j_running = Job(id=1, name="r", user="u", arrival_time=0, burst_time=10, memory_gb=80, base_priority=2)
    cluster.allocate(j_running)

    j_suspended = Job(id=2, name="s", user="u", arrival_time=0, burst_time=5, memory_gb=10, base_priority=3)
    j_suspended.state = JobState.SUSPENDED
    suspended_q = [j_suspended]

    # Memory utilization is 80% > 70% resume threshold: swap_in should NOT happen
    resumed = medium_term.swap_in(suspended_q, cluster, now=10)
    assert len(resumed) == 0

    # Free memory to 60 GB used (60% < 70%)
    cluster.release(j_running)
    j_running.memory_gb = 60
    cluster.allocate(j_running)
    assert cluster.memory_utilization == 0.60

    # Now swap_in should succeed: 60 + 10 = 70 GB <= 85 GB
    resumed = medium_term.swap_in(suspended_q, cluster, now=10)
    assert len(resumed) == 1
    assert resumed[0].id == 2
    assert resumed[0].last_resumed_time == 10
    assert resumed[0].state == JobState.READY

    # Anti-thrash test: resumed job at now=10 cannot be swapped out if now=11 and min_residency=3
    ready_q = [resumed[0]]
    j_blocked = Job(id=3, name="b", user="u", arrival_time=11, burst_time=5, memory_gb=50, base_priority=1)
    cfg = {"min_residency_ticks": 3, "memory_suspension_threshold": 0.85, "max_swaps_per_tick": 1}

    # Set RAM above 85%
    j_filler = Job(id=4, name="f", user="u", arrival_time=0, burst_time=5, memory_gb=20, base_priority=3)
    cluster.allocate(j_filler)
    # now 60 + 10 + 20 = 90 GB > 85%
    victims = medium_term.swap_out(ready_q, [], [j_blocked], cluster, "suspend_lowest_priority", now=11, cfg=cfg)
    # resumed[0] was resumed at 10, now=11 -> elapsed 1 < 3 -> exempt from being swapped out!
    assert len(victims) == 0

    # At now=13 (elapsed 3 >= 3): it can now be swapped out
    victims = medium_term.swap_out(ready_q, [], [j_blocked], cluster, "suspend_lowest_priority", now=13, cfg=cfg)
    assert len(victims) == 1
    assert victims[0].id == 2
    assert victims[0].state == JobState.SUSPENDED


def test_priority_aging_and_persistence() -> None:
    """Test priority aging and persistence across state transitions."""
    j1 = Job(id=1, name="j1", user="u", arrival_time=0, burst_time=10, memory_gb=8, base_priority=4)
    ready = [j1]

    # Aging interval = 3
    short_term.age(ready, interval=3)  # counter = 1
    assert j1.priority == 4
    short_term.age(ready, interval=3)  # counter = 2
    assert j1.priority == 4
    boosted = short_term.age(ready, interval=3)  # counter = 3 -> priority becomes 3!
    assert len(boosted) == 1
    assert j1.priority == 3
    assert j1.age_counter == 0

    # Priority capped at 1
    j1.priority = 1
    j1.age_counter = 2
    boosted = short_term.age(ready, interval=3)
    assert len(boosted) == 0
    assert j1.priority == 1


def test_all_five_golden_policies() -> None:
    """Verify all 5 policies against the hand-verified numbers in docs/TESTING.md."""
    golden_jobs = load_workload("tests/golden_workload.json")

    # Policy 1: FCFS
    cfg_fcfs = {
        "cluster": {"num_cpus": 1, "total_memory_gb": 64, "memory_suspension_threshold": 0.85, "memory_resume_threshold": 0.70},
        "schedulers": {"long_term_policy": "memory_aware_fcfs", "medium_term_policy": "suspend_lowest_priority", "short_term_policy": "fcfs", "time_quantum": 2, "aging_interval_ticks": 100, "min_residency_ticks": 3, "max_swaps_per_tick": 1}
    }
    eng_fcfs = Engine(cfg_fcfs, golden_jobs)
    eng_fcfs.run_to_completion()
    assert eng_fcfs.cluster.gantt[0][:11] == [101, 101, 101, 101, 101, 102, 102, 103, 103, 103, 103]
    m_fcfs = eng_fcfs.compute_current_metrics()
    assert m_fcfs["avg_waiting_time"] == 3.0
    assert m_fcfs["avg_turnaround_time"] == 6.67
    assert m_fcfs["avg_response_time"] == 3.0
    assert m_fcfs["total_ticks"] == 11
    assert m_fcfs["cpu_utilization"] == 100.0

    # Policy 2: SJF (staggered arrivals)
    cfg_sjf = {**cfg_fcfs, "schedulers": {**cfg_fcfs["schedulers"], "short_term_policy": "sjf"}}
    eng_sjf = Engine(cfg_sjf, golden_jobs)
    eng_sjf.run_to_completion()
    assert eng_sjf.cluster.gantt[0][:11] == [101, 101, 101, 101, 101, 102, 102, 103, 103, 103, 103]
    m_sjf = eng_sjf.compute_current_metrics()
    assert m_sjf["avg_waiting_time"] == 3.0

    # Policy 2b: SJF with simultaneous arrivals at t=0: order 102 -> 103 -> 101
    simultaneous_jobs = [
        Job(id=101, name="a", user="u", arrival_time=0, burst_time=5, memory_gb=8, base_priority=3),
        Job(id=102, name="b", user="u", arrival_time=0, burst_time=2, memory_gb=8, base_priority=1),
        Job(id=103, name="c", user="u", arrival_time=0, burst_time=4, memory_gb=8, base_priority=2),
    ]
    eng_sjf0 = Engine(cfg_sjf, simultaneous_jobs)
    eng_sjf0.run_to_completion()
    assert eng_sjf0.cluster.gantt[0][:11] == [102, 102, 103, 103, 103, 103, 101, 101, 101, 101, 101]
    m_sjf0 = eng_sjf0.compute_current_metrics()
    assert m_sjf0["avg_waiting_time"] == 2.67

    # Policy 3: SRTF
    cfg_srtf = {**cfg_fcfs, "schedulers": {**cfg_fcfs["schedulers"], "short_term_policy": "srtf"}}
    eng_srtf = Engine(cfg_srtf, golden_jobs)
    eng_srtf.run_to_completion()
    assert eng_srtf.cluster.gantt[0][:11] == [101, 102, 102, 101, 101, 101, 101, 103, 103, 103, 103]
    m_srtf = eng_srtf.compute_current_metrics()
    assert m_srtf["avg_waiting_time"] == 2.33
    assert m_srtf["avg_turnaround_time"] == 6.0
    assert m_srtf["avg_response_time"] == 1.67

    # Policy 4: Round Robin (quantum=2)
    cfg_rr = {**cfg_fcfs, "schedulers": {**cfg_fcfs["schedulers"], "short_term_policy": "round_robin", "time_quantum": 2}}
    eng_rr = Engine(cfg_rr, golden_jobs)
    eng_rr.run_to_completion()
    assert eng_rr.cluster.gantt[0][:11] == [101, 101, 102, 102, 103, 103, 101, 101, 103, 103, 101]
    m_rr = eng_rr.compute_current_metrics()
    assert m_rr["avg_waiting_time"] == 3.67
    assert m_rr["avg_turnaround_time"] == 7.33
    assert m_rr["avg_response_time"] == 1.0

    # Policy 5: Priority (preemptive, aging=100)
    cfg_prio = {**cfg_fcfs, "schedulers": {**cfg_fcfs["schedulers"], "short_term_policy": "priority", "aging_interval_ticks": 100}}
    eng_prio = Engine(cfg_prio, golden_jobs)
    eng_prio.run_to_completion()
    assert eng_prio.cluster.gantt[0][:11] == [101, 102, 102, 103, 103, 103, 103, 101, 101, 101, 101]
    m_prio = eng_prio.compute_current_metrics()
    assert m_prio["avg_waiting_time"] == 2.33
    assert m_prio["avg_turnaround_time"] == 6.0
    assert m_prio["avg_response_time"] == 0.33
