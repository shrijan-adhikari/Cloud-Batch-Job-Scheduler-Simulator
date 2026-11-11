"""Tests for Section 6 formulas, hand-verified metrics calculations, and benchmark speed."""

import time
import pytest
from simulator.cluster import Cluster
from simulator.job import Job, JobState, generate_workload
from metrics.calculator import compute_metrics
from metrics.benchmark import run_benchmark


def test_hand_calculated_formulas():
    """Verify metrics calculation against exact hand-calculated expected values."""
    # Cluster setup: 2 CPUs, 32 GB RAM
    cluster = Cluster(num_cpus=2, total_memory_gb=32)

    # Job 1: arrived 0, burst 4, first run 0, finish 4
    j1 = Job(id=1, name="j1", user="u1", arrival_time=0, burst_time=4, memory_gb=8, base_priority=1)
    j1.state = JobState.TERMINATED
    j1.first_run_time = 0
    j1.finish_time = 4
    j1.suspension_count = 0

    # Job 2: arrived 1, burst 2, first run 4, finish 6
    j2 = Job(id=2, name="j2", user="u2", arrival_time=1, burst_time=2, memory_gb=8, base_priority=2)
    j2.state = JobState.TERMINATED
    j2.first_run_time = 4
    j2.finish_time = 6
    j2.suspension_count = 1

    # TAT:
    # j1: 4 - 0 = 4
    # j2: 6 - 1 = 5
    # avg_tat = (4 + 5) / 2 = 4.5

    # WT = TAT - burst:
    # j1: 4 - 4 = 0
    # j2: 5 - 2 = 3
    # avg_wt = (0 + 3) / 2 = 1.5

    # RT = first_run - arrival:
    # j1: 0 - 0 = 0
    # j2: 4 - 1 = 3
    # avg_rt = (0 + 3) / 2 = 1.5

    # Starvation threshold: max(3 * 1.5 = 4.5, 10.0) = 10.0
    # neither WT (0, 3) > 10.0 -> starvation_count = 0

    # Cluster history over 6 ticks:
    # Ticks: 6
    # Busy cores: [1, 1, 1, 1, 1, 1] -> total busy = 6
    # CPU util: 100 * (6 / (2 * 6)) = 50.0%
    cluster.busy_history = [1, 1, 1, 1, 1, 1]

    # Memory history: [8, 8, 8, 8, 8, 8] -> mean 8
    # Mem util: 100 * (8 / 32) = 25.0%
    cluster.memory_history = [8, 8, 8, 8, 8, 8]
    cluster.queue_length_history = [1, 2, 1, 0, 0, 0]

    # Throughput: (2 / 6) * 100 = 33.33 jobs / 100 ticks

    metrics = compute_metrics([j1, j2], cluster, total_ticks=6)

    assert metrics["avg_turnaround_time"] == 4.5
    assert metrics["avg_waiting_time"] == 1.5
    assert metrics["avg_response_time"] == 1.5
    assert metrics["cpu_utilization"] == 50.0
    assert metrics["memory_utilization"] == 25.0
    assert metrics["throughput"] == 33.33
    assert metrics["starvation_count"] == 0
    assert metrics["suspensions"] == 1
    assert metrics["jobs_completed"] == 2
    assert metrics["total_ticks"] == 6


def test_starvation_count_trigger():
    """Verify starvation count when a job's waiting time exceeds max(3 * avg_wt, 10)."""
    cluster = Cluster(num_cpus=1, total_memory_gb=32)
    # Job 1: WT = 1
    j1 = Job(id=1, name="j1", user="u1", arrival_time=0, burst_time=5, memory_gb=8, base_priority=1)
    j1.state = JobState.TERMINATED
    j1.first_run_time = 0
    j1.finish_time = 6  # TAT=6, WT=1

    # Job 2: Starved job with WT = 15
    j2 = Job(id=2, name="j2", user="u2", arrival_time=0, burst_time=2, memory_gb=8, base_priority=5)
    j2.state = JobState.TERMINATED
    j2.first_run_time = 15
    j2.finish_time = 17  # TAT=17, WT=15

    # avg_wt = (1 + 15) / 2 = 8.0
    # threshold = max(3 * 8.0 = 24.0, 10.0) = 24.0 -> neither > 24
    # But if we have 4 quick jobs and 1 delayed job:
    jobs = [
        Job(id=1, name="j1", user="u", arrival_time=0, burst_time=2, memory_gb=4, base_priority=1),
        Job(id=2, name="j2", user="u", arrival_time=0, burst_time=2, memory_gb=4, base_priority=1),
        Job(id=3, name="j3", user="u", arrival_time=0, burst_time=2, memory_gb=4, base_priority=1),
        Job(id=4, name="j4", user="u", arrival_time=0, burst_time=2, memory_gb=4, base_priority=1),
        Job(id=5, name="j5", user="u", arrival_time=0, burst_time=2, memory_gb=4, base_priority=5),
    ]
    for idx, j in enumerate(jobs[:4]):
        j.state = JobState.TERMINATED
        j.first_run_time = idx * 2
        j.finish_time = (idx + 1) * 2  # WT = idx * 2 (0, 2, 4, 6)
    # j5 waited 20 ticks:
    jobs[4].state = JobState.TERMINATED
    jobs[4].first_run_time = 20
    jobs[4].finish_time = 22  # WT = 20

    # WTs: [0, 2, 4, 6, 20] -> sum = 32, avg_wt = 6.4
    # threshold: max(3 * 6.4 = 19.2, 10.0) = 19.2
    # Job 5 WT = 20 > 19.2!
    metrics = compute_metrics(jobs, cluster, total_ticks=22)
    assert metrics["starvation_count"] == 1


def test_benchmark_performance_under_200ms():
    """Verify that a 100-job 5-policy benchmark completes in under 200 ms."""
    config = {
        "cluster": {
            "num_cpus": 4,
            "total_memory_gb": 64,
            "memory_suspension_threshold": 0.85,
            "memory_resume_threshold": 0.70,
        },
        "schedulers": {
            "time_quantum": 4,
            "aging_interval_ticks": 6,
            "min_residency_ticks": 3,
            "max_swaps_per_tick": 1,
        },
        "workload": {"num_jobs": 100, "random_seed": 42},
    }
    jobs = generate_workload(100, 42, 64)

    start_time = time.perf_counter()
    results = run_benchmark(config, jobs=jobs)
    elapsed_ms = (time.perf_counter() - start_time) * 1000.0

    assert set(results.keys()) == {"fcfs", "sjf", "srtf", "round_robin", "priority"}
    for pol, m in results.items():
        assert m["jobs_completed"] == 100
        assert m["total_ticks"] > 0
        assert m["cpu_utilization"] > 0.0

    print(f"\n100-job 5-policy benchmark execution time: {elapsed_ms:.2f} ms")
    assert elapsed_ms < 200.0, f"Benchmark took {elapsed_ms:.2f} ms, exceeding 200 ms target!"
