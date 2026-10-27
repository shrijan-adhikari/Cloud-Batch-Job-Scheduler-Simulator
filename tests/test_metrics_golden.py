"""Test suite verifying hand-calculated golden numbers in docs/TESTING.md and benchmark latency."""

import time
import pytest
from simulator.job import Job, load_workload, generate_workload
from metrics.benchmark import run_benchmark
from simulator.engine import Engine


def test_golden_metrics_all_five_policies() -> None:
    """Verify hand-calculated golden results from docs/TESTING.md across all 5 policies."""
    golden_jobs = load_workload("tests/golden_workload.json")
    cfg = {
        "cluster": {
            "num_cpus": 1,
            "total_memory_gb": 64,
            "memory_suspension_threshold": 0.85,
            "memory_resume_threshold": 0.70,
        },
        "schedulers": {
            "long_term_policy": "memory_aware_fcfs",
            "medium_term_policy": "suspend_lowest_priority",
            "short_term_policy": "fcfs",
            "time_quantum": 2,
            "aging_interval_ticks": 100,
            "min_residency_ticks": 3,
            "max_swaps_per_tick": 1,
        },
    }

    results = run_benchmark(cfg, golden_jobs)

    # 1. FCFS
    fcfs = results["fcfs"]
    assert fcfs["avg_waiting_time"] == 3.0
    assert fcfs["avg_turnaround_time"] == 6.67
    assert fcfs["avg_response_time"] == 3.0
    assert fcfs["total_ticks"] == 11
    assert fcfs["cpu_utilization"] == 100.0
    assert fcfs["jobs_completed"] == 3

    # 2. SJF
    sjf = results["sjf"]
    assert sjf["avg_waiting_time"] == 3.0
    assert sjf["avg_turnaround_time"] == 6.67
    assert sjf["avg_response_time"] == 3.0
    assert sjf["total_ticks"] == 11
    assert sjf["cpu_utilization"] == 100.0

    # 3. SRTF
    srtf = results["srtf"]
    assert srtf["avg_waiting_time"] == 2.33
    assert srtf["avg_turnaround_time"] == 6.0
    assert srtf["avg_response_time"] == 1.67
    assert srtf["total_ticks"] == 11
    assert srtf["cpu_utilization"] == 100.0

    # 4. Round Robin (quantum=2)
    rr = results["round_robin"]
    assert rr["avg_waiting_time"] == 3.67
    assert rr["avg_turnaround_time"] == 7.33
    assert rr["avg_response_time"] == 1.0
    assert rr["total_ticks"] == 11
    assert rr["cpu_utilization"] == 100.0

    # 5. Priority (aging=100)
    prio = results["priority"]
    assert prio["avg_waiting_time"] == 2.33
    assert prio["avg_turnaround_time"] == 6.0
    assert prio["avg_response_time"] == 0.33
    assert prio["total_ticks"] == 11
    assert prio["cpu_utilization"] == 100.0


def test_sjf_simultaneous_arrivals_golden() -> None:
    """Verify SJF with simultaneous arrivals at t=0 produces WT = (0 + 2 + 6)/3 = 2.67."""
    simultaneous_jobs = [
        Job(id=101, name="a", user="u", arrival_time=0, burst_time=5, memory_gb=8, base_priority=3),
        Job(id=102, name="b", user="u", arrival_time=0, burst_time=2, memory_gb=8, base_priority=1),
        Job(id=103, name="c", user="u", arrival_time=0, burst_time=4, memory_gb=8, base_priority=2),
    ]
    cfg = {
        "cluster": {"num_cpus": 1, "total_memory_gb": 64},
        "schedulers": {
            "long_term_policy": "memory_aware_fcfs",
            "medium_term_policy": "suspend_lowest_priority",
            "short_term_policy": "sjf",
            "time_quantum": 2,
            "aging_interval_ticks": 100,
        },
    }
    eng = Engine(cfg, simultaneous_jobs)
    eng.run_to_completion()
    m = eng.compute_current_metrics()
    assert m["avg_waiting_time"] == 2.67


def test_benchmark_performance_100_jobs_under_200ms() -> None:
    """Gate 3 Verification: 100-job 5-policy benchmark completes in under 200 ms."""
    jobs_100 = generate_workload(num_jobs=100, seed=42, total_memory_gb=64)
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
            "time_quantum": 4,
            "aging_interval_ticks": 6,
            "min_residency_ticks": 3,
            "max_swaps_per_tick": 1,
        },
        "workload": {"num_jobs": 100, "random_seed": 42},
    }

    t0 = time.perf_counter()
    results = run_benchmark(cfg, jobs_100)
    elapsed = time.perf_counter() - t0

    print(f"\n100-job 5-policy benchmark elapsed time: {elapsed * 1000:.1f} ms")
    assert elapsed < 0.200, f"Benchmark exceeded 200 ms limit: {elapsed * 1000:.1f} ms"

    for pol in ["fcfs", "sjf", "srtf", "round_robin", "priority"]:
        assert pol in results
        assert results[pol]["jobs_completed"] == 100
