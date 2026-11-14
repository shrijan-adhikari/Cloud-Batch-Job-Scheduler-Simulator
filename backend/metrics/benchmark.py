"""Benchmark harness evaluating all five short-term scheduling policies across identical workloads."""

import copy
from typing import Optional
from simulator.engine import Engine
from simulator.job import Job, clone_jobs, generate_workload


def run_benchmark(config: dict, jobs: Optional[list[Job]] = None) -> dict[str, dict]:
    """Run all five short-term scheduling policies across identical workloads in pure simulation."""
    if jobs is None:
        workload_cfg = config.get("workload", {})
        cluster_cfg = config.get("cluster", {})
        num_jobs = workload_cfg.get("num_jobs", 50)
        seed = workload_cfg.get("random_seed", 42)
        total_mem = cluster_cfg.get("total_memory_gb", 64)
        base_jobs = generate_workload(num_jobs, seed, total_mem)
    else:
        base_jobs = jobs

    policies = ["fcfs", "sjf", "srtf", "round_robin", "priority"]
    results: dict[str, dict] = {}

    for policy in policies:
        # Create a deep configuration copy with the targeted policy and disabled OS bridge
        cfg = copy.deepcopy(config)
        cfg.setdefault("schedulers", {})["short_term_policy"] = policy
        cfg.setdefault("os_bridge", {})["enabled"] = False

        engine_jobs = clone_jobs(base_jobs)
        engine = Engine(config=cfg, jobs=engine_jobs, os_bridge=None)
        engine.run_to_completion()

        results[policy] = engine.snapshot()["metrics"]

    return results
