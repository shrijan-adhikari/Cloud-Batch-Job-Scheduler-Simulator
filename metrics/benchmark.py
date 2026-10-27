"""Benchmark runner executing all 5 short-term scheduling policies on identical workloads."""

import copy
from typing import Optional
from simulator.job import Job, clone_jobs, generate_workload
from simulator.engine import Engine


def run_benchmark(config: dict, jobs: Optional[list[Job]] = None) -> dict[str, dict]:
    """Run all 5 short-term scheduling policies on cloned workloads and return comparative metrics."""
    if jobs is None:
        workload_cfg = config.get("workload", {})
        num_jobs = workload_cfg.get("num_jobs", 20)
        seed = workload_cfg.get("random_seed", 42)
        total_mem = config.get("cluster", {}).get("total_memory_gb", 64)
        base_jobs = generate_workload(num_jobs=num_jobs, seed=seed, total_memory_gb=total_mem)
    else:
        base_jobs = jobs

    policies = ["fcfs", "sjf", "srtf", "round_robin", "priority"]
    results: dict[str, dict] = {}

    for policy in policies:
        policy_cfg = copy.deepcopy(config)
        if "schedulers" not in policy_cfg:
            policy_cfg["schedulers"] = {}
        policy_cfg["schedulers"]["short_term_policy"] = policy
        if "os_bridge" not in policy_cfg:
            policy_cfg["os_bridge"] = {}
        policy_cfg["os_bridge"]["enabled"] = False

        cloned = clone_jobs(base_jobs)
        engine = Engine(config=policy_cfg, jobs=cloned, os_bridge=None)
        engine.run_to_completion()
        results[policy] = engine.compute_current_metrics()

    return results
