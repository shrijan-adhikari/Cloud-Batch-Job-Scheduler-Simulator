"""Metrics calculation engine implementing exact formulas from PROJECT_CONTEXT.md §6."""

from simulator.job import Job, JobState
from simulator.cluster import Cluster


def compute_metrics(jobs: list[Job], cluster: Cluster, total_ticks: int) -> dict:
    """Compute benchmark and simulation metrics across completed, non-cancelled jobs."""
    completed = [
        j for j in jobs
        if j.state == JobState.TERMINATED and not j.cancelled and j.finish_time is not None
    ]
    n = len(completed)
    t_total = max(total_ticks, 1)

    if completed:
        tats = [j.finish_time - j.arrival_time for j in completed]  # type: ignore[operator]
        wts = [tat - j.burst_time for tat, j in zip(tats, completed)]
        rts = [
            (j.first_run_time - j.arrival_time)
            for j in completed
            if j.first_run_time is not None
        ]

        avg_tat = sum(tats) / n
        avg_wt = sum(wts) / n
        avg_rt = sum(rts) / len(rts) if rts else 0.0

        starvation_thresh = max(3.0 * avg_wt, 10.0)
        starvation_count = sum(1 for wt in wts if wt > starvation_thresh)
    else:
        avg_tat = 0.0
        avg_wt = 0.0
        avg_rt = 0.0
        starvation_count = 0

    c = cluster.num_cpus
    busy_history = cluster.busy_history[:t_total]
    if c > 0 and busy_history:
        cpu_util = 100.0 * sum(busy_history) / (c * len(busy_history))
    else:
        cpu_util = 0.0

    mem_history = cluster.memory_history[:t_total]
    if mem_history and cluster.total_memory_gb > 0:
        mean_mem = sum(mem_history) / len(mem_history)
        mem_util = 100.0 * mean_mem / cluster.total_memory_gb
    else:
        mem_util = 0.0

    throughput = (n / t_total) * 100.0
    suspensions = sum(j.suspension_count for j in jobs)

    q_history = cluster.queue_length_history[:t_total]
    avg_q = (sum(q_history) / len(q_history)) if q_history else 0.0
    max_q = max(q_history, default=0)

    return {
        "avg_waiting_time": round(avg_wt, 2),
        "avg_turnaround_time": round(avg_tat, 2),
        "avg_response_time": round(avg_rt, 2),
        "cpu_utilization": round(cpu_util, 2),
        "memory_utilization": round(mem_util, 2),
        "throughput": round(throughput, 2),
        "starvation_count": starvation_count,
        "suspensions": suspensions,
        "avg_queue_length": round(avg_q, 2),
        "max_queue_length": max_q,
        "jobs_completed": n,
        "total_ticks": t_total,
    }
