"""Performance metric calculations according to Section 6 of the authoritative specification."""

from typing import Any
from simulator.cluster import Cluster
from simulator.job import Job, JobState


def compute_metrics(jobs: list[Job], cluster: Cluster, total_ticks: int) -> dict[str, Any]:
    """Compute cluster performance metrics across all completed non-cancelled jobs."""
    completed_jobs = [
        j for j in jobs
        if j.state == JobState.TERMINATED and not j.cancelled and j.finish_time is not None
    ]
    num_completed = len(completed_jobs)

    if num_completed > 0:
        tat_list = [j.finish_time - j.arrival_time for j in completed_jobs]
        wt_list = [tat - j.burst_time for tat, j in zip(tat_list, completed_jobs)]
        rt_list = [
            (j.first_run_time - j.arrival_time) if j.first_run_time is not None else 0
            for j in completed_jobs
        ]

        avg_tat = sum(tat_list) / num_completed
        avg_wt = sum(wt_list) / num_completed
        avg_rt = sum(rt_list) / num_completed

        starvation_threshold = max(3.0 * avg_wt, 10.0)
        starvation_count = sum(1 for wt in wt_list if wt > starvation_threshold)
    else:
        avg_tat = 0.0
        avg_wt = 0.0
        avg_rt = 0.0
        starvation_count = 0

    if total_ticks > 0 and cluster.num_cpus > 0:
        total_busy = sum(cluster.busy_history) if cluster.busy_history else 0
        cpu_util = 100.0 * (total_busy / (cluster.num_cpus * total_ticks))
        throughput = (num_completed / total_ticks) * 100.0
    else:
        cpu_util = 0.0
        throughput = 0.0

    if cluster.total_memory_gb > 0 and cluster.memory_history:
        mean_used_mem = sum(cluster.memory_history) / len(cluster.memory_history)
        mem_util = 100.0 * (mean_used_mem / cluster.total_memory_gb)
    else:
        mem_util = 0.0

    if cluster.queue_length_history:
        avg_ql = sum(cluster.queue_length_history) / len(cluster.queue_length_history)
        max_ql = max(cluster.queue_length_history)
    else:
        avg_ql = 0.0
        max_ql = 0

    total_suspensions = sum(j.suspension_count for j in jobs)

    return {
        "avg_waiting_time": round(avg_wt, 2),
        "avg_turnaround_time": round(avg_tat, 2),
        "avg_response_time": round(avg_rt, 2),
        "cpu_utilization": round(cpu_util, 2),
        "memory_utilization": round(mem_util, 2),
        "throughput": round(throughput, 2),
        "starvation_count": int(starvation_count),
        "suspensions": int(total_suspensions),
        "avg_queue_length": round(avg_ql, 2),
        "max_queue_length": int(max_ql),
        "jobs_completed": int(num_completed),
        "total_ticks": int(total_ticks),
    }
