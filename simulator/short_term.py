"""Short-Term Scheduler (CPU Dispatcher) supporting FCFS, SJF, SRTF, Round Robin, and Priority."""

from typing import Optional
from simulator.job import Job


def age(ready: list[Job], interval: int) -> list[Job]:
    """Increment age_counter on ready jobs; boost priority when counter >= interval. Return boosted jobs."""
    boosted: list[Job] = []
    for job in ready:
        job.age_counter += 1
        if job.age_counter >= interval:
            job.age_counter = 0
            if job.priority > 1:
                job.priority -= 1
                boosted.append(job)
    return boosted


def quantum_preempt(running: list[Job], quantum: int) -> list[Job]:
    """Return jobs in running whose quantum_used >= quantum."""
    return [job for job in running if job.quantum_used >= quantum]


def pick_next(ready: list[Job], policy: str) -> Optional[Job]:
    """Return best candidate in ready without removing it."""
    if not ready:
        return None

    if policy in ("fcfs", "round_robin"):
        return ready[0]
    elif policy == "sjf":
        return min(ready, key=lambda j: (j.burst_time, j.arrival_time, j.id))
    elif policy == "srtf":
        return min(ready, key=lambda j: (j.remaining_time, j.arrival_time, j.id))
    elif policy == "priority":
        return min(ready, key=lambda j: (j.priority, j.arrival_time, j.id))
    else:
        return ready[0]


def find_preemption(ready: list[Job], running: list[Job], policy: str) -> Optional[tuple[Job, Job]]:
    """Return (better_ready_job, worst_running_job) or None if no preemption should occur."""
    if not ready or not running:
        return None

    if policy == "srtf":
        best_ready = min(ready, key=lambda j: (j.remaining_time, j.arrival_time, j.id))
        worst_running = max(running, key=lambda j: (j.remaining_time, j.arrival_time, j.id))
        if best_ready.remaining_time < worst_running.remaining_time:
            return (best_ready, worst_running)
        return None

    elif policy == "priority":
        best_ready = min(ready, key=lambda j: (j.priority, j.arrival_time, j.id))
        worst_running = max(running, key=lambda j: (j.priority, j.arrival_time, j.id))
        if best_ready.priority < worst_running.priority:
            return (best_ready, worst_running)
        return None

    return None
