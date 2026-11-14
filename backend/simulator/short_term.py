"""Short-Term Scheduler implementing CPU dispatching, preemption, quantum handling, and aging."""

from typing import Optional
from simulator.job import Job


def age(ready: list[Job], interval: int) -> list[Job]:
    """Increment age_counter on ready jobs; boost priority when counter >= interval. Return boosted jobs."""
    boosted: list[Job] = []
    for job in ready:
        job.age_counter += 1
        if job.age_counter >= interval and job.priority > 1:
            job.priority -= 1
            job.age_counter = 0
            boosted.append(job)
    return boosted


def quantum_preempt(running: list[Job], quantum: int) -> list[Job]:
    """Return jobs in running whose quantum_used >= quantum."""
    return [job for job in running if job.quantum_used >= quantum]


def pick_next(ready: list[Job], policy: str) -> Optional[Job]:
    """Return best candidate in ready without removing it."""
    if not ready:
        return None

    if policy == "fcfs" or policy == "round_robin":
        return ready[0]
    if policy == "sjf":
        return min(ready, key=lambda j: (j.burst_time, j.arrival_time, j.id))
    if policy == "srtf":
        return min(ready, key=lambda j: (j.remaining_time, j.arrival_time, j.id))
    if policy == "priority":
        return min(ready, key=lambda j: (j.priority, j.arrival_time, j.id))

    return ready[0]


def find_preemption(ready: list[Job], running: list[Job], policy: str) -> Optional[tuple[Job, Job]]:
    """Return (better_ready_job, worst_running_job) or None."""
    if not ready or not running:
        return None

    if policy == "srtf":
        best_ready = pick_next(ready, "srtf")
        if best_ready is None:
            return None
        worst_running = max(running, key=lambda j: (j.remaining_time, j.arrival_time, j.id))
        if best_ready.remaining_time < worst_running.remaining_time:
            return (best_ready, worst_running)
        return None

    if policy == "priority":
        best_ready = pick_next(ready, "priority")
        if best_ready is None:
            return None
        worst_running = max(running, key=lambda j: (j.priority, j.arrival_time, j.id))
        if best_ready.priority < worst_running.priority:
            return (best_ready, worst_running)
        return None

    return None
