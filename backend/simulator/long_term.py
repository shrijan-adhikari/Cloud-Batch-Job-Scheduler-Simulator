"""Long-Term Scheduler implementing admission control and memory-aware queue progression."""

from simulator.cluster import Cluster
from simulator.job import Job, JobState


def admit(new_queue: list[Job], cluster: Cluster, policy: str, now: int) -> list[Job]:
    """Remove admitted jobs from new_queue, allocate RAM, set READY/admitted_time, return them in order."""
    if policy == "priority_admission":
        candidates = sorted(new_queue, key=lambda j: (j.priority, j.arrival_time, j.id))
    else:
        # Default policy: memory_aware_fcfs
        candidates = sorted(new_queue, key=lambda j: (j.arrival_time, j.id))

    admitted: list[Job] = []
    for job in candidates:
        if cluster.can_fit(job):
            cluster.allocate(job)
            job.state = JobState.READY
            job.admitted_time = now
            job.age_counter = 0
            if job in new_queue:
                new_queue.remove(job)
            admitted.append(job)

    return admitted
