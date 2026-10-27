"""Long-Term Scheduler (Admission Controller)."""

from simulator.job import Job, JobState
from simulator.cluster import Cluster


def admit(new_queue: list[Job], cluster: Cluster, policy: str, now: int) -> list[Job]:
    """Remove admitted jobs from new_queue, allocate RAM, set READY/admitted_time, return them in order."""
    if policy == "priority_admission":
        candidates = sorted(new_queue, key=lambda j: (j.priority, j.arrival_time, j.id))
    elif policy == "memory_aware_fcfs":
        candidates = sorted(new_queue, key=lambda j: (j.arrival_time, j.id))
    else:
        candidates = list(new_queue)

    admitted: list[Job] = []
    for job in candidates:
        if cluster.can_fit(job):
            cluster.allocate(job)
            job.state = JobState.READY
            job.admitted_time = now
            job.age_counter = 0
            new_queue.remove(job)
            admitted.append(job)

    return admitted
