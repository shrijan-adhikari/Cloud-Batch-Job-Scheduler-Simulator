"""Medium-Term Scheduler implementing swap-in, swap-out, memory pressure handling, and residency rules."""

from typing import Optional
from simulator.cluster import Cluster
from simulator.job import Job, JobState


def swap_in(suspended: list[Job], cluster: Cluster, now: int) -> list[Job]:
    """Remove resumed jobs from suspended, allocate RAM, set READY/last_resumed_time, return them."""
    resumed: list[Job] = []

    while cluster.memory_utilization < cluster.resume_threshold and suspended:
        best_candidate = min(suspended, key=lambda j: (j.priority, j.arrival_time, j.id))

        can_fit = cluster.can_fit(best_candidate)
        new_utilization = (
            (cluster.used_memory_gb + best_candidate.memory_gb) / cluster.total_memory_gb
            if cluster.total_memory_gb > 0
            else 0.0
        )

        if can_fit and new_utilization <= cluster.suspension_threshold:
            suspended.remove(best_candidate)
            cluster.allocate(best_candidate)
            best_candidate.state = JobState.READY
            best_candidate.last_resumed_time = now
            best_candidate.age_counter = 0
            resumed.append(best_candidate)
        else:
            # Stop swap-in loop if the highest-priority suspended job cannot fit
            break

    return resumed


def swap_out(
    ready: list[Job],
    running: list[Job],
    new_queue: list[Job],
    cluster: Cluster,
    policy: str,
    now: int,
    cfg: dict,
) -> list[Job]:
    """Return suspended victims. Removes from ready/running, vacates core if running, frees RAM."""
    max_swaps = cfg.get("max_swaps_per_tick", 1)
    min_residency = cfg.get("min_residency_ticks", 3)
    lt_policy = cfg.get("long_term_policy", "memory_aware_fcfs")

    victims: list[Job] = []

    for _ in range(max_swaps):
        if not new_queue:
            break

        if lt_policy == "priority_admission":
            candidates = sorted(new_queue, key=lambda j: (j.priority, j.arrival_time, j.id))
        else:
            candidates = sorted(new_queue, key=lambda j: (j.arrival_time, j.id))

        first_cand = candidates[0]
        if cluster.can_fit(first_cand):
            # If the candidate already fits, do not swap out anything
            break

        blocked_job = first_cand
        trigger_a = cluster.memory_utilization > cluster.suspension_threshold
        trigger_b = blocked_job.priority <= 2

        if not trigger_a and not trigger_b:
            break

        def is_eligible_victim(job: Job, require_prio_4: bool = False) -> bool:
            if job.priority <= blocked_job.priority:
                return False
            if require_prio_4 and job.priority < 4:
                return False
            if job.last_resumed_time is not None and (now - job.last_resumed_time) < min_residency:
                return False
            return True

        def victim_key(job: Job):
            if policy == "suspend_largest_memory":
                return (job.memory_gb, job.priority, job.id)
            # Default: suspend_lowest_priority
            return (job.priority, job.memory_gb, job.id)

        # Look in ready_queue first
        require_prio_4 = not trigger_a and trigger_b
        eligible_ready = [j for j in ready if is_eligible_victim(j, require_prio_4=require_prio_4)]

        victim: Optional[Job] = None
        victim_from_running = False

        if eligible_ready:
            victim = max(eligible_ready, key=victim_key)
        elif trigger_b:
            # Check running jobs with priority >= 4 and eligible residency
            eligible_running = [
                j for j in running if is_eligible_victim(j, require_prio_4=True)
            ]
            if eligible_running:
                victim = max(eligible_running, key=victim_key)
                victim_from_running = True

        if victim is None:
            break

        # Process victim removal and state update
        if victim_from_running:
            if victim in running:
                running.remove(victim)
            if victim.assigned_cpu is not None:
                cluster.vacate(victim.assigned_cpu)
        else:
            if victim in ready:
                ready.remove(victim)

        cluster.release(victim)
        victim.state = JobState.SUSPENDED
        victim.assigned_cpu = None
        victim.quantum_used = 0
        victim.age_counter = 0
        victim.suspension_count += 1
        victims.append(victim)

    return victims
