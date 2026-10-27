"""Medium-Term Scheduler (RAM Swapper / Anti-Thrashing Hysteresis)."""

from simulator.job import Job, JobState
from simulator.cluster import Cluster


def swap_in(suspended: list[Job], cluster: Cluster, now: int) -> list[Job]:
    """Remove resumed jobs from suspended, allocate RAM, set READY/last_resumed_time, return them."""
    resumed: list[Job] = []

    while cluster.memory_utilization < cluster.resume_threshold and suspended:
        best_suspended = min(suspended, key=lambda j: (j.priority, j.arrival_time, j.id))

        can_fit = cluster.can_fit(best_suspended)
        post_util = (cluster.used_memory_gb + best_suspended.memory_gb) / cluster.total_memory_gb
        under_suspension = post_util <= cluster.suspension_threshold

        if can_fit and under_suspension:
            suspended.remove(best_suspended)
            cluster.allocate(best_suspended)
            best_suspended.state = JobState.READY
            best_suspended.last_resumed_time = now
            best_suspended.age_counter = 0
            resumed.append(best_suspended)
        else:
            # Stop the swap-in loop for this tick if highest-priority cannot fit
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
    if not new_queue:
        return []

    min_residency = cfg.get("min_residency_ticks", 3)
    max_swaps = cfg.get("max_swaps_per_tick", 1)
    long_term_policy = cfg.get("long_term_policy", "memory_aware_fcfs")
    suspension_threshold = cfg.get("memory_suspension_threshold", cluster.suspension_threshold)

    victims: list[Job] = []

    for _ in range(max_swaps):
        # Determine candidate ordering under active long-term policy
        if long_term_policy == "priority_admission":
            candidates = sorted(new_queue, key=lambda j: (j.priority, j.arrival_time, j.id))
        else:
            candidates = sorted(new_queue, key=lambda j: (j.arrival_time, j.id))

        if not candidates or cluster.can_fit(candidates[0]):
            break

        blocked_job = candidates[0]

        trigger_a = cluster.memory_utilization > suspension_threshold

        # Trigger B: blocked_job has priority <= 2 and at least one eligible victim with priority >= 4 exists
        has_eligible_b_victim = any(
            j.priority >= 4 and (j.last_resumed_time is None or (now - j.last_resumed_time) >= min_residency)
            for j in (ready + running)
        )
        trigger_b = (blocked_job.priority <= 2) and has_eligible_b_victim

        if not trigger_a and not trigger_b:
            break

        # Eligible victims in ready_queue
        eligible_ready = [
            j for j in ready
            if j.priority > blocked_job.priority
            and (j.last_resumed_time is None or (now - j.last_resumed_time) >= min_residency)
            and (not trigger_b or trigger_a or j.priority >= 4)
        ]

        target_pool = eligible_ready
        victim_from_running = False

        if not eligible_ready and trigger_b:
            # Check running jobs with priority >= 4 and eligible residency
            eligible_running = [
                j for j in running
                if j.priority >= 4 and j.priority > blocked_job.priority
                and (j.last_resumed_time is None or (now - j.last_resumed_time) >= min_residency)
            ]
            if eligible_running:
                target_pool = eligible_running
                victim_from_running = True

        if not target_pool:
            break

        # Select victim based on medium-term policy
        if policy == "suspend_largest_memory":
            victim = max(target_pool, key=lambda j: (j.memory_gb, j.priority, j.id))
        else:  # suspend_lowest_priority (default)
            victim = max(target_pool, key=lambda j: (j.priority, j.memory_gb, j.id))

        # Effect on victim
        if victim_from_running:
            running.remove(victim)
            if victim.assigned_cpu is not None:
                cluster.vacate(victim.assigned_cpu)
        else:
            ready.remove(victim)

        cluster.release(victim)
        victim.state = JobState.SUSPENDED
        victim.assigned_cpu = None
        victim.quantum_used = 0
        victim.age_counter = 0
        victim.suspension_count += 1
        victims.append(victim)

        if cluster.can_fit(blocked_job):
            break

    return victims
