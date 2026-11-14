"""Discrete tick simulation engine orchestrating the seven-step clock tick order."""

from typing import Optional, Any
from simulator.cluster import Cluster
from simulator.job import Job, JobState, clone_jobs, generate_workload
from simulator.long_term import admit
from simulator.medium_term import swap_in, swap_out
from simulator.short_term import age, quantum_preempt, pick_next, find_preemption
from metrics.calculator import compute_metrics
from os_bridge import host_monitor


class Engine:
    """Discrete-time engine driving the three-tier scheduling simulation."""

    def __init__(
        self,
        config: dict,
        jobs: list[Job],
        os_bridge: Optional[Any] = None,
    ) -> None:
        """Initialize simulation cluster, queues, scheduler settings, and initial jobs."""
        self.config: dict = config
        self.os_bridge: Optional[Any] = os_bridge
        self.initial_jobs: list[Job] = clone_jobs(jobs)

        cluster_cfg = config.get("cluster", {})
        self.cluster = Cluster(
            num_cpus=cluster_cfg.get("num_cpus", 4),
            total_memory_gb=cluster_cfg.get("total_memory_gb", 64),
            suspension_threshold=cluster_cfg.get("memory_suspension_threshold", 0.85),
            resume_threshold=cluster_cfg.get("memory_resume_threshold", 0.70),
        )

        sched_cfg = config.get("schedulers", {})
        self.long_term_policy: str = sched_cfg.get("long_term_policy", "memory_aware_fcfs")
        self.medium_term_policy: str = sched_cfg.get("medium_term_policy", "suspend_lowest_priority")
        self.short_term_policy: str = sched_cfg.get("short_term_policy", "round_robin")
        self.time_quantum: int = sched_cfg.get("time_quantum", 4)
        self.aging_interval_ticks: int = sched_cfg.get("aging_interval_ticks", 6)
        self.min_residency_ticks: int = sched_cfg.get("min_residency_ticks", 3)
        self.max_swaps_per_tick: int = sched_cfg.get("max_swaps_per_tick", 1)

        self.tick: int = 0
        self.unsubmitted_jobs: list[Job] = clone_jobs(self.initial_jobs)
        self.all_jobs_list: list[Job] = list(self.unsubmitted_jobs)
        self.new_queue: list[Job] = []
        self.ready_queue: list[Job] = []
        self.running_jobs: list[Job] = []
        self.suspended_queue: list[Job] = []
        self.terminated_jobs: list[Job] = []

        self.pending_submissions: list[Job] = []
        self.pending_cancellations: list[int] = []
        self.events: list[dict] = []
        self.current_tick_events: list[dict] = []

    @property
    def all_jobs(self) -> list[Job]:
        """Return all tracked jobs across all queues and states."""
        return self.all_jobs_list

    @property
    def finished(self) -> bool:
        """Return True when all submitted and unsubmitted jobs have reached termination."""
        return (
            len(self.unsubmitted_jobs) == 0
            and len(self.new_queue) == 0
            and len(self.ready_queue) == 0
            and len(self.running_jobs) == 0
            and len(self.suspended_queue) == 0
            and len(self.pending_submissions) == 0
        )

    def submit(self, name: str, user: str, burst_time: int, memory_gb: int, priority: int) -> Job:
        """Submit a new job externally to be admitted atomically at the start of the next tick."""
        existing_ids = [j.id for j in self.all_jobs] + [j.id for j in self.pending_submissions]
        new_id = (max(existing_ids) + 1) if existing_ids else 101

        max_allowed_mem = max(1, self.cluster.total_memory_gb // 2)
        safe_memory_gb = max(1, min(memory_gb, max_allowed_mem))
        safe_priority = max(1, min(priority, 5))

        job = Job(
            id=new_id,
            name=name,
            user=user,
            arrival_time=self.tick,
            burst_time=max(1, burst_time),
            memory_gb=safe_memory_gb,
            base_priority=safe_priority,
        )
        self.pending_submissions.append(job)
        return job

    def cancel(self, job_id: int) -> bool:
        """Register an external scancel command to cancel a job at the start of the next tick."""
        for j in list(self.pending_submissions):
            if j.id == job_id and not j.cancelled:
                self.pending_submissions.remove(j)
                j.state = JobState.TERMINATED
                j.cancelled = True
                j.finish_time = self.tick
                self.terminated_jobs.append(j)
                return True

        for j in (
            self.unsubmitted_jobs
            + self.new_queue
            + self.ready_queue
            + self.running_jobs
            + self.suspended_queue
        ):
            if j.id == job_id and not j.cancelled:
                self.pending_cancellations.append(job_id)
                return True
        return False

    def reset(self, config: Optional[dict] = None, jobs: Optional[list[Job]] = None) -> None:
        """Reset the simulation engine and terminate any real OS worker processes."""
        if self.os_bridge:
            self.os_bridge.terminate_all()

        if config is not None:
            self.config = config
            cluster_cfg = config.get("cluster", {})
            self.cluster = Cluster(
                num_cpus=cluster_cfg.get("num_cpus", 4),
                total_memory_gb=cluster_cfg.get("total_memory_gb", 64),
                suspension_threshold=cluster_cfg.get("memory_suspension_threshold", 0.85),
                resume_threshold=cluster_cfg.get("memory_resume_threshold", 0.70),
            )
            sched_cfg = config.get("schedulers", {})
            self.long_term_policy = sched_cfg.get("long_term_policy", "memory_aware_fcfs")
            self.medium_term_policy = sched_cfg.get("medium_term_policy", "suspend_lowest_priority")
            self.short_term_policy = sched_cfg.get("short_term_policy", "round_robin")
            self.time_quantum = sched_cfg.get("time_quantum", 4)
            self.aging_interval_ticks = sched_cfg.get("aging_interval_ticks", 6)
            self.min_residency_ticks = sched_cfg.get("min_residency_ticks", 3)
            self.max_swaps_per_tick = sched_cfg.get("max_swaps_per_tick", 1)
        else:
            self.cluster = Cluster(
                num_cpus=self.cluster.num_cpus,
                total_memory_gb=self.cluster.total_memory_gb,
                suspension_threshold=self.cluster.suspension_threshold,
                resume_threshold=self.cluster.resume_threshold,
            )

        if jobs is not None:
            self.initial_jobs = clone_jobs(jobs)

        self.tick = 0
        self.unsubmitted_jobs = clone_jobs(self.initial_jobs)
        self.all_jobs_list = list(self.unsubmitted_jobs)
        self.new_queue = []
        self.ready_queue = []
        self.running_jobs = []
        self.suspended_queue = []
        self.terminated_jobs = []
        self.pending_submissions = []
        self.pending_cancellations = []
        self.events = []
        self.current_tick_events = []

    def step_tick(self, record_snapshot: bool = True) -> dict:
        """Execute exactly one clock tick following the mandatory 7-step sequence."""
        t = self.tick
        self.current_tick_events = []

        # Apply pending external commands atomically
        for sub_job in self.pending_submissions:
            sub_job.arrival_time = t
            self.all_jobs_list.append(sub_job)
            self.new_queue.append(sub_job)
            self.current_tick_events.append({
                "tick": t,
                "scheduler": "ENGINE",
                "job_id": sub_job.id,
                "message": f"Submitted JOB-{sub_job.id} ({sub_job.name}) into NEW",
            })
        self.pending_submissions.clear()

        for cancel_id in self.pending_cancellations:
            self._apply_cancellation(cancel_id, t)
        self.pending_cancellations.clear()

        # Step 1: Ingest arrivals
        arriving = [j for j in self.unsubmitted_jobs if j.arrival_time <= t]
        arriving.sort(key=lambda j: (j.arrival_time, j.id))
        for j in arriving:
            self.unsubmitted_jobs.remove(j)
            j.state = JobState.NEW
            self.new_queue.append(j)
            self.current_tick_events.append({
                "tick": t,
                "scheduler": "LT",
                "job_id": j.id,
                "message": f"JOB-{j.id} ({j.name}) arrived in NEW",
            })

        # Step 2: Advance running jobs
        for core in range(self.cluster.num_cpus):
            job_id = self.cluster.cores[core]
            if job_id is None:
                continue
            job = next((j for j in self.running_jobs if j.id == job_id), None)
            if job is None:
                continue

            job.remaining_time -= 1
            job.quantum_used += 1

            if job.remaining_time <= 0:
                job.state = JobState.TERMINATED
                job.finish_time = t
                job.assigned_cpu = None
                self.cluster.release(job)
                self.cluster.vacate(core)
                if self.os_bridge:
                    self.os_bridge.terminate(job)
                if job in self.running_jobs:
                    self.running_jobs.remove(job)
                self.terminated_jobs.append(job)
                self.current_tick_events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": job.id,
                    "message": f"JOB-{job.id} finished execution -> TERMINATED",
                })

        # Step 3: Priority aging (only when short_term_policy == "priority")
        if self.short_term_policy == "priority":
            boosted = age(self.ready_queue, self.aging_interval_ticks)
            for b in boosted:
                self.current_tick_events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": b.id,
                    "message": f"Priority aged: JOB-{b.id} priority boosted to {b.priority}",
                })

        # Step 4: Medium-term scheduler (swap-in first, then swap-out)
        resumed = swap_in(self.suspended_queue, self.cluster, t)
        for job in resumed:
            self.ready_queue.append(job)
            self.current_tick_events.append({
                "tick": t,
                "scheduler": "MT",
                "job_id": job.id,
                "message": f"Swapped in JOB-{job.id} ({job.memory_gb} GB) -> READY",
            })

        mt_cfg = {
            "max_swaps_per_tick": self.max_swaps_per_tick,
            "min_residency_ticks": self.min_residency_ticks,
            "long_term_policy": self.long_term_policy,
        }
        victims = swap_out(
            self.ready_queue,
            self.running_jobs,
            self.new_queue,
            self.cluster,
            self.medium_term_policy,
            t,
            mt_cfg,
        )
        for victim in victims:
            if self.os_bridge:
                self.os_bridge.suspend(victim)
            self.suspended_queue.append(victim)
            self.current_tick_events.append({
                "tick": t,
                "scheduler": "MT",
                "job_id": victim.id,
                "message": f"Swapped out JOB-{victim.id} ({victim.memory_gb} GB) -> SUSPENDED",
            })

        # Step 5: Long-term scheduler
        admitted = admit(self.new_queue, self.cluster, self.long_term_policy, t)
        for job in admitted:
            self.ready_queue.append(job)
            self.current_tick_events.append({
                "tick": t,
                "scheduler": "LT",
                "job_id": job.id,
                "message": f"Admitted JOB-{job.id} ({job.memory_gb} GB) -> READY",
            })

        # Step 6: Short-term scheduler
        # 6.1 Quantum check (round_robin only)
        if self.short_term_policy == "round_robin":
            preempted = quantum_preempt(self.running_jobs, self.time_quantum)
            for job in preempted:
                self.running_jobs.remove(job)
                core = job.assigned_cpu
                if core is not None:
                    self.cluster.vacate(core)
                job.state = JobState.READY
                job.assigned_cpu = None
                job.quantum_used = 0
                job.age_counter = 0
                if self.os_bridge:
                    self.os_bridge.suspend(job)
                self.ready_queue.append(job)
                self.current_tick_events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": job.id,
                    "message": f"Quantum expired for JOB-{job.id} -> READY",
                })

        # 6.2 Fill free cores in ascending order
        for core in self.cluster.free_cores():
            if not self.ready_queue:
                break
            best_job = pick_next(self.ready_queue, self.short_term_policy)
            if best_job is None:
                break
            self.ready_queue.remove(best_job)
            self.cluster.assign(best_job, core)
            best_job.state = JobState.RUNNING
            best_job.quantum_used = 0
            best_job.age_counter = 0
            if best_job.first_run_time is None:
                best_job.first_run_time = t
            self.running_jobs.append(best_job)
            if self.os_bridge:
                self.os_bridge.start_or_resume(best_job, core)
            self.current_tick_events.append({
                "tick": t,
                "scheduler": "ST",
                "job_id": best_job.id,
                "message": f"Dispatched JOB-{best_job.id} to Core {core}",
            })

        # 6.3 Preemption (srtf and priority only)
        if self.short_term_policy in ("srtf", "priority"):
            while self.ready_queue and self.running_jobs:
                preemption_pair = find_preemption(
                    self.ready_queue, self.running_jobs, self.short_term_policy
                )
                if preemption_pair is None:
                    break
                better_ready, worst_running = preemption_pair
                core = worst_running.assigned_cpu

                self.running_jobs.remove(worst_running)
                self.ready_queue.remove(better_ready)

                # Preempt worst running job
                worst_running.state = JobState.READY
                worst_running.assigned_cpu = None
                worst_running.quantum_used = 0
                worst_running.age_counter = 0
                if self.os_bridge:
                    self.os_bridge.suspend(worst_running)
                self.ready_queue.append(worst_running)

                # Assign better ready job to the core
                better_ready.state = JobState.RUNNING
                better_ready.assigned_cpu = core
                better_ready.quantum_used = 0
                better_ready.age_counter = 0
                if better_ready.first_run_time is None:
                    better_ready.first_run_time = t
                if core is not None:
                    self.cluster.cores[core] = better_ready.id
                self.running_jobs.append(better_ready)
                if self.os_bridge and core is not None:
                    self.os_bridge.start_or_resume(better_ready, core)

                self.current_tick_events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": worst_running.id,
                    "message": (
                        f"JOB-{worst_running.id} preempted by JOB-{better_ready.id} on Core {core}"
                    ),
                })

        # Step 7: Record telemetry and return TickSnapshot
        for job in self.ready_queue:
            job.ready_wait_ticks += 1
        for job in self.suspended_queue:
            job.ready_wait_ticks += 1

        total_queued = len(self.new_queue) + len(self.ready_queue) + len(self.suspended_queue)
        self.cluster.record_tick(total_queued)

        self.events.extend(self.current_tick_events)
        snap = self.snapshot() if record_snapshot else {}

        self.tick += 1
        return snap

    def run_to_completion(self, max_ticks: int = 10_000) -> list[Job]:
        """Execute step_tick in a loop until all jobs terminate or max_ticks is reached."""
        while not self.finished and self.tick < max_ticks:
            self.step_tick(record_snapshot=False)
        return [j for j in self.all_jobs if j.state == JobState.TERMINATED]

    def snapshot(self) -> dict:
        """Construct a complete dictionary matching the strict TickSnapshot JSON contract."""
        active_mode = "live" if self.os_bridge is not None else "simulation"

        if self.os_bridge is not None:
            host_telemetry = host_monitor.sample()
            bridge_status = self.os_bridge.status()
            active_workers = sum(1 for w in bridge_status if w.get("status") == "running")
            suspended_workers = sum(1 for w in bridge_status if w.get("status") == "suspended")
        else:
            host_telemetry = {
                "cpu_percent_per_core": [0.0] * self.cluster.num_cpus,
                "ram_percent": 0.0,
                "ram_used_mb": 0,
            }
            active_workers = 0
            suspended_workers = 0

        host_telemetry["active_workers"] = active_workers
        host_telemetry["suspended_workers"] = suspended_workers

        # Build metrics using elapsed ticks
        elapsed_ticks = max(1, self.tick if not self.finished else len(self.cluster.busy_history))
        calc_metrics = compute_metrics(self.all_jobs, self.cluster, elapsed_ticks)

        return {
            "tick": self.tick,
            "finished": self.finished,
            "mode": active_mode,
            "config": {
                "long_term_policy": self.long_term_policy,
                "medium_term_policy": self.medium_term_policy,
                "short_term_policy": self.short_term_policy,
                "time_quantum": self.time_quantum,
            },
            "cluster": {
                "num_cpus": self.cluster.num_cpus,
                "cores": list(self.cluster.cores),
                "used_memory_gb": self.cluster.used_memory_gb,
                "available_memory_gb": self.cluster.available_memory_gb,
                "total_memory_gb": self.cluster.total_memory_gb,
                "memory_utilization": round(self.cluster.memory_utilization, 4),
                "suspension_threshold": self.cluster.suspension_threshold,
                "resume_threshold": self.cluster.resume_threshold,
            },
            "queues": {
                "new": [j.id for j in self.new_queue],
                "ready": [j.id for j in self.ready_queue],
                "running": [j.id for j in self.running_jobs],
                "suspended": [j.id for j in self.suspended_queue],
                "terminated": [j.id for j in self.terminated_jobs],
            },
            "jobs": [j.to_dict() for j in self.all_jobs],
            "events": list(self.current_tick_events),
            "gantt_slice": list(self.cluster.cores),
            "metrics": calc_metrics,
            "host": host_telemetry,
        }

    def _apply_cancellation(self, job_id: int, current_tick: int) -> None:
        """Internal helper to cancel and reclaim resources of a target job."""
        for q, in_running, in_ready in [
            (self.unsubmitted_jobs, False, False),
            (self.new_queue, False, False),
            (self.ready_queue, False, True),
            (self.running_jobs, True, False),
            (self.suspended_queue, False, False),
        ]:
            for job in list(q):
                if job.id == job_id:
                    q.remove(job)
                    if in_running:
                        if job.assigned_cpu is not None:
                            self.cluster.vacate(job.assigned_cpu)
                        self.cluster.release(job)
                        if self.os_bridge:
                            self.os_bridge.terminate(job)
                    elif in_ready:
                        self.cluster.release(job)

                    if self.os_bridge and not in_running:
                        self.os_bridge.terminate(job)

                    job.state = JobState.TERMINATED
                    job.cancelled = True
                    job.finish_time = current_tick
                    job.assigned_cpu = None
                    self.terminated_jobs.append(job)
                    self.current_tick_events.append({
                        "tick": current_tick,
                        "scheduler": "ENGINE",
                        "job_id": job.id,
                        "message": f"Cancelled JOB-{job.id} -> TERMINATED",
                    })
                    return
