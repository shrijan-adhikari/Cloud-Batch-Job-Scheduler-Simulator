"""Discrete Tick Engine for Cloud Batch Job Scheduler Simulator."""

from typing import Optional, Any
from simulator.job import Job, JobState, clone_jobs
from simulator.cluster import Cluster
from simulator import long_term, medium_term, short_term


class Engine:
    """Discrete clock tick engine orchestrating 3-tier schedulers and telemetry."""

    def __init__(
        self,
        config: dict,
        jobs: list[Job],
        os_bridge: Optional[Any] = None,
    ) -> None:
        self.config = config
        self.os_bridge = os_bridge
        self.tick: int = 0

        cluster_cfg = config.get("cluster", {})
        self.cluster = Cluster(
            num_cpus=cluster_cfg.get("num_cpus", 4),
            total_memory_gb=cluster_cfg.get("total_memory_gb", 64),
            suspension_threshold=cluster_cfg.get("memory_suspension_threshold", 0.85),
            resume_threshold=cluster_cfg.get("memory_resume_threshold", 0.70),
        )

        self._initial_jobs = clone_jobs(jobs)
        self.all_jobs: list[Job] = clone_jobs(jobs)
        self.next_job_id: int = max([j.id for j in self.all_jobs], default=100) + 1

        self.new_queue: list[Job] = []
        self.ready_queue: list[Job] = []
        self.running_jobs: list[Job] = []
        self.suspended_queue: list[Job] = []
        self.terminated_jobs: list[Job] = []

        self.events: list[dict] = []
        self.pending_submits: list[Job] = []
        self.pending_cancels: list[int] = []

    @property
    def finished(self) -> bool:
        """Return True if all jobs have reached TERMINATED state."""
        return len(self.all_jobs) > 0 and all(j.state == JobState.TERMINATED for j in self.all_jobs)

    def submit(self, name: str, user: str, burst_time: int, memory_gb: int, priority: int) -> Job:
        """Submit a new job to be ingested at the current tick."""
        job = Job(
            id=self.next_job_id,
            name=name,
            user=user,
            arrival_time=self.tick,
            burst_time=burst_time,
            memory_gb=memory_gb,
            base_priority=priority,
        )
        self.next_job_id += 1
        self.all_jobs.append(job)
        self.new_queue.append(job)
        self.new_queue.sort(key=lambda j: (j.arrival_time, j.id))
        return job

    def cancel(self, job_id: int) -> bool:
        """Queue or execute cancellation of a job by ID."""
        return self._execute_cancel(job_id)

    def _execute_cancel(self, job_id: int) -> bool:
        """Cancel a job immediately, reclaiming core and memory if needed."""
        target: Optional[Job] = None
        for j in self.all_jobs:
            if j.id == job_id:
                target = j
                break

        if target is None or target.state == JobState.TERMINATED:
            return False

        old_state = target.state
        target.cancelled = True
        target.state = JobState.TERMINATED
        target.finish_time = self.tick

        if target in self.new_queue:
            self.new_queue.remove(target)
        elif target in self.ready_queue:
            self.ready_queue.remove(target)
            self.cluster.release(target)
        elif target in self.running_jobs:
            self.running_jobs.remove(target)
            if target.assigned_cpu is not None:
                self.cluster.vacate(target.assigned_cpu)
                target.assigned_cpu = None
            self.cluster.release(target)
            if self.os_bridge is not None and hasattr(self.os_bridge, "terminate"):
                self.os_bridge.terminate(target)
        elif target in self.suspended_queue:
            self.suspended_queue.remove(target)

        self.terminated_jobs.append(target)
        self.events.append({
            "tick": self.tick,
            "scheduler": "CANCEL",
            "job_id": target.id,
            "message": f"Job {target.id} cancelled (was {old_state.value})",
        })
        return True

    def reset(self, config: Optional[dict] = None, jobs: Optional[list[Job]] = None) -> None:
        """Reset the engine back to tick 0."""
        if self.os_bridge is not None and hasattr(self.os_bridge, "terminate_all"):
            self.os_bridge.terminate_all()

        if config is not None:
            self.config = config

        cluster_cfg = self.config.get("cluster", {})
        self.cluster = Cluster(
            num_cpus=cluster_cfg.get("num_cpus", 4),
            total_memory_gb=cluster_cfg.get("total_memory_gb", 64),
            suspension_threshold=cluster_cfg.get("memory_suspension_threshold", 0.85),
            resume_threshold=cluster_cfg.get("memory_resume_threshold", 0.70),
        )

        self.tick = 0
        self.events = []
        self.new_queue = []
        self.ready_queue = []
        self.running_jobs = []
        self.suspended_queue = []
        self.terminated_jobs = []
        self.pending_submits = []
        self.pending_cancels = []

        if jobs is not None:
            self._initial_jobs = clone_jobs(jobs)
        self.all_jobs = clone_jobs(self._initial_jobs)
        self.next_job_id = max([j.id for j in self.all_jobs], default=100) + 1

    def step_tick(self, record_snapshot: bool = True) -> dict:
        """Execute one clock tick strictly following the 7-step sequence in PROJECT_CONTEXT.md §4.4."""
        t = self.tick
        sched_cfg = self.config.get("schedulers", {})
        lt_policy = sched_cfg.get("long_term_policy", "memory_aware_fcfs")
        mt_policy = sched_cfg.get("medium_term_policy", "suspend_lowest_priority")
        st_policy = sched_cfg.get("short_term_policy", "round_robin")
        time_quantum = sched_cfg.get("time_quantum", 4)
        aging_interval = sched_cfg.get("aging_interval_ticks", 6)

        # (0) Apply pending external commands atomically
        for cid in list(self.pending_cancels):
            self._execute_cancel(cid)
        self.pending_cancels.clear()

        for sjob in list(self.pending_submits):
            sjob.arrival_time = t
            self.all_jobs.append(sjob)
            self.new_queue.append(sjob)
        self.pending_submits.clear()

        # Step 1: Ingest arrivals
        new_arrivals = [
            j for j in self.all_jobs
            if j.arrival_time == t
            and j.state == JobState.NEW
            and j not in self.new_queue
            and j not in self.ready_queue
            and j not in self.running_jobs
            and j not in self.suspended_queue
            and j not in self.terminated_jobs
        ]
        self.new_queue.extend(new_arrivals)
        self.new_queue.sort(key=lambda j: (j.arrival_time, j.id))

        # Step 2: Advance running jobs (work done during tick t-1 to t)
        for job in list(self.running_jobs):
            job.remaining_time -= 1
            job.quantum_used += 1
            if job.remaining_time == 0:
                job.state = JobState.TERMINATED
                job.finish_time = t
                core = job.assigned_cpu
                job.assigned_cpu = None
                self.cluster.release(job)
                if core is not None:
                    self.cluster.vacate(core)
                self.running_jobs.remove(job)
                self.terminated_jobs.append(job)
                if self.os_bridge is not None and hasattr(self.os_bridge, "terminate"):
                    self.os_bridge.terminate(job)
                self.events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": job.id,
                    "message": f"Job {job.id} ({job.name}) completed",
                })

        # Step 3: Priority aging (only when short_term_policy == "priority")
        if st_policy == "priority":
            boosted = short_term.age(self.ready_queue, aging_interval)
            for b in boosted:
                self.events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": b.id,
                    "message": f"Aged JOB-{b.id} priority boosted to {b.priority}",
                })

        # Step 4: Medium-term scheduler (swap-in first, then swap-out)
        resumed = medium_term.swap_in(self.suspended_queue, self.cluster, t)
        for r in resumed:
            self.ready_queue.append(r)
            self.events.append({
                "tick": t,
                "scheduler": "MT",
                "job_id": r.id,
                "message": f"Swapped in JOB-{r.id} ({r.memory_gb} GB) -> READY",
            })

        victims = medium_term.swap_out(
            self.ready_queue,
            self.running_jobs,
            self.new_queue,
            self.cluster,
            mt_policy,
            t,
            sched_cfg,
        )
        for v in victims:
            self.suspended_queue.append(v)
            if self.os_bridge is not None and hasattr(self.os_bridge, "suspend"):
                self.os_bridge.suspend(v)
            self.events.append({
                "tick": t,
                "scheduler": "MT",
                "job_id": v.id,
                "message": f"Swapped out JOB-{v.id} ({v.memory_gb} GB) -> SUSPENDED",
            })

        # Step 5: Long-term scheduler
        admitted = long_term.admit(self.new_queue, self.cluster, lt_policy, t)
        for a in admitted:
            self.ready_queue.append(a)
            self.events.append({
                "tick": t,
                "scheduler": "LT",
                "job_id": a.id,
                "message": f"Admitted JOB-{a.id} ({a.memory_gb} GB) -> READY",
            })

        # Step 6: Short-term scheduler
        # 6.1: Quantum check (round_robin only)
        if st_policy == "round_robin":
            expired = short_term.quantum_preempt(self.running_jobs, time_quantum)
            for job in expired:
                core = job.assigned_cpu
                if core is not None:
                    self.cluster.vacate(core)
                self.running_jobs.remove(job)
                job.state = JobState.READY
                job.assigned_cpu = None
                job.quantum_used = 0
                job.age_counter = 0
                if self.os_bridge is not None and hasattr(self.os_bridge, "suspend"):
                    self.os_bridge.suspend(job)
                self.ready_queue.append(job)
                self.events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": job.id,
                    "message": f"Quantum expired for JOB-{job.id} -> READY",
                })

        # 6.2: Fill free cores in ascending order
        for core in self.cluster.free_cores():
            job = short_term.pick_next(self.ready_queue, st_policy)
            if job is None:
                break
            self.ready_queue.remove(job)
            job.state = JobState.RUNNING
            self.cluster.assign(job, core)
            self.running_jobs.append(job)
            job.quantum_used = 0
            job.age_counter = 0
            if job.first_run_time is None:
                job.first_run_time = t
            if self.os_bridge is not None and hasattr(self.os_bridge, "start_or_resume"):
                self.os_bridge.start_or_resume(job, core)
            self.events.append({
                "tick": t,
                "scheduler": "ST",
                "job_id": job.id,
                "message": f"Dispatched JOB-{job.id} to Core {core}",
            })

        # 6.3: Preemption (srtf and priority only)
        if st_policy in ("srtf", "priority"):
            while self.ready_queue:
                pair = short_term.find_preemption(self.ready_queue, self.running_jobs, st_policy)
                if pair is None:
                    break
                better, victim = pair
                core = victim.assigned_cpu
                if core is not None:
                    self.cluster.vacate(core)
                self.running_jobs.remove(victim)
                victim.state = JobState.READY
                victim.assigned_cpu = None
                victim.quantum_used = 0
                victim.age_counter = 0
                if self.os_bridge is not None and hasattr(self.os_bridge, "suspend"):
                    self.os_bridge.suspend(victim)
                self.ready_queue.append(victim)

                self.ready_queue.remove(better)
                better.state = JobState.RUNNING
                if core is not None:
                    self.cluster.assign(better, core)
                self.running_jobs.append(better)
                better.quantum_used = 0
                better.age_counter = 0
                if better.first_run_time is None:
                    better.first_run_time = t
                if self.os_bridge is not None and hasattr(self.os_bridge, "start_or_resume"):
                    self.os_bridge.start_or_resume(better, core if core is not None else 0)
                self.events.append({
                    "tick": t,
                    "scheduler": "ST",
                    "job_id": better.id,
                    "message": f"JOB-{better.id} preempted JOB-{victim.id} on Core {core}",
                })

        # Step 7: Record telemetry
        for j in self.ready_queue:
            j.ready_wait_ticks += 1
        for j in self.suspended_queue:
            j.ready_wait_ticks += 1

        total_queued = len(self.new_queue) + len(self.ready_queue) + len(self.suspended_queue)
        self.cluster.record_tick(total_queued=total_queued)

        snap = self.snapshot() if record_snapshot else {}
        self.tick += 1
        return snap

    def run_to_completion(self, max_ticks: int = 10_000) -> list[Job]:
        """Run discrete simulation until all jobs terminate or max_ticks is reached."""
        while not self.finished and self.tick < max_ticks:
            self.step_tick(record_snapshot=False)
        return self.all_jobs

    def compute_current_metrics(self) -> dict:
        """Compute metrics according to PROJECT_CONTEXT.md §6."""
        from metrics.calculator import compute_metrics
        completed = [j for j in self.all_jobs if j.state == JobState.TERMINATED and not j.cancelled and j.finish_time is not None]
        if self.finished and completed:
            t_total = max(max(j.finish_time for j in completed), 1)
        else:
            t_total = max(self.tick, 1)

        return compute_metrics(self.all_jobs, self.cluster, t_total)

    def _sample_host(self) -> dict:
        """Sample host telemetry (or return simulated host metrics)."""
        if self.os_bridge is not None and hasattr(self.os_bridge, "sample_host"):
            return self.os_bridge.sample_host()
        return {
            "cpu_percent_per_core": [0.0] * self.cluster.num_cpus,
            "ram_percent": round(self.cluster.memory_utilization * 100.0, 1),
            "ram_used_mb": self.cluster.used_memory_gb * 1024,
            "active_workers": len(self.running_jobs),
            "suspended_workers": len(self.suspended_queue),
        }

    def snapshot(self) -> dict:
        """Build a TickSnapshot dict strictly matching PROJECT_CONTEXT.md §4.6."""
        sched_cfg = self.config.get("schedulers", {})
        return {
            "tick": self.tick,
            "finished": self.finished,
            "mode": "live" if (self.os_bridge is not None and getattr(self.os_bridge, "enabled", True)) else "simulation",
            "config": {
                "long_term_policy": sched_cfg.get("long_term_policy", "memory_aware_fcfs"),
                "medium_term_policy": sched_cfg.get("medium_term_policy", "suspend_lowest_priority"),
                "short_term_policy": sched_cfg.get("short_term_policy", "round_robin"),
                "time_quantum": sched_cfg.get("time_quantum", 4),
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
                "running": [c for c in self.cluster.cores if c is not None],
                "suspended": [j.id for j in self.suspended_queue],
                "terminated": [j.id for j in self.terminated_jobs],
            },
            "jobs": [j.to_dict() for j in self.all_jobs],
            "events": list(self.events[-50:]),
            "gantt_slice": list(self.cluster.cores),
            "metrics": self.compute_current_metrics(),
            "host": self._sample_host(),
        }
