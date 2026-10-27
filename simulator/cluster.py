"""Cluster state, RAM tracking, core assignments, and telemetry recording."""

from typing import Optional
from simulator.job import Job


class Cluster:
    """Manages physical/simulated cluster resources including CPU cores and RAM."""

    def __init__(
        self,
        num_cpus: int,
        total_memory_gb: int,
        suspension_threshold: float = 0.85,
        resume_threshold: float = 0.70,
    ) -> None:
        self.num_cpus = num_cpus
        self.total_memory_gb = total_memory_gb
        self.suspension_threshold = suspension_threshold
        self.resume_threshold = resume_threshold

        self.cores: list[Optional[int]] = [None] * num_cpus
        self.used_memory_gb: int = 0
        self.gantt: list[list[Optional[int]]] = [[] for _ in range(num_cpus)]
        self.memory_history: list[int] = []
        self.busy_history: list[int] = []
        self.queue_length_history: list[int] = []

    @property
    def available_memory_gb(self) -> int:
        """Return remaining unallocated memory in GB."""
        return self.total_memory_gb - self.used_memory_gb

    @property
    def memory_utilization(self) -> float:
        """Return memory utilization fraction in [0.0, 1.0]."""
        if self.total_memory_gb <= 0:
            return 0.0
        return self.used_memory_gb / self.total_memory_gb

    def can_fit(self, job: Job) -> bool:
        """Check if cluster has sufficient available RAM for the job."""
        return self.available_memory_gb >= job.memory_gb

    def allocate(self, job: Job) -> None:
        """Reserve RAM for a job. Raises ValueError if insufficient RAM."""
        if not self.can_fit(job):
            raise ValueError(
                f"Cannot allocate {job.memory_gb} GB; only {self.available_memory_gb} GB available."
            )
        self.used_memory_gb += job.memory_gb

    def release(self, job: Job) -> None:
        """Release RAM held by a job."""
        self.used_memory_gb = max(0, self.used_memory_gb - job.memory_gb)

    def free_cores(self) -> list[int]:
        """Return list of free core indices in ascending order."""
        return [i for i, jid in enumerate(self.cores) if jid is None]

    def assign(self, job: Job, core: int) -> None:
        """Assign a job to a specific CPU core."""
        if core < 0 or core >= self.num_cpus:
            raise IndexError(f"Core index {core} out of range [0, {self.num_cpus - 1}]")
        if self.cores[core] is not None:
            raise ValueError(f"Core {core} already occupied by job {self.cores[core]}")
        self.cores[core] = job.id
        job.assigned_cpu = core

    def vacate(self, core: int) -> None:
        """Vacate a specific CPU core."""
        if core < 0 or core >= self.num_cpus:
            raise IndexError(f"Core index {core} out of range [0, {self.num_cpus - 1}]")
        self.cores[core] = None

    def record_tick(self, total_queued: int = 0) -> None:
        """Record telemetry for the current discrete clock tick."""
        for c in range(self.num_cpus):
            self.gantt[c].append(self.cores[c])
        self.memory_history.append(self.used_memory_gb)
        self.busy_history.append(sum(1 for c in self.cores if c is not None))
        self.queue_length_history.append(total_queued)
