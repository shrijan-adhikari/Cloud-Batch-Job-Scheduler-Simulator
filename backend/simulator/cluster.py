"""Cluster hardware state, CPU core assignment, memory accounting, and timeline tracking."""

from typing import Optional
from simulator.job import Job


class Cluster:
    """Manages simulated CPU cores, RAM accounting, and historical execution telemetry."""

    def __init__(
        self,
        num_cpus: int,
        total_memory_gb: int,
        suspension_threshold: float = 0.85,
        resume_threshold: float = 0.70,
    ) -> None:
        """Initialize cluster capacity, thresholds, and telemetry trackers."""
        self.num_cpus: int = num_cpus
        self.total_memory_gb: int = total_memory_gb
        self.suspension_threshold: float = suspension_threshold
        self.resume_threshold: float = resume_threshold

        self.cores: list[Optional[int]] = [None] * num_cpus
        self.used_memory_gb: int = 0
        self.gantt: list[list[Optional[int]]] = [[] for _ in range(num_cpus)]
        self.memory_history: list[int] = []
        self.busy_history: list[int] = []
        self.queue_length_history: list[int] = []

    @property
    def available_memory_gb(self) -> int:
        """Return the unallocated simulated memory capacity in GB."""
        return max(0, self.total_memory_gb - self.used_memory_gb)

    @property
    def memory_utilization(self) -> float:
        """Return the current memory utilization as a decimal ratio between 0.0 and 1.0."""
        if self.total_memory_gb <= 0:
            return 0.0
        return self.used_memory_gb / self.total_memory_gb

    def can_fit(self, job: Job) -> bool:
        """Check whether the cluster currently has enough free RAM for the specified job."""
        return (self.used_memory_gb + job.memory_gb) <= self.total_memory_gb

    def allocate(self, job: Job) -> None:
        """Reserve simulated RAM for an admitted or swapped-in job."""
        if not self.can_fit(job):
            raise ValueError(
                f"Cannot allocate {job.memory_gb} GB for job {job.id}. "
                f"Available: {self.available_memory_gb} GB, Total: {self.total_memory_gb} GB"
            )
        self.used_memory_gb += job.memory_gb

    def release(self, job: Job) -> None:
        """Release simulated RAM previously held by a job."""
        self.used_memory_gb = max(0, self.used_memory_gb - job.memory_gb)

    def free_cores(self) -> list[int]:
        """Return an ascending list of core indices that are currently idle."""
        return [core_idx for core_idx, occupant in enumerate(self.cores) if occupant is None]

    def assign(self, job: Job, core: int) -> None:
        """Assign a job to an idle CPU core."""
        if core < 0 or core >= self.num_cpus:
            raise IndexError(f"Core index {core} out of bounds (num_cpus={self.num_cpus})")
        self.cores[core] = job.id
        job.assigned_cpu = core

    def vacate(self, core: int) -> None:
        """Clear the occupant from the specified CPU core."""
        if 0 <= core < self.num_cpus:
            self.cores[core] = None

    def record_tick(self, total_queued: int = 0) -> None:
        """Record the current tick's core assignments, memory usage, and queue lengths into telemetry."""
        for core_idx in range(self.num_cpus):
            self.gantt[core_idx].append(self.cores[core_idx])
        self.memory_history.append(self.used_memory_gb)
        busy_cores = sum(1 for c in self.cores if c is not None)
        self.busy_history.append(busy_cores)
        self.queue_length_history.append(total_queued)
