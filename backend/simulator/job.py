"""Job definitions, state management, workload generator, and loader."""

from dataclasses import dataclass, field, asdict
from enum import Enum
import json
import random
from typing import Optional


class JobState(str, Enum):
    """Execution states for simulation jobs."""
    NEW = "NEW"
    READY = "READY"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    TERMINATED = "TERMINATED"


@dataclass
class Job:
    """Represents a batch computation job in the cluster simulator."""
    id: int                          # unique, e.g. 101
    name: str                        # e.g. "train-resnet"
    user: str                        # e.g. "alice"
    arrival_time: int                # tick when the job enters NEW
    burst_time: int                  # total CPU ticks needed (>= 1)
    memory_gb: int                   # simulated RAM (1 .. total_memory_gb // 2)
    base_priority: int               # 1 (highest) .. 5 (lowest)

    # dynamic runtime fields
    priority: int = field(init=False)         # current priority (aging may lower the number)
    remaining_time: int = field(init=False)
    state: JobState = JobState.NEW
    assigned_cpu: Optional[int] = None
    os_pid: Optional[int] = None              # Live OS mode only

    # timing and accounting
    admitted_time: Optional[int] = None
    first_run_time: Optional[int] = None
    finish_time: Optional[int] = None
    quantum_used: int = 0
    ready_wait_ticks: int = 0                 # ticks spent in READY + SUSPENDED (display only)
    age_counter: int = 0
    suspension_count: int = 0
    cancelled: bool = False                   # set by scancel (excluded from metrics)
    last_resumed_time: Optional[int] = None   # tick of last swap-in (anti-thrash rule)

    def __post_init__(self):
        """Initialize dynamic runtime values from base configuration."""
        self.priority = self.base_priority
        self.remaining_time = self.burst_time

    def to_dict(self) -> dict:
        """Serialize job state to dictionary matching the JSON specification."""
        return {
            "id": self.id,
            "name": self.name,
            "user": self.user,
            "arrival_time": self.arrival_time,
            "burst_time": self.burst_time,
            "remaining_time": self.remaining_time,
            "memory_gb": self.memory_gb,
            "base_priority": self.base_priority,
            "priority": self.priority,
            "state": self.state.value,
            "assigned_cpu": self.assigned_cpu,
            "os_pid": self.os_pid,
            "admitted_time": self.admitted_time,
            "first_run_time": self.first_run_time,
            "finish_time": self.finish_time,
            "quantum_used": self.quantum_used,
            "ready_wait_ticks": self.ready_wait_ticks,
            "age_counter": self.age_counter,
            "suspension_count": self.suspension_count,
            "cancelled": self.cancelled,
            "last_resumed_time": self.last_resumed_time,
        }


def generate_workload(num_jobs: int, seed: int, total_memory_gb: int) -> list[Job]:
    """Generate deterministic simulated batch jobs using an isolated random generator."""
    rng = random.Random(seed)
    users = ["alice", "bob", "carol", "dave"]
    job_names = [
        "train-resnet",
        "etl-spark",
        "render-frames",
        "log-parse",
        "monte-carlo",
        "backup",
    ]

    max_mem_allowed = max(1, total_memory_gb // 2)
    normal_max_mem = max(1, min(16, total_memory_gb // 4))
    normal_min_mem = min(4, normal_max_mem)

    jobs: list[Job] = []
    current_tick = 0

    for i in range(num_jobs):
        job_id = 101 + i
        if i > 0:
            current_tick += rng.randint(0, 2)
        burst_time = rng.randint(3, 15)

        is_heavy = rng.random() < 0.15
        if is_heavy and max_mem_allowed > normal_max_mem:
            memory_gb = rng.randint(normal_max_mem + 1, max_mem_allowed)
        else:
            memory_gb = rng.randint(normal_min_mem, normal_max_mem)

        memory_gb = max(1, min(memory_gb, max_mem_allowed))
        base_priority = rng.choices([1, 2, 3, 4, 5], weights=[1, 3, 4, 3, 1])[0]
        user = rng.choice(users)
        name = rng.choice(job_names)

        job = Job(
            id=job_id,
            name=name,
            user=user,
            arrival_time=current_tick,
            burst_time=burst_time,
            memory_gb=memory_gb,
            base_priority=base_priority,
        )
        jobs.append(job)

    return jobs


def load_workload(path: str) -> list[Job]:
    """Load and parse a workload JSON file into a list of Job instances."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_jobs = data.get("jobs", data) if isinstance(data, dict) else data
    jobs: list[Job] = []
    for item in raw_jobs:
        job = Job(
            id=int(item["id"]),
            name=str(item["name"]),
            user=str(item["user"]),
            arrival_time=int(item["arrival_time"]),
            burst_time=int(item["burst_time"]),
            memory_gb=int(item["memory_gb"]),
            base_priority=int(item["base_priority"]),
        )
        jobs.append(job)
    return jobs


def clone_jobs(jobs: list[Job]) -> list[Job]:
    """Return fresh independent copies of jobs with reset runtime state."""
    cloned: list[Job] = []
    for j in jobs:
        new_j = Job(
            id=j.id,
            name=j.name,
            user=j.user,
            arrival_time=j.arrival_time,
            burst_time=j.burst_time,
            memory_gb=j.memory_gb,
            base_priority=j.base_priority,
        )
        cloned.append(new_j)
    return cloned
