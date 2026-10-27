"""Job data model, workload generator, and workload loader."""

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional
import json
import random


class JobState(str, Enum):
    NEW = "NEW"
    READY = "READY"
    RUNNING = "RUNNING"
    SUSPENDED = "SUSPENDED"
    TERMINATED = "TERMINATED"


@dataclass
class Job:
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

    def __post_init__(self) -> None:
        self.priority = self.base_priority
        self.remaining_time = self.burst_time

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "user": self.user,
            "arrival_time": self.arrival_time,
            "burst_time": self.burst_time,
            "memory_gb": self.memory_gb,
            "base_priority": self.base_priority,
            "priority": self.priority,
            "remaining_time": self.remaining_time,
            "state": self.state.value if hasattr(self.state, "value") else str(self.state),
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
    """Generate a deterministic synthetic workload using a dedicated random generator."""
    rng = random.Random(seed)
    users = ["alice", "bob", "carol", "dave"]
    names = ["train-resnet", "etl-spark", "render-frames", "log-parse", "monte-carlo", "backup"]
    priority_choices = [1, 2, 3, 4, 5]
    priority_weights = [10, 25, 30, 25, 10]

    max_normal_mem = max(4, min(16, total_memory_gb // 4))
    max_heavy_mem = max(max_normal_mem, total_memory_gb // 2)

    jobs: list[Job] = []
    current_arrival = 0

    for i in range(num_jobs):
        if i > 0:
            current_arrival += rng.randint(0, 2)
        job_id = 101 + i
        burst = rng.randint(3, 15)

        is_heavy = rng.random() < 0.15
        if is_heavy:
            mem = rng.randint(max_normal_mem, max_heavy_mem)
        else:
            mem = rng.randint(4, max_normal_mem)
        mem = max(1, min(mem, total_memory_gb // 2))

        prio = rng.choices(priority_choices, weights=priority_weights)[0]
        name = rng.choice(names)
        user = rng.choice(users)

        jobs.append(
            Job(
                id=job_id,
                name=name,
                user=user,
                arrival_time=current_arrival,
                burst_time=burst,
                memory_gb=mem,
                base_priority=prio,
            )
        )

    return jobs


def load_workload(path: str) -> list[Job]:
    """Load jobs from a JSON file matching tests/*.json schema."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    jobs: list[Job] = []
    for item in data:
        job = Job(
            id=item["id"],
            name=item["name"],
            user=item["user"],
            arrival_time=item["arrival_time"],
            burst_time=item["burst_time"],
            memory_gb=item["memory_gb"],
            base_priority=item.get("base_priority", item.get("priority", 3)),
        )
        if "priority" in item:
            job.priority = item["priority"]
        if "remaining_time" in item:
            job.remaining_time = item["remaining_time"]
        if "state" in item:
            job.state = JobState(item["state"])
        if "assigned_cpu" in item:
            job.assigned_cpu = item["assigned_cpu"]
        if "os_pid" in item:
            job.os_pid = item["os_pid"]
        if "admitted_time" in item:
            job.admitted_time = item["admitted_time"]
        if "first_run_time" in item:
            job.first_run_time = item["first_run_time"]
        if "finish_time" in item:
            job.finish_time = item["finish_time"]
        if "quantum_used" in item:
            job.quantum_used = item["quantum_used"]
        if "ready_wait_ticks" in item:
            job.ready_wait_ticks = item["ready_wait_ticks"]
        if "age_counter" in item:
            job.age_counter = item["age_counter"]
        if "suspension_count" in item:
            job.suspension_count = item["suspension_count"]
        if "cancelled" in item:
            job.cancelled = item["cancelled"]
        if "last_resumed_time" in item:
            job.last_resumed_time = item["last_resumed_time"]

        jobs.append(job)

    return jobs


def clone_jobs(jobs: list[Job]) -> list[Job]:
    """Create fresh copies of jobs with runtime state reset to initial values."""
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
