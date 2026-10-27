# Testing Strategy & Hand-Verified Golden Results (`docs/TESTING.md`)

> Used by all team members and AI agents to verify correctness via `pytest -q`.

---

## 1. Hand-Verified Golden Workload (`tests/golden_workload.json`)

To verify that all 5 short-term policies (`fcfs`, `sjf`, `srtf`, `round_robin`, `priority`) and the metric formulas (`TAT`, `WT`, `RT`) work with 100% mathematical accuracy, we test them on a **1-CPU, 64 GB RAM** cluster (so all 3 jobs fit in memory immediately upon arrival and test pure Short-Term Scheduler behavior with `time_quantum = 2` and `aging_interval_ticks = 100`).

### Input Jobs (`tests/golden_workload.json`)
```json
[
  {
    "id": 101,
    "name": "job-alpha",
    "user": "alice",
    "arrival_time": 0,
    "burst_time": 5,
    "memory_gb": 8,
    "base_priority": 3
  },
  {
    "id": 102,
    "name": "job-beta",
    "user": "bob",
    "arrival_time": 1,
    "burst_time": 2,
    "memory_gb": 8,
    "base_priority": 1
  },
  {
    "id": 103,
    "name": "job-gamma",
    "user": "carol",
    "arrival_time": 2,
    "burst_time": 4,
    "memory_gb": 8,
    "base_priority": 2
  }
]
```

---

### Expected Golden Execution Timelines & Metrics (1 CPU, 64 GB RAM)

#### Policy 1: `fcfs` (Non-Preemptive FIFO)
* **Gantt (Core 0, ticks $0..10$):** `[101, 101, 101, 101, 101, 102, 102, 103, 103, 103, 103]`
* **Per-Job Accounting:**
  * `101`: `first_run=0`, `finish=5` $\to$ $TAT=5, WT=0, RT=0$
  * `102`: `first_run=5`, `finish=7` $\to$ $TAT=6, WT=4, RT=4$
  * `103`: `first_run=7`, `finish=11` $\to$ $TAT=9, WT=5, RT=5$
* **Expected Metrics:**
  * `avg_waiting_time` = $\frac{0 + 4 + 5}{3} = 3.0$
  * `avg_turnaround_time` = $\frac{5 + 6 + 9}{3} = 6.6667$ (`6.67`)
  * `avg_response_time` = $\frac{0 + 4 + 5}{3} = 3.0$
  * `total_ticks` = `11`, `cpu_utilization` = `100.0`

#### Policy 2: `sjf` (Shortest Job First — Non-Preemptive)
* At $t=0$, only `101` is ready $\to$ runs until $t=5$. At $t=5$, `102` (`burst=2`) and `103` (`burst=4`) are ready $\to$ `102` runs $[5..7)$, then `103` runs $[7..11)$.
* **Gantt (Core 0, ticks $0..10$):** `[101, 101, 101, 101, 101, 102, 102, 103, 103, 103, 103]`
* **Expected Metrics:** Identical to `fcfs` for this arrival pattern (`avg_waiting_time = 3.0`).
  *(Note: If all 3 arrive at $t=0$, SJF order is `102` $\to$ `103` $\to$ `101`, giving $WT = (0 + 2 + 6)/3 = 2.67$. Test both in `test_metrics_golden.py`!)*

#### Policy 3: `srtf` (Shortest Remaining Time First — Preemptive)
* $t=0$: `101` (`rem=5`) starts on Core 0.
* $t=1$: `101` advances to `rem=4`. `102` arrives (`rem=2 < 4`) $\to$ **preempts `101`**. `102` runs $[1..3)$.
* $t=2$: `102` has `rem=1`. `103` arrives (`rem=4`). `102` keeps running.
* $t=3$: `102` finishes (`finish=3`). Ready has `101` (`rem=4, arr=0`) and `103` (`rem=4, arr=2`). Tie-breaker `(remaining_time, arrival_time, id)` picks `101`! `101` runs $[3..7)$.
* $t=7$: `101` finishes (`finish=7`). `103` runs $[7..11)$ and finishes at $t=11$.
* **Gantt (Core 0, ticks $0..10$):** `[101, 102, 102, 101, 101, 101, 101, 103, 103, 103, 103]`
* **Per-Job Accounting:**
  * `101`: `first_run=0`, `finish=7` $\to$ $TAT=7, WT=2, RT=0$
  * `102`: `first_run=1`, `finish=3` $\to$ $TAT=2, WT=0, RT=0$
  * `103`: `first_run=7`, `finish=11` $\to$ $TAT=9, WT=5, RT=5$
* **Expected Metrics:**
  * `avg_waiting_time` = $\frac{2 + 0 + 5}{3} = 2.3333$ (`2.33`)
  * `avg_turnaround_time` = $\frac{7 + 2 + 9}{3} = 6.0$
  * `avg_response_time` = $\frac{0 + 0 + 5}{3} = 1.6667$ (`1.67`)

#### Policy 4: `round_robin` (`time_quantum = 2`)
* $t=0$: `101` starts (`q=0`).
* $t=1$: `101` (`rem=4, q=1`). `102` admitted to `ready_queue` $\to$ `ready = [102]`.
* $t=2$: `101` (`rem=3, q=2`). Step 5 admits `103` $\to$ `ready = [102, 103]`. Step 6.1 quantum-preempts `101` to tail $\to$ `ready = [102, 103, 101]`. Step 6.2 dispatches `102`.
* $t=4$: `102` finishes (`finish=4`). `ready = [103, 101]`. Dispatches `103`.
* $t=6$: `103` (`rem=2, q=2`) quantum-preempted to tail $\to$ `ready = [101, 103]`. Dispatches `101`.
* $t=8$: `101` (`rem=1, q=2`) quantum-preempted to tail $\to$ `ready = [103, 101]`. Dispatches `103`.
* $t=10$: `103` finishes (`finish=10`). Dispatches `101`.
* $t=11$: `101` finishes (`finish=11`).
* **Gantt (Core 0, ticks $0..10$):** `[101, 101, 102, 102, 103, 103, 101, 101, 103, 103, 101]`
* **Per-Job Accounting:**
  * `101`: `first_run=0`, `finish=11` $\to$ $TAT=11, WT=6, RT=0$
  * `102`: `first_run=2`, `finish=4` $\to$ $TAT=3, WT=1, RT=1$
  * `103`: `first_run=4`, `finish=10` $\to$ $TAT=8, WT=4, RT=2$
* **Expected Metrics:**
  * `avg_waiting_time` = $\frac{6 + 1 + 4}{3} = 3.6667$ (`3.67`)
  * `avg_turnaround_time` = $\frac{11 + 3 + 8}{3} = 7.3333$ (`7.33`)
  * `avg_response_time` = $\frac{0 + 1 + 2}{3} = 1.0$

#### Policy 5: `priority` (Preemptive Priority, `aging_interval = 100`)
* $t=0$: `101` (`prio=3`) starts.
* $t=1$: `102` (`prio=1`) arrives and preempts `101`. `102` runs $[1..3)$.
* $t=2$: `103` (`prio=2`) arrives in `ready_queue` (behind `102` which is `prio=1`).
* $t=3$: `102` finishes (`finish=3`). Between `103` (`prio=2`) and `101` (`prio=3`), `103` wins and runs $[3..7)$.
* $t=7$: `103` finishes (`finish=7`). `101` runs $[7..11)$ and finishes at $t=11$.
* **Gantt (Core 0, ticks $0..10$):** `[101, 102, 102, 103, 103, 103, 103, 101, 101, 101, 101]`
* **Per-Job Accounting:**
  * `101`: `first_run=0`, `finish=11` $\to$ $TAT=11, WT=6, RT=0$
  * `102`: `first_run=1`, `finish=3` $\to$ $TAT=2, WT=0, RT=0$
  * `103`: `first_run=3`, `finish=7` $\to$ $TAT=5, WT=1, RT=1$
* **Expected Metrics:**
  * `avg_waiting_time` = $\frac{6 + 0 + 1}{3} = 2.3333$ (`2.33`)
  * `avg_turnaround_time` = $\frac{11 + 2 + 5}{3} = 6.0$
  * `avg_response_time` = $\frac{0 + 0 + 1}{3} = 0.3333$ (`0.33`)

---

## 2. Invariant Tests (`tests/test_engine_invariants.py`)

Run a 50-job seeded simulation across all combinations of Long-Term, Medium-Term, and Short-Term policies and assert at **every single tick**:
1. **Memory Conservation:** `cluster.used_memory_gb == sum(j.memory_gb for j in all_jobs if j.state in (JobState.READY, JobState.RUNNING))`
2. **Memory Bounds:** `0 <= cluster.used_memory_gb <= cluster.total_memory_gb`
3. **Core Exclusivity:** Every non-`None` job ID in `cluster.cores` is unique, has `job.state == JobState.RUNNING`, and `job.assigned_cpu == core_index`.
4. **Queue Partitioning:** No job appears in more than one queue, and `job.state` matches the queue it resides in.
5. **No Infinite Loops / Deadlocks:** `engine.run_to_completion(max_ticks=5000)` terminates with all jobs in `TERMINATED`.