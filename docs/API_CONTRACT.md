# REST API, WebSocket & SLURM CLI Contract (`docs/API_CONTRACT.md`)

> Supplements `PROJECT_CONTEXT.md` Section 4.6 and Section 8. Used by **Member 4** (`app.py`, `cli.py`) and **Member 5** (`dashboard/app.js`).

---

## 1. FastAPI REST Endpoints (`app.py`)

Base URL: `http://127.0.0.1:8000`

### 1.1 Cluster & Simulation Control

| Method | Path | Request Body | Response (`200 OK`) | Description |
| :--- | :--- | :--- | :--- | :--- |
| `GET` | `/api/state` | None | `TickSnapshot` (see `PROJECT_CONTEXT.md` §4.6) | Returns current full simulation snapshot. |
| `POST` | `/api/control/start` | `{"tick_duration_sec": 1.0}` (optional) | `{"status": "running", "tick": int}` | Starts background async tick loop. |
| `POST` | `/api/control/pause` | None | `{"status": "paused", "tick": int}` | Pauses background tick loop. |
| `POST` | `/api/control/step` | `{"ticks": 1}` (optional, default 1) | `TickSnapshot` | Advances simulation by $N$ ticks immediately (while paused). |
| `POST` | `/api/control/reset` | `ResetRequest` (optional, see below) | `TickSnapshot` | Terminates any live OS workers, rebuilds engine with config/seed. |
| `POST` | `/api/control/mode` | `{"os_bridge_enabled": bool}` | `{"os_bridge_enabled": bool}` | Toggles Live OS Execution Mode (Task Manager sync). |

#### `ResetRequest` JSON Schema (all fields optional)
```json
{
  "num_cpus": 4,
  "total_memory_gb": 64,
  "long_term_policy": "memory_aware_fcfs",
  "medium_term_policy": "suspend_lowest_priority",
  "short_term_policy": "round_robin",
  "time_quantum": 4,
  "num_jobs": 20,
  "random_seed": 42,
  "os_bridge_enabled": false
}
```

---

### 1.2 SLURM Job & Node Operations

| Method | Path | Request Body | Response (`200 OK`) | Description |
| :--- | :--- | :--- | :--- | :--- |
| `POST` | `/api/jobs` (`sbatch`) | `SubmitJobRequest` | `{"submitted": JobDict, "message": "Submitted batch job 121"}` | Submits a new job at the current tick. |
| `DELETE` | `/api/jobs/{job_id}` (`scancel`) | None | `{"cancelled": true, "job_id": 105}` | Cancels job (frees RAM/CPU and kills OS PID if running). |
| `POST` | `/api/jobs/{job_id}/suspend` | None | `{"status": "SUSPENDED", "job_id": 105}` | Manually suspends a `READY` or `RUNNING` job (Job Inspector action). |
| `POST` | `/api/jobs/{job_id}/resume` | None | `{"status": "READY", "job_id": 105}` | Manually resumes a `SUSPENDED` job if RAM fits. |
| `GET` | `/api/squeue` | None | `{"jobs": [JobDict, ...]}` | Lists active jobs (`NEW`, `READY`, `RUNNING`, `SUSPENDED`). |
| `GET` | `/api/sinfo` | None | `SinfoResponse` | Node state, core occupancy, RAM usage, and host telemetry. |
| `GET` | `/api/sacct` | None | `{"jobs": [JobDict, ...]}` | Accounting log of all jobs including `TERMINATED` and `cancelled`. |

#### `SubmitJobRequest` JSON Schema
```json
{
  "name": "llm-finetune",
  "user": "alice",
  "burst_time": 8,
  "memory_gb": 16,
  "priority": 1
}
```

---

### 1.3 Policy Benchmark Endpoint

| Method | Path | Request Body | Response (`200 OK`) |
| :--- | :--- | :--- | :--- |
| `POST` | `/api/benchmark` | `{"num_jobs": 30, "seed": 42}` (optional) | `BenchmarkResponse` |

#### `BenchmarkResponse` JSON Schema
```json
{
  "num_jobs": 30,
  "seed": 42,
  "policies": {
    "fcfs": {
      "avg_waiting_time": 14.2,
      "avg_turnaround_time": 22.7,
      "avg_response_time": 12.1,
      "cpu_utilization": 84.5,
      "memory_utilization": 71.2,
      "throughput": 18.5,
      "starvation_count": 1,
      "suspensions": 2,
      "avg_queue_length": 6.4,
      "max_queue_length": 14,
      "jobs_completed": 30,
      "total_ticks": 162
    },
    "sjf": { "...": "..." },
    "srtf": { "...": "..." },
    "round_robin": { "...": "..." },
    "priority": { "...": "..." }
  }
}
```

---

## 2. WebSocket Protocol (`/ws`)

* **URL:** `ws://127.0.0.1:8000/ws`
* **On Connect:** Server immediately sends the current `TickSnapshot` JSON.
* **On Every Tick / State Mutation:** Server broadcasts the latest `TickSnapshot` JSON to all connected clients.
* **Client-to-Server Messages (Optional convenience):** Clients may use REST `POST` calls for controls or send JSON `{ "action": "start" | "pause" | "step" | "reset" }`.

---

## 3. SLURM CLI Output Formats (`cli.py`)

`cli.py` supports both subcommands (`python cli.py squeue`) and interactive web terminal parsing.

### 3.1 `squeue` Output Format
```text
JOBID   NAME            USER    PRIO   MEM(GB)   STATE       TIME_REM   CORE    OS_PID
101     train-resnet    alice   1      16        RUNNING     3/8        CPU0    18420
102     etl-spark       bob     2      12        RUNNING     5/6        CPU1    18455
103     monte-carlo     carol   3      16        READY       10/10      -       -
104     backup          dave    5      24        SUSPENDED   7/12       -       18490 (Susp)
105     render-frames   alice   2      16        NEW         4/4        -       -
```

### 3.2 `sinfo` Output Format
```text
PARTITION   AVAIL   TIMELIMIT   CPUS(A/I/T)   MEMORY(USED/TOTAL)   STATE      OS_BRIDGE
batch*      up      infinite    2/2/4         28G / 64G (43.8%)    allocated  LIVE (2 active, 1 susp)
```

### 3.3 `sacct` Output Format
```text
JOBID   NAME            USER    PRIO   SUBMIT   START   END   BURST   WAIT   TAT   RESP   SUSP   STATE
100     log-parse       bob     2      0        0       4     4       0      4     0      0      COMPLETED
101     train-resnet    alice   1      0        0       -     8       0      -     0      0      RUNNING
```