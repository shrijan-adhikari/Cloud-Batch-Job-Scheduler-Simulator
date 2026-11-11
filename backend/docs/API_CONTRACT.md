# API Contract & Protocol Specification

## 1. Overview
The backend exposes:
- **REST Endpoints:** under `/api`
- **WebSocket Stream:** at `/ws`
- **Swagger Documentation:** at `/docs` (OpenAPI schema at `/openapi.json`)

All responses use JSON format. Concurrency is guarded by an internal `asyncio.Lock` preventing concurrent state mutation during simulation ticks.

---

## 2. REST Endpoints

### 2.1 State & Configuration

- **`GET /api/state`**
  - **Description:** Returns the current `TickSnapshot` representing the full simulation state.
  - **Response:** `200 OK` (JSON matching Section 3 `TickSnapshot`).

- **`GET /api/config`**
  - **Description:** Returns the active cluster and scheduling configuration.
  - **Response:** `200 OK` with JSON object.

- **`POST /api/config`**
  - **Description:** Dynamically updates scheduling parameters without server restart.
  - **Request Body:**
    ```json
    {
      "long_term_policy": "memory_aware_fcfs",
      "medium_term_policy": "suspend_lowest_priority",
      "short_term_policy": "round_robin",
      "time_quantum": 4,
      "aging_interval_ticks": 6,
      "min_residency_ticks": 3,
      "max_swaps_per_tick": 1,
      "tick_duration_sec": 1.0
    }
    ```
  - **Response:** `200 OK` with updated configuration.

### 2.2 Simulation Control

- **`POST /api/control/start`**
  - **Description:** Begins automated ticking of the simulation engine in the background.
  - **Response:** `{"running": true, "tick": <int>}`

- **`POST /api/control/pause`**
  - **Description:** Pauses automated ticking.
  - **Response:** `{"running": false, "tick": <int>}`

- **`POST /api/control/step`**
  - **Description:** Executes exactly one clock tick (all 7 steps) and broadcasts the snapshot.
  - **Response:** `200 OK` with `TickSnapshot`.

- **`POST /api/control/reset`**
  - **Description:** Resets simulation back to tick 0, clears all queues, terminates any live worker processes, and restores the initial workload.
  - **Response:** `200 OK` with initial `TickSnapshot`.

- **`POST /api/control/mode`**
  - **Description:** Toggles between pure simulation mode and live OS bridge mode.
  - **Request Body:** `{"mode": "simulation" | "live"}`
  - **Response:** `{"mode": "simulation" | "live", "snapshot": <TickSnapshot>}`

### 2.3 Slurm-Compatible Job Management

- **`POST /api/jobs/sbatch`**
  - **Description:** Submits a batch job into the simulation (enters `NEW` at start of next tick).
  - **Request Body:**
    ```json
    {
      "name": "train-resnet",
      "user": "alice",
      "burst_time": 6,
      "memory_gb": 16,
      "priority": 2
    }
    ```
  - **Response:**
    ```json
    {
      "job_id": 105,
      "message": "Submitted batch job 105",
      "job": { ... }
    }
    ```

- **`POST /api/jobs/scancel/{job_id}`**
  - **Description:** Cancels a target job. Releases RAM and CPU core immediately if running or ready.
  - **Response:** `{"job_id": 105, "cancelled": true}`

- **`GET /api/jobs/squeue`**
  - **Description:** Lists all active jobs in `NEW`, `READY`, `RUNNING`, or `SUSPENDED` states.
  - **Response:** `{"jobs": [ ... ]}`

- **`GET /api/jobs/sinfo`**
  - **Description:** Reports cluster core occupancy, RAM capacity, and utilization.
  - **Response:**
    ```json
    {
      "num_cpus": 4,
      "cores": [101, 102, null, null],
      "total_memory_gb": 64,
      "used_memory_gb": 28,
      "available_memory_gb": 36,
      "memory_utilization": 0.4375,
      "suspension_threshold": 0.85,
      "resume_threshold": 0.70
    }
    ```

- **`GET /api/jobs/sacct`**
  - **Description:** Accounting records for all jobs (historical and active).
  - **Response:** `{"jobs": [ ... ]}`

### 2.4 Benchmark & Telemetry

- **`POST /api/benchmark/run`**
  - **Description:** Runs 50–100 seeded jobs through all 5 short-term policies (`fcfs`, `sjf`, `srtf`, `round_robin`, `priority`) in pure simulation.
  - **Response:** `{"benchmark": { "fcfs": { ... }, "sjf": { ... }, ... }}`

- **`GET /api/telemetry`**
  - **Description:** Real-time host utilization sampler from `psutil` (per-core CPU % and RAM).
  - **Response:** `{"host": { ... }, "tick": <int>, ...}`

---

## 3. WebSocket Protocol (`/ws`)

Clients connect via WebSocket at `ws://127.0.0.1:8000/ws`.

1. **On Connect:** The server immediately sends the current `TickSnapshot` JSON.
2. **On Every Tick:** Broadcasts a new `TickSnapshot` to all connected clients.
3. **On Mutation:** Broadcasts immediately after any control action (`start`, `pause`, `step`, `reset`, `sbatch`, `scancel`, `mode`).

### TickSnapshot JSON Schema
```json
{
  "tick": 12,
  "finished": false,
  "mode": "simulation",
  "config": {
    "long_term_policy": "memory_aware_fcfs",
    "medium_term_policy": "suspend_lowest_priority",
    "short_term_policy": "round_robin",
    "time_quantum": 4
  },
  "cluster": {
    "num_cpus": 4,
    "cores": [101, 102, null, null],
    "used_memory_gb": 28,
    "available_memory_gb": 36,
    "total_memory_gb": 64,
    "memory_utilization": 0.4375,
    "suspension_threshold": 0.85,
    "resume_threshold": 0.70
  },
  "queues": {
    "new": [105],
    "ready": [103, 104],
    "running": [101, 102],
    "suspended": [],
    "terminated": [100]
  },
  "jobs": [
    {
      "id": 101,
      "name": "train-resnet",
      "user": "alice",
      "arrival_time": 0,
      "burst_time": 6,
      "remaining_time": 3,
      "memory_gb": 16,
      "base_priority": 2,
      "priority": 2,
      "state": "RUNNING",
      "assigned_cpu": 0,
      "os_pid": 18420,
      "admitted_time": 0,
      "first_run_time": 0,
      "finish_time": null,
      "quantum_used": 3,
      "ready_wait_ticks": 0,
      "age_counter": 0,
      "suspension_count": 0,
      "cancelled": false,
      "last_resumed_time": null
    }
  ],
  "events": [
    {"tick": 12, "scheduler": "MT", "job_id": 104, "message": "Swapped in JOB-104 (12 GB) -> READY"}
  ],
  "gantt_slice": [101, 102, null, null],
  "metrics": {
    "avg_waiting_time": 2.5,
    "avg_turnaround_time": 7.5,
    "avg_response_time": 1.0,
    "cpu_utilization": 68.75,
    "memory_utilization": 43.75,
    "throughput": 8.33,
    "starvation_count": 0,
    "suspensions": 0,
    "avg_queue_length": 1.8,
    "max_queue_length": 4,
    "jobs_completed": 1,
    "total_ticks": 12
  },
  "host": {
    "cpu_percent_per_core": [24.0, 18.5, 5.0, 12.0],
    "ram_percent": 61.2,
    "ram_used_mb": 9800,
    "active_workers": 2,
    "suspended_workers": 0
  }
}
```
