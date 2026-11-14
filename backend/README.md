# Cloud Batch Job Scheduler Simulator - Backend

A complete, high-performance, three-tier batch job scheduler simulator built for Operating Systems study and research, featuring discrete-event simulation, Slurm-compatible CLI interfaces, real-time WebSocket telemetry, and optional Live OS process management.

---

## 1. Architecture Overview

### Three-Tier Scheduling Pipeline
1. **Long-Term Scheduler (Admission Controller):**
   - Controls multiprogramming by admitting jobs from `NEW` to `READY` based on cluster memory capacity.
   - Policies: `memory_aware_fcfs` and `priority_admission`.
2. **Medium-Term Scheduler (RAM Swapper):**
   - Regulates RAM pressure and mitigates priority inversion by swapping out eligible jobs to `SUSPENDED`.
   - Swaps in suspended jobs when memory pressure subsides below resume threshold.
   - Enforces anti-thrashing residency rules (`min_residency_ticks = 3`).
   - Policies: `suspend_lowest_priority` and `suspend_largest_memory`.
3. **Short-Term Scheduler (CPU Dispatcher):**
   - Dispatches `READY` jobs to available CPU cores each clock tick.
   - Supports preemption, time quantum enforcement, and priority aging.
   - Policies: `fcfs`, `sjf`, `srtf`, `round_robin`, `priority` (with dynamic aging).

### Dual Execution Modes
- **Mode A: Benchmark Mode (Pure Simulation):**
  - No real OS processes spawned (`os_bridge.enabled = false`).
  - Evaluates 100 seeded jobs across all 5 scheduling policies simultaneously in **~65 ms** (well under 200 ms).
  - Yields genuine, un-hardcoded metrics for waiting time, turnaround time, CPU/RAM utilization, throughput, starvation, and suspensions.
- **Mode B: Live OS Mode (Observer Bridge):**
  - Spawns real Python worker subprocesses (`os_bridge.enabled = true`).
  - Allocates scaled real memory buffers, touches OS pages, and pins worker affinity to host CPU cores.
  - Suspends and resumes processes in real-time, visible as **Suspended** in Windows Task Manager or `htop`.

---

## 2. Directory Structure

```text
cloud_batch_scheduler/
├── CLAUDE.md                     # Agent guide and quick commands
├── AGENTS.md                     # Multi-agent reference pointer
├── PROJECT_CONTEXT.md            # Frozen authoritative specification (v2.1)
├── README.md                     # Setup, usage, and architecture guide
├── app.py                        # FastAPI application, REST endpoints, and WebSocket loop
├── cli.py                        # Slurm-style CLI client (sbatch, squeue, sinfo, sacct, benchmark)
├── config.json                   # Cluster configuration and scheduler parameters
├── requirements.txt              # Project dependencies
│
├── docs/
│   ├── API_CONTRACT.md           # REST endpoints, WebSocket schema, and TickSnapshot spec
│   ├── TESTING.md                # Test execution report and invariant verification
│   └── DECISIONS.md              # Architectural decisions and gap analysis
│
├── simulator/
│   ├── __init__.py
│   ├── job.py                    # Job, JobState, workload generator, and loader
│   ├── cluster.py                # CPU cores, memory tracking, and timeline history
│   ├── long_term.py              # Admission control policies
│   ├── medium_term.py            # Swap-in and swap-out swapper policies
│   ├── short_term.py             # CPU dispatcher policies (FCFS, SJF, SRTF, RR, Priority)
│   └── engine.py                 # 7-step discrete tick engine
│
├── os_bridge/
│   ├── __init__.py
│   ├── process_manager.py        # Worker process spawning, pinning, and suspension
│   ├── worker.py                 # Live worker payload allocating physical memory
│   └── host_monitor.py           # psutil host hardware sampler
│
├── metrics/
│   ├── __init__.py
│   ├── calculator.py             # Section 6 metrics formulas
│   └── benchmark.py              # Five-policy benchmark harness
│
└── tests/
    ├── test_schedulers.py        # Tests for LT, MT, and ST algorithms
    ├── test_engine_invariants.py # Tests for 7-step tick order and invariants
    ├── test_metrics_golden.py    # Hand-verified formula calculations and speed
    ├── test_api.py               # REST, WebSocket, and bridge safety tests
    ├── golden_workload.json      # 3-job hand-verified workload
    └── sample_jobs.json          # 15-job deterministic workload
```

---

## 3. Installation & Setup

### Prerequisites
- Python 3.12+ (tested on Python 3.14).
- No external databases (Redis, PostgreSQL, etc.) or Celery required.

### Install Dependencies
```bash
pip install -r requirements.txt
```

---

## 4. Running the Backend Server

Start the FastAPI backend with uvicorn:
```bash
python -m uvicorn app:app --reload
```

The server binds to `http://127.0.0.1:8000`.

### Interactive Swagger UI
Open your browser and navigate to:
**[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)**

From Swagger UI, you can:
- Inspect the current simulation state via `GET /api/state`.
- Advance clock ticks via `POST /api/control/step`.
- Start / pause automated execution via `POST /api/control/start` and `POST /api/control/pause`.
- Submit jobs via `POST /api/jobs/sbatch` and cancel jobs via `POST /api/jobs/scancel/{job_id}`.
- Trigger policy benchmarks via `POST /api/benchmark/run`.
- Switch to Live OS mode via `POST /api/control/mode`.

---

## 5. Running Automated Tests

Run the complete test suite:
```bash
python -m pytest -q
```

Expected output:
```text
24 passed in ~1.36s
```

Run specific test modules:
```bash
python -m pytest tests/test_schedulers.py -v
python -m pytest tests/test_engine_invariants.py -v
python -m pytest tests/test_metrics_golden.py -v
python -m pytest tests/test_api.py -v
```

---

## 6. Slurm-Style CLI Usage

The CLI (`cli.py`) operates as a thin HTTP client against the running FastAPI server, or in standalone mode for local benchmarks:

### Submit a Batch Job
```bash
python cli.py sbatch --name model-train --burst 8 --memory 16 --priority 2 --user alice
```

### Inspect the Active Queue
```bash
python cli.py squeue
```
Output:
```text
JOBID    NAME             USER       STATE        BURST   REMAIN   MEM(GB)  PRIO   CPU  
--------------------------------------------------------------------------------------
101      train-resnet     alice      RUNNING      6       3        16       2      0    
102      etl-spark        bob        RUNNING      8       5        12       3      1    
103      render-frames    carol      READY        4       4        8        2      -    
104      monte-carlo      dave       SUSPENDED    10      7        16       4      -    
```

### View Cluster Status
```bash
python cli.py sinfo
```

### View Historical Accounting Records
```bash
python cli.py sacct
```

### Cancel a Job
```bash
python cli.py scancel 101
```

### Run Benchmark Comparison
Locally without running server:
```bash
python cli.py benchmark --local
```
Or against the running server:
```bash
python cli.py benchmark
```
Output:
```text
======================= BENCHMARK POLICY COMPARISON =======================
POLICY         AVG WT    AVG TAT   AVG RT    CPU %    MEM %    THROUGHPUT   STARVED  SWAPS  
--------------------------------------------------------------------------------------------
fcfs           13.95     23.75     13.35     85.96    77.25    35.09        1        14     
sjf            13.40     23.20     10.55     85.96    77.99    35.09        0        5      
srtf           14.80     24.60     11.20     77.78    68.82    31.75        0        8      
round_robin    16.05     25.85     11.65     96.08    87.38    39.22        0        6      
priority       13.90     23.70     13.15     85.96    76.37    35.09        0        10     
===========================================================================
```

---

## 7. Frontend Integration Guide

The backend is fully prepared for independent frontend integration:
1. **CORS:** Configured in `app.py` to allow origins `http://localhost:3000`, `http://localhost:5173`, `http://127.0.0.1:3000`, `http://127.0.0.1:5173`.
2. **REST Endpoints:** Standard REST requests under `http://127.0.0.1:8000/api/*`.
3. **WebSocket Stream:** Real-time state synchronization via `ws://127.0.0.1:8000/ws`. Upon connection, the backend immediately streams the current `TickSnapshot`, followed by real-time broadcasts on every tick or mutation.
4. **Contract Reference:** Consult `docs/API_CONTRACT.md` for exact JSON schemas.
