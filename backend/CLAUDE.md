# CLAUDE.md - Developer and Agent Guide

## Overview
This repository contains the backend for the **Cloud Batch Job Scheduler Simulator**, a three-tier Operating Systems batch scheduler implementing:
1. **Long-Term Scheduler (Admission Controller):** `memory_aware_fcfs` and `priority_admission`
2. **Medium-Term Scheduler (RAM Swapper):** `suspend_lowest_priority` and `suspend_largest_memory` (with anti-thrashing residency rules)
3. **Short-Term Scheduler (CPU Dispatcher):** `fcfs`, `sjf`, `srtf`, `round_robin`, `priority` (with dynamic priority aging)

The authoritative specification is `PROJECT_CONTEXT.md` (v2.1 Frozen Specification).

---

## Key Commands

- **Install dependencies:**
  ```bash
  pip install -r requirements.txt
  ```

- **Run FastAPI server:**
  ```bash
  python -m uvicorn app:app --reload
  ```
  Swagger UI: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

- **Run tests:**
  ```bash
  pytest -q
  # or
  python -m pytest -q
  ```

- **CLI Usage (Slurm-style):**
  ```bash
  python cli.py squeue
  python cli.py sinfo
  python cli.py sacct
  python cli.py sbatch --name sim-job --burst 8 --memory 12 --priority 2
  python cli.py scancel <JOB_ID>
  python cli.py benchmark --local
  ```

---

## Architectural Rules for Agents

1. **Frozen Specification Authority:** Follow `PROJECT_CONTEXT.md`. Never rename fields in `Job`, policy strings, or keys in `TickSnapshot`.
2. **Tick Engine Order:** The seven steps in `simulator/engine.py` (`step_tick()`) must run in exact sequential order:
   1. Ingest arrivals
   2. Advance running jobs
   3. Priority aging
   4. Medium-term scheduling (swap-in first, then swap-out)
   5. Long-term admission
   6. Short-term scheduling (quantum check -> fill free cores -> preemption)
   7. Record telemetry & build TickSnapshot
3. **psutil Isolation:** `psutil` must ONLY be imported and called inside `os_bridge/`.
4. **Safety & Subprocess Caps:** Live OS mode is disabled by default. When enabled, pass only integer arguments to worker subprocesses. Never exceed configured real RAM limits or active worker caps. Always register `atexit` cleanup.
5. **No External Databases:** All state is maintained in-memory in `simulator/`. Concurrency is guarded by a single `asyncio.Lock` in `app.py`.
