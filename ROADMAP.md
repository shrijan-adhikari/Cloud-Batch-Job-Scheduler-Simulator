# Solo Developer & AI Agent Build Roadmap (`docs/ROADMAP.md`)

> **Rule for AI Agent:** Execute this roadmap strictly one phase at a time. Do NOT start Phase N+1 until all checkboxes and `pytest` verification steps in Phase N pass with zero errors. Update checkboxes `[x]` as you complete each step.

## Phase 1: Core Data Models, Config & Admission (Foundation)
- [x] Create `requirements.txt` (`fastapi`, `uvicorn`, `psutil`, `pydantic`, `pytest`, `httpx`) and `config.json`.
- [x] Implement `simulator/job.py` (`Job`, `JobState`, `generate_workload`, `load_workload`, `clone_jobs`).
- [x] Implement `simulator/cluster.py` (`Cluster` state, RAM tracking, core assignment, Gantt history).
- [x] Implement `simulator/long_term.py` (`memory_aware_fcfs` and `priority_admission`).
- [x] Create `tests/golden_workload.json` (3-job test) and `tests/sample_jobs.json` (15-job test).
- [x] **Gate 1 Verification:** Run `pytest` on job generation determinism, cluster RAM invariants, and long-term admission.

## Phase 2: Short-Term & Medium-Term Schedulers + Unified Tick Engine
- [x] Implement `simulator/short_term.py` (`fcfs`, `sjf`, `srtf`, `round_robin`, `priority` + aging + tie-breaking).
- [x] Implement `simulator/medium_term.py` (`swap_in`, `swap_out` with 85%/70% hysteresis and anti-thrash residency check).
- [x] Implement `simulator/engine.py` (`Engine` class following the exact 7-step tick order in `PROJECT_CONTEXT.md` §4.4).
- [x] Write `tests/test_schedulers.py` and `tests/test_engine_invariants.py`.
- [x] **Gate 2 Verification:** Run `pytest -q` and confirm all 100-job engine invariants (no negative RAM, no double-assigned cores) pass.

## Phase 3: Metrics Calculator & 5-Policy Benchmark Engine
- [x] Implement `metrics/calculator.py` (exact formulas from `PROJECT_CONTEXT.md` §6).
- [x] Implement `metrics/benchmark.py` (`run_benchmark` across all 5 policies on deep-copied workloads).
- [x] Write `tests/test_metrics_golden.py` verifying the hand-calculated golden numbers in `docs/TESTING.md`.
- [x] **Gate 3 Verification:** Run `pytest -q` and confirm < 200 ms execution time for a 100-job 5-policy benchmark.

## Phase 4: Real OS Task Manager Bridge (`psutil`)
- [x] Implement `os_bridge/worker.py` (allocates scaled bytearray RAM, touches pages, runs light CPU loop).
- [x] Implement `os_bridge/process_manager.py` (`start_or_resume`, `suspend`, `terminate`, `terminate_all`, `cpu_affinity` with OS fallback, `atexit` cleanup).
- [x] Implement `os_bridge/host_monitor.py` (live per-core CPU% and RAM% via `psutil`).
- [x] **Gate 4 Verification:** Write a quick test spawning 2 workers, suspending 1, verifying status via `psutil`, and cleaning up all PIDs.

## Phase 5: FastAPI Backend, WebSockets & SLURM CLI
- [x] Implement `app.py` (REST endpoints + WebSocket `/ws` tick loop guarded by `asyncio.Lock` per `docs/API_CONTRACT.md`).
- [x] Implement `cli.py` (`sbatch`, `squeue`, `sinfo`, `sacct`, `scancel`, `benchmark`).
- [x] Write `tests/test_api.py` using FastAPI `TestClient`.
- [x] **Gate 5 Verification:** Run `pytest -q` across the entire test suite.

## Phase 6: Dark-Mode Cloud Control Plane Dashboard
- [x] Build `dashboard/index.html`, `dashboard/styles.css`, and `dashboard/app.js` per `docs/DASHBOARD_SPEC.md`.
- [x] Wire live WebSocket updates to the Dual Telemetry Bar, 3-Tier Animated Queue Pipeline, Multi-Core Gantt Chart, Memory Watermark Graph, All-Policy Benchmark Modal, and Embedded SLURM Web Terminal.
- [x] **Final Verification:** Launch `python -m uvicorn app:app --reload`, test both Benchmark Mode and Live OS Task Manager Mode in the browser.