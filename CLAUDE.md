# CLAUDE.md — AI Coding Agent Instructions

> **Note:** Also applies to Cursor, Windsurf, GitHub Copilot, and OpenAI Codex (`AGENTS.md`).

## 1. Read First
Before generating or editing any code, read **`PROJECT_CONTEXT.md`** (v2.1). It is the single source of truth for all data structures, function signatures, tick execution order, and JSON contracts.

## 2. Quick Commands
```bash
# Install dependencies
pip install -r requirements.txt

# Run automated test suite (must pass 100% before completing any task)
pytest -q

# Start FastAPI + WebSocket server + Web Control Plane
python -m uvicorn app:app --reload --host 127.0.0.1 --port 8000

# Run SLURM CLI commands
python cli.py sinfo
python cli.py squeue
python cli.py sbatch --name "train-gpt" --mem 16 --burst 6 --priority 1
python cli.py scancel 101
python cli.py benchmark --jobs 30 --seed 42 --local
```

## 3. Non-Negotiable Architecture Rules
1. **Strict 7-Step Tick Sequence (`simulator/engine.py`):**
   Every call to `engine.step_tick()` must execute in this exact order:
   `(0) External commands (sbatch/scancel)` $\to$ `(1) Arrivals` $\to$ `(2) Advance running jobs` $\to$ `(3) Priority aging` $\to$ `(4) Medium-term scheduler (swap-in then swap-out)` $\to$ `(5) Long-term scheduler` $\to$ `(6) Short-term scheduler (quantum check, fill free cores, preempt)` $\to$ `(7) Record telemetry & increment tick`.
2. **Strict Separation of Simulation and OS Bridge:**
   * `simulator/*` and `metrics/*` **MUST NEVER** import `psutil` or `subprocess`.
   * All real OS process management lives exclusively in `os_bridge/`.
   * When `os_bridge` is `None` (Benchmark Mode and unit tests), the engine runs purely in memory at >10,000 ticks/sec.
3. **Cross-Platform Safety (`os_bridge/`):**
   * Always wrap `psutil.Process(pid).cpu_affinity(...)` in `try ... except (AttributeError, psutil.AccessDenied, OSError): pass` so macOS, Linux, and Windows all work seamlessly.
   * Never exceed `num_cpus` actively running workers or `max_live_workers` (16) total spawned worker PIDs (including suspended ones).
   * Register `atexit.register(self.terminate_all)` in `ProcessManager` so zombie processes are impossible.
4. **Tie-Breaking Rule Everywhere:**
   * Break ties by `(arrival_time, id)` ascending.
   * For `fcfs` and `round_robin`, preserve strict FIFO order in `ready_queue` (newly admitted, resumed, or preempted jobs are appended to the **tail** of `ready_queue`).
5. **Golden Test Verification:**
   * Do not alter the hand-verified expected values in `docs/TESTING.md`. If a test fails, fix the scheduler implementation to match `PROJECT_CONTEXT.md`.