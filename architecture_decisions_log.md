# Architecture Decision Records (`docs/DECISIONS.md`)

> Documents key design choices and resolved edge cases so team members and AI agents do not re-introduce bugs.

---

### ADR-001: Unified Discrete Tick Engine Instead of SimPy
* **Context:** Combining `SimPy` coroutines with FastAPI WebSockets and `psutil` real processes creates complex thread/async synchronization issues.
* **Decision:** Use a deterministic discrete tick engine (`engine.step_tick()`). In Benchmark Mode, a `while not engine.finished:` loop runs 100 jobs in `< 30 ms`. In Live Mode, an `asyncio` loop calls `engine.step_tick()` once every `tick_duration_sec` (1.0s) and broadcasts the resulting `TickSnapshot` over WebSockets.

### ADR-002: Anti-Thrashing Hysteresis in Medium-Term Scheduler
* **Context:** If the Medium-Term Scheduler swaps out jobs whenever RAM > 85% and swaps them back in whenever RAM <= 85%, a single job can bounce between `READY` and `SUSPENDED` every tick.
* **Decision:**
  1. Use dual watermarks: swap out above **85%** (`memory_suspension_threshold`), swap in below **70%** (`memory_resume_threshold`), and ensure post-swap-in utilization stays $\le 85\%$.
  2. Only swap out when a job in `new_queue` is actually blocked by insufficient RAM.
  3. Enforce `min_residency_ticks = 3` so a newly resumed job cannot be immediately swapped out again.
  4. Run `swap_in` before `swap_out` and before Long-Term `admit` in Step 4 so previously admitted jobs get priority over brand-new arrivals.

### ADR-003: Separating Active Cores (`num_cpus`) from Total Spawned Workers (`max_live_workers`)
* **Context:** When a running job is preempted or swapped out in Live OS Mode, we call `psutil.Process(pid).suspend()` so the evaluator can see the process status change to **"Suspended"** in Windows Task Manager. That means suspended worker PIDs remain alive in the OS alongside the currently running worker PIDs.
* **Decision:** Enforce two distinct caps in `ProcessManager`:
  * Active (unsuspended) workers $\le \text{num\_cpus}$
  * Total spawned workers (active + suspended) $\le \text{max\_live\_workers}$ (16), and total scaled physical RAM $\le \text{max\_total\_real\_ram\_mb}$ (1024 MB).

### ADR-004: Priority Aging Persistence Across Preemptions
* **Context:** If a low-priority job ($P=5$) waits long enough in `READY` to age up to $P=3$, gets dispatched for 1 tick, and is immediately preempted by a $P=1$ job, resetting its priority back to $P=5$ would cause starvation again.
* **Decision:** When a job transitions out of `READY` (to `RUNNING` or `SUSPENDED`), reset `job.age_counter = 0`, but **preserve** its boosted `job.priority`.