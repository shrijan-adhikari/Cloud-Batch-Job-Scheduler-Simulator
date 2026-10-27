# Web Control Plane UI Specification (`docs/DASHBOARD_SPEC.md`)

> Used by **Member 5** (`dashboard/index.html`, `dashboard/app.js`, `dashboard/styles.css`).

---

## 1. Visual Theme & Stack
* **Aesthetic:** Dark-mode Cloud & HPC Control Plane (inspired by AWS Batch, Grafana, and Kubernetes Dashboard).
* **Colors:**
  * Background: `#0b0f19` (deep slate), Cards: `#111827` with `#1f2937` borders.
  * State Badges:
    * `NEW`: Indigo (`#6366f1`)
    * `READY`: Cyan (`#06b6d4`)
    * `RUNNING`: Emerald (`#10b981`, pulsing dot)
    * `SUSPENDED`: Amber/Orange (`#f59e0b`, swap badge)
    * `TERMINATED`: Slate (`#64748b`)
* **Libraries (CDN):** TailwindCSS (`https://cdn.tailwindcss.com`) + Chart.js (`https://cdn.jsdelivr.net/npm/chart.js`). Zero build step required.

---

## 2. Layout Sections (Single-Page Control Plane)

### 2.1 Top Telemetry & Control Bar
* **Left:** Project Title + Live Tick Badge (`TICK #12`) + Connection Status (`WS CONNECTED`).
* **Center (Dual Telemetry Gauges):**
  * **Simulated Cluster:** `CPU: 75% (3/4 Cores)` | `RAM: 52 / 64 GB (81.2%)`
  * **Real Host PC (`psutil`):** `Host CPU: 34.2%` | `Host RAM: 62.1%` | `PIDs: 3 Active, 1 Suspended`
* **Right (Controls):**
  * Toggle Switch: **`Live OS Mode (Task Manager Sync)`**
  * Buttons: **`Play`**, **`Pause`**, **`Step +1`**, **`Reset`**, **`+ Submit Job`**

### 2.2 Scheduler Policy Configuration Strip
* Dropdowns that dynamically update or reset the cluster:
  * **Long-Term Policy:** `Memory-Aware FCFS` | `Priority Admission`
  * **Medium-Term Policy:** `Suspend Lowest Priority` | `Suspend Largest Memory`
  * **Short-Term Policy:** `FCFS` | `SJF` | `SRTF` | `Round Robin` | `Priority (with Aging)`
  * **Quantum / CPUs / RAM:** Quick selectors (`2/4/8 CPUs`, `Quantum: 2/4/6`).

### 2.3 Main 3-Tier Animated Pipeline Grid (4 Columns)
1. **Column 1 — Job Queue (`NEW`) + Long-Term Gate:**
   * Shows incoming jobs waiting for RAM admission.
2. **Column 2 — Ready Queue (`READY`) + Medium-Term Swapper:**
   * Shows admitted jobs holding RAM, sorted by current dispatch order, with priority and aging badges.
3. **Column 3 — Multi-CPU Core Grid (`RUNNING`):**
   * One card per CPU Core (`CPU 0` .. `CPU N-1`).
   * Displays active job name, progress bar (`remaining_time / burst_time`), quantum counter, RAM allocated, and a highlighted **`OS PID: 18420 (Pinned Core 0)`** badge when in Live OS Mode.
4. **Column 4 — Suspended Swap Queue (`SUSPENDED`) & Inspector:**
   * Shows jobs swapped out by the Medium-Term Scheduler when RAM crossed 85%.
   * Clicking any job card opens the **Job Inspector** with manual **Suspend / Resume / Cancel (`scancel`)** buttons.

### 2.4 Live Multi-Core Gantt Timeline & Memory Watermark Graph
* **Left Panel (Live Gantt Chart):** Horizontal timeline strip per CPU core (`CPU 0` .. `CPU N-1`) showing color-coded job blocks for the last 40 ticks. Hovering a block shows `Job ID, User, Tick, Priority`.
* **Right Panel (Live Cluster RAM & Queue Graph):** Real-time Chart.js line chart plotting `Used Memory %` against a dashed red horizontal line at **`85% (Medium-Term Suspend Threshold)`** and a dashed green line at **`70% (Resume Threshold)`**.

### 2.5 Policy Benchmark Comparison Tab / Modal
* Button: **`Run 5-Policy Benchmark (Seed: 42)`**
* Calls `POST /api/benchmark` and renders 4 side-by-side Chart.js bar charts comparing **FCFS, SJF, SRTF, Round Robin, and Priority** across:
  1. Average Waiting Time & Response Time (Lower is better)
  2. Average Turnaround Time (Lower is better)
  3. CPU Utilization % & Throughput (Higher is better)
  4. Medium-Term Suspensions & Starvation Count (Lower is better)
* Automatically highlights the winning policy for each metric in green.

### 2.6 Embedded SLURM Web Console & Live Scheduler Event Log (Bottom Split Drawer)
* **Left (Scheduler Event Log):** Color-coded stream of `[LT]`, `[MT]`, and `[ST]` decisions per tick.
* **Right (Interactive SLURM Terminal):** Prompt supporting `sbatch --name test --mem 24 --burst 6 --prio 1`, `squeue`, `sinfo`, `sacct`, `scancel <id>`, `step`, and `help`.