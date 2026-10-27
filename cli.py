"""SLURM CLI client and terminal formatting utility."""

import argparse
import sys
import json
import httpx

from simulator.job import generate_workload
from metrics.benchmark import run_benchmark

DEFAULT_BASE_URL = "http://127.0.0.1:8000"


def format_squeue_table(jobs: list[dict]) -> str:
    """Format a list of active job dicts into SLURM squeue table string."""
    header = f"{'JOBID':<7} {'NAME':<15} {'USER':<7} {'PRIO':<6} {'MEM(GB)':<9} {'STATE':<11} {'TIME_REM':<10} {'CORE':<7} {'OS_PID'}"
    lines = [header]

    for j in jobs:
        jid = str(j.get("id", ""))
        name = str(j.get("name", ""))[:14]
        user = str(j.get("user", ""))[:6]
        prio = str(j.get("priority", ""))
        mem = str(j.get("memory_gb", ""))
        state = str(j.get("state", ""))
        burst = j.get("burst_time", 0)
        rem = j.get("remaining_time", 0)
        time_rem = f"{rem}/{burst}"
        assigned_cpu = j.get("assigned_cpu")
        core = f"CPU{assigned_cpu}" if assigned_cpu is not None else "-"
        os_pid = j.get("os_pid")
        if os_pid is not None:
            pid_str = f"{os_pid} (Susp)" if state == "SUSPENDED" else str(os_pid)
        else:
            pid_str = "-"

        lines.append(f"{jid:<7} {name:<15} {user:<7} {prio:<6} {mem:<9} {state:<11} {time_rem:<10} {core:<7} {pid_str}")

    return "\n".join(lines)


def format_sinfo_table(info: dict) -> str:
    """Format sinfo dictionary into SLURM partition summary table string."""
    header = f"{'PARTITION':<11} {'AVAIL':<7} {'TIMELIMIT':<11} {'CPUS(A/I/T)':<13} {'MEMORY(USED/TOTAL)':<20} {'STATE':<10} {'OS_BRIDGE'}"
    part = info.get("partition", "batch*")
    avail = info.get("avail", "up")
    tlimit = info.get("timelimit", "infinite")

    cpus = info.get("cpus", {})
    a = cpus.get("allocated", 0)
    i = cpus.get("idle", 0)
    t = cpus.get("total", 0)
    cpus_str = f"{a}/{i}/{t}"

    mem = info.get("memory_gb", {})
    used = mem.get("used", 0)
    total = mem.get("total", 0)
    pct = mem.get("utilization_percent", 0.0)
    mem_str = f"{used}G / {total}G ({pct}%)"

    state = info.get("state", "idle")
    os_b = info.get("os_bridge", "DISABLED")

    row = f"{part:<11} {avail:<7} {tlimit:<11} {cpus_str:<13} {mem_str:<20} {state:<10} {os_b}"
    return f"{header}\n{row}"


def format_sacct_table(jobs: list[dict]) -> str:
    """Format accounting job list into SLURM sacct table string."""
    header = f"{'JOBID':<7} {'NAME':<15} {'USER':<7} {'PRIO':<6} {'SUBMIT':<8} {'START':<7} {'END':<7} {'BURST':<7} {'WAIT':<6} {'TAT':<6} {'RESP':<6} {'SUSP':<6} {'STATE'}"
    lines = [header]

    for j in jobs:
        jid = str(j.get("id", ""))
        name = str(j.get("name", ""))[:14]
        user = str(j.get("user", ""))[:6]
        prio = str(j.get("priority", ""))
        submit = str(j.get("arrival_time", 0))
        start = str(j.get("first_run_time")) if j.get("first_run_time") is not None else "-"
        end = str(j.get("finish_time")) if j.get("finish_time") is not None else "-"
        burst = j.get("burst_time", 0)

        finish = j.get("finish_time")
        arr = j.get("arrival_time", 0)
        first = j.get("first_run_time")

        if finish is not None:
            tat_val = finish - arr
            wt_val = tat_val - burst
            tat_str = str(tat_val)
            wt_str = str(wt_val)
        else:
            tat_str = "-"
            wt_str = str(j.get("ready_wait_ticks", 0))

        resp_str = str(first - arr) if first is not None else "-"
        susp_str = str(j.get("suspension_count", 0))

        state = j.get("state", "")
        if j.get("cancelled"):
            state = "CANCELLED"
        elif state == "TERMINATED":
            state = "COMPLETED"

        lines.append(
            f"{jid:<7} {name:<15} {user:<7} {prio:<6} {submit:<8} {start:<7} {end:<7} {burst:<7} {wt_str:<6} {tat_str:<6} {resp_str:<6} {susp_str:<6} {state}"
        )

    return "\n".join(lines)


def run_cli_command(args: list[str], base_url: str = DEFAULT_BASE_URL) -> str:
    """Parse and execute a CLI command either locally or against running server."""
    parser = argparse.ArgumentParser(prog="cli.py", description="SLURM-style CLI for Cloud Batch Simulator")
    subparsers = parser.add_subparsers(dest="command")

    # squeue
    subparsers.add_parser("squeue", help="View active jobs queue")

    # sinfo
    subparsers.add_parser("sinfo", help="View partition and node status")

    # sacct
    subparsers.add_parser("sacct", help="View job accounting records")

    # sbatch
    sb = subparsers.add_parser("sbatch", help="Submit batch job")
    sb.add_argument("--name", default="batch-job")
    sb.add_argument("--user", default="alice")
    sb.add_argument("--burst", type=int, default=6)
    sb.add_argument("--mem", type=int, default=8)
    sb.add_argument("--priority", "--prio", type=int, default=3)

    # scancel
    sc = subparsers.add_parser("scancel", help="Cancel a job by ID")
    sc.add_argument("job_id", type=int)

    # benchmark
    bm = subparsers.add_parser("benchmark", help="Run 5-policy benchmark")
    bm.add_argument("--jobs", type=int, default=30)
    bm.add_argument("--seed", type=int, default=42)
    bm.add_argument("--local", action="store_true", help="Run benchmark locally in-memory")

    # step / start / pause / reset
    st = subparsers.add_parser("step", help="Step simulation by N ticks")
    st.add_argument("--ticks", type=int, default=1)
    subparsers.add_parser("start", help="Start background simulation loop")
    subparsers.add_parser("pause", help="Pause background simulation loop")
    subparsers.add_parser("reset", help="Reset simulation back to tick 0")

    try:
        parsed = parser.parse_args(args)
    except SystemExit:
        return "Invalid arguments or help requested."

    if not parsed.command:
        return parser.format_help()

    client = httpx.Client(base_url=base_url, timeout=5.0)

    try:
        if parsed.command == "squeue":
            res = client.get("/api/squeue")
            res.raise_for_status()
            return format_squeue_table(res.json().get("jobs", []))

        elif parsed.command == "sinfo":
            res = client.get("/api/sinfo")
            res.raise_for_status()
            return format_sinfo_table(res.json())

        elif parsed.command == "sacct":
            res = client.get("/api/sacct")
            res.raise_for_status()
            return format_sacct_table(res.json().get("jobs", []))

        elif parsed.command == "sbatch":
            payload = {
                "name": parsed.name,
                "user": parsed.user,
                "burst_time": parsed.burst,
                "memory_gb": parsed.mem,
                "priority": parsed.priority,
            }
            res = client.post("/api/jobs", json=payload)
            res.raise_for_status()
            return res.json().get("message", "Submitted batch job")

        elif parsed.command == "scancel":
            res = client.delete(f"/api/jobs/{parsed.job_id}")
            if res.status_code == 404:
                return f"scancel: error: Job {parsed.job_id} not found or already terminated"
            res.raise_for_status()
            return f"Cancelled job {parsed.job_id}"

        elif parsed.command == "benchmark":
            if parsed.local:
                workload = generate_workload(parsed.jobs, parsed.seed, 64)
                cfg = {
                    "cluster": {"num_cpus": 4, "total_memory_gb": 64},
                    "schedulers": {"long_term_policy": "memory_aware_fcfs", "medium_term_policy": "suspend_lowest_priority", "time_quantum": 4, "aging_interval_ticks": 6, "min_residency_ticks": 3, "max_swaps_per_tick": 1},
                }
                res_local = run_benchmark(cfg, workload)
                return json.dumps({"num_jobs": parsed.jobs, "seed": parsed.seed, "policies": res_local}, indent=2)
            else:
                res = client.post("/api/benchmark", json={"num_jobs": parsed.jobs, "seed": parsed.seed})
                res.raise_for_status()
                return json.dumps(res.json(), indent=2)

        elif parsed.command == "step":
            res = client.post("/api/control/step", json={"ticks": parsed.ticks})
            res.raise_for_status()
            return f"Advanced simulation by {parsed.ticks} tick(s). Current tick: {res.json().get('tick')}"

        elif parsed.command == "start":
            res = client.post("/api/control/start")
            res.raise_for_status()
            return "Simulation running."

        elif parsed.command == "pause":
            res = client.post("/api/control/pause")
            res.raise_for_status()
            return "Simulation paused."

        elif parsed.command == "reset":
            res = client.post("/api/control/reset")
            res.raise_for_status()
            return "Simulation reset to tick 0."

    except httpx.ConnectError:
        return f"Error: Cannot connect to server at {base_url}. Is uvicorn running? Use --local for local benchmark."
    except Exception as e:
        return f"Error executing command: {e}"

    return ""


if __name__ == "__main__":
    out = run_cli_command(sys.argv[1:])
    print(out)
