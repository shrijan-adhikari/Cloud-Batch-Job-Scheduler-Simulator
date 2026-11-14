"""Slurm-style CLI client and standalone local benchmark utility."""

import argparse
import json
import os
import sys
from typing import Optional
import requests

from simulator.job import generate_workload
from metrics.benchmark import run_benchmark

DEFAULT_SERVER_URL = os.environ.get("SCHEDULER_URL", "http://127.0.0.1:8000")


def get_server_url(args: argparse.Namespace) -> str:
    """Return the targeted server base URL."""
    return getattr(args, "server", DEFAULT_SERVER_URL).rstrip("/")


def cmd_sbatch(args: argparse.Namespace) -> None:
    """Submit a batch job through the REST API."""
    url = f"{get_server_url(args)}/api/jobs/sbatch"
    payload = {
        "name": args.name,
        "user": args.user,
        "burst_time": args.burst,
        "memory_gb": args.memory,
        "priority": args.priority,
    }
    try:
        resp = requests.post(url, json=payload, timeout=5)
        resp.raise_for_status()
        data = resp.json()
        print(f"Submitted batch job {data.get('job_id')}")
    except Exception as e:
        print(f"Error submitting job: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_squeue(args: argparse.Namespace) -> None:
    """Query and display active jobs across simulation queues."""
    url = f"{get_server_url(args)}/api/jobs/squeue"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        if not jobs:
            print("No active jobs in queue.")
            return

        header = f"{'JOBID':<8} {'NAME':<16} {'USER':<10} {'STATE':<12} {'BURST':<7} {'REMAIN':<8} {'MEM(GB)':<8} {'PRIO':<6} {'CPU':<5}"
        print(header)
        print("-" * len(header))
        for j in jobs:
            cpu_str = str(j.get("assigned_cpu")) if j.get("assigned_cpu") is not None else "-"
            print(
                f"{j.get('id'):<8} "
                f"{j.get('name')[:15]:<16} "
                f"{j.get('user')[:9]:<10} "
                f"{j.get('state'):<12} "
                f"{j.get('burst_time'):<7} "
                f"{j.get('remaining_time'):<8} "
                f"{j.get('memory_gb'):<8} "
                f"{j.get('priority'):<6} "
                f"{cpu_str:<5}"
            )
    except Exception as e:
        print(f"Error fetching squeue: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_sinfo(args: argparse.Namespace) -> None:
    """Display cluster node allocation and memory headroom."""
    url = f"{get_server_url(args)}/api/jobs/sinfo"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        info = resp.json()

        cores = info.get("cores", [])
        busy = sum(1 for c in cores if c is not None)
        total_cpus = info.get("num_cpus", len(cores))

        print(f"Cluster Core Status: {busy}/{total_cpus} cores active")
        for idx, job_id in enumerate(cores):
            status = f"JOB-{job_id}" if job_id is not None else "IDLE"
            print(f"  Core {idx}: {status}")

        used_mem = info.get("used_memory_gb", 0)
        tot_mem = info.get("total_memory_gb", 0)
        avail_mem = info.get("available_memory_gb", 0)
        mem_util = info.get("memory_utilization", 0.0) * 100.0

        print(f"\nMemory Accounting: {used_mem} GB used / {tot_mem} GB total ({avail_mem} GB free, {mem_util:.1f}% util)")
    except Exception as e:
        print(f"Error fetching sinfo: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_sacct(args: argparse.Namespace) -> None:
    """Display historical and accounting data for all jobs."""
    url = f"{get_server_url(args)}/api/jobs/sacct"
    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        if not jobs:
            print("No accounting records found.")
            return

        header = f"{'JOBID':<8} {'NAME':<16} {'USER':<10} {'STATE':<12} {'BURST':<7} {'ARRIVED':<9} {'START':<7} {'FINISH':<8} {'WAIT':<6}"
        print(header)
        print("-" * len(header))
        for j in jobs:
            start_str = str(j.get("first_run_time")) if j.get("first_run_time") is not None else "-"
            finish_str = str(j.get("finish_time")) if j.get("finish_time") is not None else "-"
            arr = j.get("arrival_time", 0)
            fin = j.get("finish_time")
            burst = j.get("burst_time", 0)
            wait_str = str((fin - arr) - burst) if fin is not None else "-"

            print(
                f"{j.get('id'):<8} "
                f"{j.get('name')[:15]:<16} "
                f"{j.get('user')[:9]:<10} "
                f"{j.get('state'):<12} "
                f"{burst:<7} "
                f"{arr:<9} "
                f"{start_str:<7} "
                f"{finish_str:<8} "
                f"{wait_str:<6}"
            )
    except Exception as e:
        print(f"Error fetching sacct: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_scancel(args: argparse.Namespace) -> None:
    """Cancel a target job."""
    url = f"{get_server_url(args)}/api/jobs/scancel/{args.job_id}"
    try:
        resp = requests.post(url, timeout=5)
        resp.raise_for_status()
        print(f"Cancelled job {args.job_id}")
    except Exception as e:
        print(f"Error cancelling job {args.job_id}: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_benchmark(args: argparse.Namespace) -> None:
    """Run benchmark comparison across all 5 scheduling policies."""
    is_local = getattr(args, "local", False)

    if is_local:
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        benchmark_results = run_benchmark(cfg)
    else:
        url = f"{get_server_url(args)}/api/benchmark/run"
        try:
            resp = requests.post(url, timeout=30)
            resp.raise_for_status()
            benchmark_results = resp.json().get("benchmark", {})
        except Exception as e:
            print(f"Error executing remote benchmark: {e}", file=sys.stderr)
            print("Tip: Run locally using 'python cli.py benchmark --local'", file=sys.stderr)
            sys.exit(1)

    print("\n======================= BENCHMARK POLICY COMPARISON =======================")
    header = f"{'POLICY':<14} {'AVG WT':<9} {'AVG TAT':<9} {'AVG RT':<9} {'CPU %':<8} {'MEM %':<8} {'THROUGHPUT':<12} {'STARVED':<8} {'SWAPS':<7}"
    print(header)
    print("-" * len(header))
    for policy, m in benchmark_results.items():
        print(
            f"{policy:<14} "
            f"{m.get('avg_waiting_time', 0.0):<9.2f} "
            f"{m.get('avg_turnaround_time', 0.0):<9.2f} "
            f"{m.get('avg_response_time', 0.0):<9.2f} "
            f"{m.get('cpu_utilization', 0.0):<8.2f} "
            f"{m.get('memory_utilization', 0.0):<8.2f} "
            f"{m.get('throughput', 0.0):<12.2f} "
            f"{m.get('starvation_count', 0):<8} "
            f"{m.get('suspensions', 0):<7}"
        )
    print("===========================================================================\n")


def main() -> None:
    """Parse top-level CLI command and dispatch to handler."""
    parser = argparse.ArgumentParser(description="Slurm-style batch scheduler CLI")
    parser.add_argument("--server", default=DEFAULT_SERVER_URL, help="Server base URL")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # sbatch
    p_sbatch = subparsers.add_parser("sbatch", help="Submit a batch job")
    p_sbatch.add_argument("--name", required=True, help="Job name")
    p_sbatch.add_argument("--burst", type=int, required=True, help="CPU burst ticks")
    p_sbatch.add_argument("--memory", type=int, required=True, help="Memory in GB")
    p_sbatch.add_argument("--priority", type=int, default=3, help="Priority 1-5 (1 highest)")
    p_sbatch.add_argument("--user", default="user", help="User name")
    p_sbatch.set_defaults(func=cmd_sbatch)

    # squeue
    p_squeue = subparsers.add_parser("squeue", help="List active jobs")
    p_squeue.set_defaults(func=cmd_squeue)

    # sinfo
    p_sinfo = subparsers.add_parser("sinfo", help="Cluster status")
    p_sinfo.set_defaults(func=cmd_sinfo)

    # sacct
    p_sacct = subparsers.add_parser("sacct", help="Job accounting")
    p_sacct.set_defaults(func=cmd_sacct)

    # scancel
    p_scancel = subparsers.add_parser("scancel", help="Cancel a job")
    p_scancel.add_argument("job_id", type=int, help="Numeric Job ID")
    p_scancel.set_defaults(func=cmd_scancel)

    # benchmark
    p_bench = subparsers.add_parser("benchmark", help="Run benchmark across 5 policies")
    p_bench.add_argument(
        "--local", "-local", action="store_true", help="Execute benchmark locally without server"
    )
    p_bench.set_defaults(func=cmd_benchmark)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
