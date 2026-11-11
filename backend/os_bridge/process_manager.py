"""Process Manager controlling live OS worker processes, core affinity, and memory scaling."""

import atexit
import os
import subprocess
import sys
import time
from typing import Optional
import psutil

from simulator.job import Job


class ProcessManager:
    """Spawns, pins, suspends, resumes, and terminates real OS worker processes."""

    def __init__(self, config: dict) -> None:
        """Initialize worker parameters and register emergency shutdown cleanup."""
        bridge_cfg = config.get("os_bridge", {})
        self.ram_scale_mb_per_gb: int = bridge_cfg.get("ram_scale_mb_per_gb", 15)
        self.max_total_real_ram_mb: int = bridge_cfg.get("max_total_real_ram_mb", 1024)
        self.max_live_workers: int = bridge_cfg.get("max_live_workers", 16)

        # Map of job_id -> worker metadata dict
        self.workers: dict[int, dict] = {}

        # Register exit hook so child processes are cleanly killed on interpreter exit
        atexit.register(self.terminate_all)

    def start_or_resume(self, job: Job, core: int) -> None:
        """Spawn a new worker process or resume an existing suspended worker."""
        if job.id in self.workers:
            worker = self.workers[job.id]
            pid = worker["pid"]
            try:
                p = psutil.Process(pid)
                p.resume()
                worker["status"] = "running"
                worker["core"] = core
                self._pin_affinity(pid, core)
                job.os_pid = pid
                return
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # Worker died unexpectedly; clean up and respawn
                self._kill_worker(job.id)

        # Enforce worker cap: if at max_live_workers, terminate oldest suspended worker
        if len(self.workers) >= self.max_live_workers:
            suspended_ids = [
                jid for jid, info in self.workers.items() if info.get("status") == "suspended"
            ]
            if suspended_ids:
                oldest_suspended_id = min(
                    suspended_ids, key=lambda jid: self.workers[jid].get("spawn_time", 0.0)
                )
                self._kill_worker(oldest_suspended_id)

        # Compute safe scaled RAM
        requested_mb = job.memory_gb * self.ram_scale_mb_per_gb
        current_real_ram = sum(w.get("mem_mb", 0) for w in self.workers.values())
        remaining_ram = max(0, self.max_total_real_ram_mb - current_real_ram)

        if current_real_ram + requested_mb > self.max_total_real_ram_mb:
            allocated_mb = max(16, remaining_ram // 2)
        else:
            allocated_mb = max(16, requested_mb)

        # Spawn worker process using integer-only arguments
        cmd = [
            sys.executable,
            "-m",
            "os_bridge.worker",
            "--job-id",
            str(int(job.id)),
            "--mem-mb",
            str(int(allocated_mb)),
        ]

        try:
            proc = subprocess.Popen(cmd)
            pid = proc.pid
            job.os_pid = pid
            self.workers[job.id] = {
                "proc": proc,
                "pid": pid,
                "mem_mb": allocated_mb,
                "core": core,
                "status": "running",
                "spawn_time": time.time(),
            }
            self._pin_affinity(pid, core)
        except Exception:
            job.os_pid = None

    def suspend(self, job: Job) -> None:
        """Suspend execution of the real OS worker process."""
        if job.id not in self.workers:
            return
        worker = self.workers[job.id]
        pid = worker["pid"]
        try:
            p = psutil.Process(pid)
            p.suspend()
            worker["status"] = "suspended"
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    def terminate(self, job: Job) -> None:
        """Terminate the worker process associated with a completed or cancelled job."""
        if job.id in self.workers:
            self._kill_worker(job.id)
            job.os_pid = None

    def terminate_all(self) -> None:
        """Safely terminate all active and suspended child worker processes."""
        for job_id in list(self.workers.keys()):
            self._kill_worker(job_id)

    def status(self) -> list[dict]:
        """Return resource telemetry for all managed worker processes."""
        result: list[dict] = []
        for job_id, worker in self.workers.items():
            pid = worker["pid"]
            rss_mb = worker.get("mem_mb", 0)
            status_str = worker.get("status", "unknown")
            try:
                p = psutil.Process(pid)
                rss_mb = int(p.memory_info().rss // (1024 * 1024))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                status_str = "dead"

            result.append({
                "job_id": job_id,
                "pid": pid,
                "rss_mb": rss_mb,
                "core": worker.get("core"),
                "status": status_str,
            })
        return result

    def _pin_affinity(self, pid: int, core: int) -> None:
        """Pin process affinity to the designated CPU core if supported by the host OS."""
        try:
            p = psutil.Process(pid)
            host_cores = os.cpu_count() or 1
            p.cpu_affinity([core % host_cores])
        except (AttributeError, psutil.AccessDenied, OSError, psutil.NoSuchProcess):
            pass

    def _kill_worker(self, job_id: int) -> None:
        """Internal helper to terminate and wait on a worker process."""
        worker = self.workers.pop(job_id, None)
        if not worker:
            return
        proc = worker.get("proc")
        if proc:
            try:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=1)
            except Exception:
                pass
