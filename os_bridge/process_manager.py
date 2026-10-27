"""ProcessManager controlling real worker child processes via psutil and subprocess."""

import atexit
import os
import subprocess
import sys
import time
from typing import Optional, Any
import psutil

from simulator.job import Job


class ProcessManager:
    """Manages real host worker processes representing simulated batch jobs."""

    def __init__(self, config: Optional[dict] = None) -> None:
        cfg = config or {}
        bridge_cfg = cfg.get("os_bridge", {})
        cluster_cfg = cfg.get("cluster", {})

        self.enabled: bool = bridge_cfg.get("enabled", True)
        self.ram_scale_mb_per_gb: int = bridge_cfg.get("ram_scale_mb_per_gb", 15)
        self.max_total_real_ram_mb: int = bridge_cfg.get("max_total_real_ram_mb", 1024)
        self.max_live_workers: int = bridge_cfg.get("max_live_workers", 16)
        self.num_cpus: int = cluster_cfg.get("num_cpus", 4)

        # workers: dict[job_id, worker_dict]
        self.workers: dict[int, dict[str, Any]] = {}

        # Ensure zombie cleanup on shutdown
        atexit.register(self.terminate_all)

    def _pin_affinity(self, proc: psutil.Process, core: int) -> None:
        """Attempt to set core affinity with fallback for OS platforms lacking affinity support."""
        try:
            num_host_cpus = os.cpu_count() or 1
            target_core = core % num_host_cpus
            proc.cpu_affinity([target_core])
        except (AttributeError, psutil.AccessDenied, psutil.NoSuchProcess, OSError):
            pass

    def _terminate_by_job_id(self, job_id: int) -> None:
        """Terminate and clean up a single worker by job ID."""
        worker_info = self.workers.pop(job_id, None)
        if not worker_info:
            return

        proc: Optional[psutil.Process] = worker_info.get("process")
        popen: Optional[subprocess.Popen] = worker_info.get("popen")

        try:
            if proc is not None and proc.is_running():
                proc.terminate()
                try:
                    proc.wait(timeout=1.0)
                except (psutil.TimeoutExpired, TimeoutError):
                    proc.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass

        try:
            if popen is not None and popen.poll() is None:
                popen.terminate()
                try:
                    popen.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    popen.kill()
        except (OSError, subprocess.SubprocessError):
            pass

    def start_or_resume(self, job: Job, core: int) -> None:
        """Spawn a new worker process or resume an existing suspended one."""
        if not self.enabled:
            return

        # Case 1: Worker already exists, resume it
        if job.id in self.workers:
            w = self.workers[job.id]
            proc: psutil.Process = w["process"]
            try:
                if proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE:
                    if w.get("status") == "suspended":
                        proc.resume()
                    w["status"] = "running"
                    w["core"] = core
                    job.os_pid = proc.pid
                    job.assigned_cpu = core
                    self._pin_affinity(proc, core)
                    return
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                pass
            # Process was dead; remove entry and re-spawn below
            self.workers.pop(job.id, None)

        # Case 2: Spawn new worker process
        # Check max_live_workers limit (16)
        if len(self.workers) >= self.max_live_workers:
            suspended_workers = [
                (w["spawn_time"], jid)
                for jid, w in self.workers.items()
                if w.get("status") == "suspended"
            ]
            if suspended_workers:
                _, oldest_jid = min(suspended_workers)
                self._terminate_by_job_id(oldest_jid)
            else:
                _, oldest_jid = min((w["spawn_time"], jid) for jid, w in self.workers.items())
                self._terminate_by_job_id(oldest_jid)

        # Calculate safe scaled RAM
        current_used_mb = sum(w.get("allocated_mb", 0) for w in self.workers.values())
        desired_mb = int(job.memory_gb * self.ram_scale_mb_per_gb)
        if current_used_mb + desired_mb > self.max_total_real_ram_mb:
            remaining_mb = max(0, self.max_total_real_ram_mb - current_used_mb)
            allocated_mb = max(16, remaining_mb // 2)
        else:
            allocated_mb = max(16, desired_mb)

        cmd = [
            sys.executable,
            "-m",
            "os_bridge.worker",
            "--job-id",
            str(int(job.id)),
            "--mem-mb",
            str(int(allocated_mb)),
        ]

        popen = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        pid = popen.pid
        proc = psutil.Process(pid)

        job.os_pid = pid
        job.assigned_cpu = core
        self.workers[job.id] = {
            "job_id": job.id,
            "pid": pid,
            "popen": popen,
            "process": proc,
            "allocated_mb": allocated_mb,
            "core": core,
            "status": "running",
            "spawn_time": time.time(),
        }
        self._pin_affinity(proc, core)

    def suspend(self, job: Job) -> None:
        """Suspend worker process in the OS (shows 'Suspended' in Task Manager)."""
        if not self.enabled or job.id not in self.workers:
            return

        w = self.workers[job.id]
        proc: psutil.Process = w["process"]
        try:
            if proc.is_running():
                proc.suspend()
            w["status"] = "suspended"
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            pass

    def terminate(self, job: Job) -> None:
        """Terminate worker process and clean up PIDs."""
        if job.id in self.workers:
            self._terminate_by_job_id(job.id)
        job.os_pid = None

    def terminate_all(self) -> None:
        """Terminate all running and suspended workers and release PIDs."""
        for jid in list(self.workers.keys()):
            self._terminate_by_job_id(jid)
        self.workers.clear()

    def status(self) -> list[dict]:
        """Return diagnostic status of all currently tracked worker processes."""
        result = []
        for jid, w in list(self.workers.items()):
            try:
                proc: psutil.Process = w["process"]
                rss_mb = round(proc.memory_info().rss / (1024 * 1024), 1)
                os_status = proc.status()
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                rss_mb = 0.0
                os_status = "dead"

            result.append({
                "job_id": jid,
                "pid": w["pid"],
                "rss_mb": rss_mb,
                "core": w["core"],
                "status": w["status"],
                "os_status": os_status,
            })
        return result

    def sample_host(self) -> dict:
        """Sample host telemetry augmented with active and suspended worker counts."""
        from os_bridge import host_monitor
        base = host_monitor.sample()
        base["active_workers"] = sum(1 for w in self.workers.values() if w.get("status") == "running")
        base["suspended_workers"] = sum(1 for w in self.workers.values() if w.get("status") == "suspended")
        return base
