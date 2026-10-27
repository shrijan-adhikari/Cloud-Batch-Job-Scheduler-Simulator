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
        is_adopted: bool = worker_info.get("is_adopted", False)

        # For adopted tasks from host Task Manager, safely un-suspend without terminating
        if is_adopted:
            if proc is not None:
                try:
                    if proc.is_running() and worker_info.get("status") == "suspended":
                        proc.resume()
                except Exception:
                    pass
            return

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

        runner_exe = self._get_runner_executable(job.name)
        cmd = [
            runner_exe,
            "-m",
            "os_bridge.worker",
            "--job-id",
            str(int(job.id)),
            "--mem-mb",
            str(int(allocated_mb)),
            "--job-name",
            str(job.name),
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
            "is_adopted": False,
        }
        self._pin_affinity(proc, core)

    def _get_runner_executable(self, job_name: str) -> str:
        """Create or return a named Python runner so the process appears under job_name in Task Manager."""
        if not sys.platform.startswith("win"):
            return sys.executable

        try:
            temp_bin = os.path.join(os.environ.get("TEMP", os.path.expanduser("~")), "CloudBatch_Runners")
            os.makedirs(temp_bin, exist_ok=True)
            clean_name = "".join(c if (c.isalnum() or c in "-_") else "_" for c in job_name.strip())
            if not clean_name:
                clean_name = "batch-worker"
            target_exe = os.path.join(temp_bin, f"{clean_name}.exe")

            if not os.path.exists(target_exe):
                with open(sys.executable, "rb") as f:
                    data = bytearray(f.read())

                # Clear Resource DataDirectory in PE OptionalHeader so Windows Task Manager
                # displays the custom executable name without grouping under 'Python (N)'
                pe_offset = int.from_bytes(data[0x3C:0x40], "little")
                magic = int.from_bytes(data[pe_offset + 24 : pe_offset + 26], "little")
                if magic == 0x20B:  # PE32+ (64-bit)
                    res_entry = pe_offset + 24 + 112 + 16
                else:  # PE32 (32-bit)
                    res_entry = pe_offset + 24 + 96 + 16

                if res_entry + 8 <= len(data):
                    data[res_entry : res_entry + 8] = b"\x00" * 8

                with open(target_exe, "wb") as f:
                    f.write(data)

            return target_exe
        except Exception:
            return sys.executable


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

    def get_user_processes(self) -> list[dict]:
        """Scan active processes running on host Task Manager that can be adopted into scheduler."""
        blacklist = {
            "system", "system idle process", "registry", "smss.exe", "csrss.exe",
            "wininit.exe", "services.exe", "lsass.exe", "svchost.exe", "fontdrvhost.exe",
            "winlogon.exe", "dwm.exe", "explorer.exe", "taskmgr.exe", "antigravity.exe",
            "python.exe"
        }
        tracked_pids = {w["pid"] for w in self.workers.values()}
        results = []
        for p in psutil.process_iter(['pid', 'name', 'username', 'memory_info', 'cpu_percent']):
            try:
                info = p.info
                pname = (info.get("name") or "").lower()
                pid = info.get("pid", 0)
                if pid <= 4 or pname in blacklist or pid in tracked_pids:
                    continue
                mem_mb = round(info["memory_info"].rss / (1024 * 1024), 1) if info.get("memory_info") else 0.0
                results.append({
                    "pid": pid,
                    "name": info.get("name") or f"Process-{pid}",
                    "memory_mb": mem_mb,
                    "memory_gb": max(1, int(round(mem_mb / 1024))),
                    "cpu_percent": round(info.get("cpu_percent") or 0.0, 1),
                    "username": info.get("username") or "user",
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                continue
        results.sort(key=lambda x: x["memory_mb"], reverse=True)
        return results[:35]

    def adopt_process(self, pid: int, job: Job, core: int = 0) -> bool:
        """Adopt an already running host process from Windows Task Manager into scheduler management."""
        if not psutil.pid_exists(pid):
            return False
        try:
            proc = psutil.Process(pid)
            mem_mb = round(proc.memory_info().rss / (1024 * 1024), 1)
            job.os_pid = pid
            job.assigned_cpu = core
            job.name = f"{proc.name()}"
            self.workers[job.id] = {
                "job_id": job.id,
                "pid": pid,
                "popen": None,
                "process": proc,
                "allocated_mb": mem_mb,
                "core": core,
                "status": "running",
                "spawn_time": time.time(),
                "is_adopted": True,
            }
            self._pin_affinity(proc, core)
            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
            return False

