"""Host system hardware utilization sampler using psutil."""

import os
import psutil


def sample() -> dict:
    """Sample host CPU utilization per core and host RAM usage."""
    try:
        cpu_percentages = psutil.cpu_percent(interval=None, percpu=True)
        if not cpu_percentages:
            core_count = os.cpu_count() or 1
            cpu_percentages = [0.0] * core_count

        mem = psutil.virtual_memory()
        return {
            "cpu_percent_per_core": [float(c) for c in cpu_percentages],
            "ram_percent": round(float(mem.percent), 2),
            "ram_used_mb": int(mem.used // (1024 * 1024)),
        }
    except Exception:
        core_count = os.cpu_count() or 4
        return {
            "cpu_percent_per_core": [0.0] * core_count,
            "ram_percent": 0.0,
            "ram_used_mb": 0,
        }
