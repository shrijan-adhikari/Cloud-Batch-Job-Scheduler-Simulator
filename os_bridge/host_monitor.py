"""Host resource monitoring using psutil."""

import psutil


def sample() -> dict:
    """Sample real host per-core CPU% and system RAM% via psutil."""
    cpu_percent_per_core = psutil.cpu_percent(interval=None, percpu=True)
    vmem = psutil.virtual_memory()

    return {
        "cpu_percent_per_core": cpu_percent_per_core,
        "ram_percent": round(vmem.percent, 1),
        "ram_used_mb": int(vmem.used / (1024 * 1024)),
    }
