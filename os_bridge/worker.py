"""Worker process payload executed as a real OS child process."""

import argparse
import math
import sys
import time


def run_worker(job_id: int, mem_mb: int) -> None:
    """Allocate physical RAM and spin lightly on CPU."""
    # 1. Allocate buffer and touch memory pages to force real RSS allocation
    try:
        buffer_size = max(1, mem_mb) * 1024 * 1024
        buf = bytearray(buffer_size)
        step = 4096
        for i in range(0, buffer_size, step):
            buf[i] = 1
    except MemoryError:
        buf = bytearray(1024 * 1024)

    # 2. Light CPU spin loop (30 ms compute, 20 ms sleep)
    try:
        while True:
            t_end = time.perf_counter() + 0.030
            val = 12345.6789
            while time.perf_counter() < t_end:
                val = math.sqrt(val * val + 1.0)
            time.sleep(0.020)
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real OS worker process payload.")
    parser.add_argument("--job-id", type=int, required=True, help="Job identifier")
    parser.add_argument("--mem-mb", type=int, default=16, help="Real RAM allocation in MB")
    args = parser.parse_args()

    run_worker(args.job_id, args.mem_mb)
