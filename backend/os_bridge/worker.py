"""Worker process payload allocating real memory and generating observable CPU activity."""

import argparse
import math
import sys
import time


def main() -> None:
    """Parse CLI arguments, commit physical memory, and spin CPU in controlled cycles."""
    parser = argparse.ArgumentParser(description="Live OS Worker Process")
    parser.add_argument("--job-id", type=int, required=True, help="Numeric job ID")
    parser.add_argument("--mem-mb", type=int, required=True, help="Memory to allocate in MB")

    args = parser.parse_args()

    mem_bytes = max(1, args.mem_mb) * 1024 * 1024
    try:
        # Allocate real memory buffer
        buffer = bytearray(mem_bytes)
        # Touch each 4096-byte OS page to force page fault and physical memory allocation
        for page_offset in range(0, len(buffer), 4096):
            buffer[page_offset] = 1
    except MemoryError:
        # Fallback if host RAM is constrained
        buffer = bytearray(1024 * 1024)

    # Controlled CPU loop: ~30ms light calculation, ~20ms sleep
    try:
        val = 0.0
        while True:
            cycle_start = time.perf_counter()
            while (time.perf_counter() - cycle_start) < 0.030:
                val = math.sqrt(val + 1.0) % 1000.0
            time.sleep(0.020)
    except (KeyboardInterrupt, SystemExit):
        sys.exit(0)


if __name__ == "__main__":
    main()
