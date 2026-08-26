"""
===============================================================================
                    MASTER BENCHMARK SUITE ORCHESTRATOR
===============================================================================
Runs the full CognoDB benchmark pipeline end-to-end, in order:

  1. download_data.py         - fetch & summarize MovieLens dataset
  2. load_data.py              - ingest identical graph into all 5 platforms
  3. workloads/lookups.py      - point + filtered lookup latencies
  4. workloads/traversals.py   - 1/2/3-hop traversal latencies
  5. workloads/aggregations.py - global/group-by aggregation latencies
  6. workloads/concurrency.py  - concurrent mixed read/write throughput  [TODO: not yet implemented]
  7. workloads/footprint.py    - observable storage/memory footprint    [TODO: not yet implemented]
  8. generate_report.py        - consolidate all results into charts + summary table

Each step runs as its own subprocess (matching how you already run these
scripts manually), so a crash in one step prints its error and the failure is
logged, rather than taking down the whole suite. This mirrors the fail-fast /
keep-going philosophy already used inside the workload scripts themselves.

Usage:
  python run_suite.py                  # run every implemented step
  python run_suite.py --skip-ingestion # skip step 1-2 (data already loaded)
  python run_suite.py --only lookups traversals   # run just these steps
===============================================================================
"""

import argparse
import json
import os
import subprocess
import sys
import time

RESULTS_DIR = "results"
SUITE_LOG_PATH = os.path.join(RESULTS_DIR, "suite_run_log.json")

# (step_id, description, command, required)
# required=False steps that are missing (not yet implemented) are reported
# as skipped rather than failed.
STEPS = [
    ("download", "Download & prepare MovieLens dataset", [sys.executable, "download_data.py"], True),
    ("ingestion", "Load identical graph into all 5 platforms", [sys.executable, "load_data.py"], True),
    ("lookups", "Point + filtered lookup benchmarks", [sys.executable, os.path.join("workloads", "lookups.py")], True),
    ("traversals", "1/2/3-hop traversal benchmarks", [sys.executable, os.path.join("workloads", "traversals.py")], True),
    ("aggregations", "Aggregation benchmarks", [sys.executable, os.path.join("workloads", "aggregations.py")], True),
    ("concurrency", "Concurrent mixed read/write throughput", [sys.executable, os.path.join("workloads", "concurrency.py")], False),
    ("footprint", "Observable storage/memory footprint", [sys.executable, os.path.join("workloads", "footprint.py")], False),
    ("report", "Consolidate results into charts + summary", [sys.executable, "generate_report.py"], True),
]


def run_step(step_id, description, command, required):
    print(f"\n{'=' * 70}")
    print(f" STEP: {step_id}  —  {description}")
    print(f"{'=' * 70}")

    script_path = command[-1]
    if not os.path.exists(script_path):
        status = "skipped_not_implemented" if not required else "skipped_missing_required"
        msg = f"[{step_id}] Script not found: {script_path} -> {status}"
        print(msg)
        return {
            "step": step_id, "description": description, "status": status,
            "duration_sec": 0.0, "returncode": None,
        }

    start = time.time()
    try:
        result = subprocess.run(command, check=False)
        duration = time.time() - start

        if result.returncode == 0:
            print(f"[{step_id}] Completed successfully in {duration:.1f}s")
            return {
                "step": step_id, "description": description, "status": "success",
                "duration_sec": round(duration, 1), "returncode": 0,
            }
        else:
            print(f"[{step_id}] FAILED (exit code {result.returncode}) after {duration:.1f}s")
            return {
                "step": step_id, "description": description, "status": "failed",
                "duration_sec": round(duration, 1), "returncode": result.returncode,
            }
    except Exception as e:
        duration = time.time() - start
        print(f"[{step_id}] FAILED with exception: {e}")
        return {
            "step": step_id, "description": description, "status": "exception",
            "duration_sec": round(duration, 1), "returncode": None, "error": str(e),
        }


def main():
    parser = argparse.ArgumentParser(description="Run the CognoDB benchmark suite end-to-end.")
    parser.add_argument("--skip-ingestion", action="store_true",
                         help="Skip download + ingestion steps (use existing loaded data).")
    parser.add_argument("--only", nargs="+", metavar="STEP_ID",
                         help=f"Run only these step IDs. Choices: {[s[0] for s in STEPS]}")
    args = parser.parse_args()

    steps_to_run = STEPS
    if args.only:
        steps_to_run = [s for s in STEPS if s[0] in args.only]
        unknown = set(args.only) - {s[0] for s in STEPS}
        if unknown:
            print(f"Warning: unknown step id(s) ignored: {unknown}")
    elif args.skip_ingestion:
        steps_to_run = [s for s in STEPS if s[0] not in ("download", "ingestion")]

    print(f"Running {len(steps_to_run)} step(s): {[s[0] for s in steps_to_run]}")

    suite_start = time.time()
    log = []

    for step_id, description, command, required in steps_to_run:
        log.append(run_step(step_id, description, command, required))

    total_duration = time.time() - suite_start

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(SUITE_LOG_PATH, "w") as f:
        json.dump({
            "total_duration_sec": round(total_duration, 1),
            "steps": log,
        }, f, indent=2)

    print(f"\n{'=' * 70}")
    print(" SUITE RUN SUMMARY")
    print(f"{'=' * 70}")
    for entry in log:
        marker = {
            "success": "OK",
            "failed": "FAILED",
            "exception": "ERROR",
            "skipped_not_implemented": "SKIPPED (not yet implemented)",
            "skipped_missing_required": "MISSING (required script not found)",
        }.get(entry["status"], entry["status"])
        print(f"  [{marker:>28}] {entry['step']:<14} ({entry['duration_sec']}s)")

    failed_required = [
        e for e in log
        if e["status"] in ("failed", "exception", "skipped_missing_required")
    ]

    print(f"\nTotal suite time: {total_duration:.1f}s")
    print(f"Full log written to {SUITE_LOG_PATH}")

    if failed_required:
        print(f"\n{len(failed_required)} required step(s) did not complete successfully.")
        sys.exit(1)


if __name__ == "__main__":
    main()