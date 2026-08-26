import os
import json
import subprocess
import statistics
import sys

ITERATIONS = 3
RESULTS_DIR = "results"
INGESTION_FILE = os.path.join(RESULTS_DIR, "ingestion_results.json")
STABILITY_LOG = os.path.join(RESULTS_DIR, "ingestion_stability_log.json")


def run_iteration(iteration):
    print(f"\n{'=' * 50}")
    print(f"STARTING INGESTION ITERATION {iteration} OF {ITERATIONS}")
    print(f"{'=' * 50}")

    print("\n[1/2] Wiping databases for a clean state...")
    reset_result = subprocess.run([sys.executable, "reset_databases.py"])
    if reset_result.returncode != 0:
        print("Error during database reset. Halting.")
        sys.exit(1)

    print("\n[2/2] Running data ingestion...")
    # BUG FIX: load_data.py lives at the project root, not inside workloads/
    # (confirmed by the repo layout in README.md). The original path here
    # would never resolve and this script would halt on iteration 1.
    ingest_result = subprocess.run([sys.executable, "load_data.py"])
    if ingest_result.returncode != 0:
        print("Error during data ingestion. Halting.")
        sys.exit(1)

    with open(INGESTION_FILE, "r") as f:
        data = json.load(f)
    return data


def main():
    # BUG FIX: check for load_data.py at root, not workloads/load_data.py,
    # matching the corrected path used above.
    if not os.path.exists("reset_databases.py") or not os.path.exists("load_data.py"):
        print("Please run this script from the project root directory "
              "(expects reset_databases.py and load_data.py alongside it).")
        sys.exit(1)

    platform_rates = {
        "Local Neo4j": [],
        "Local Memgraph": [],
        "Local FalkorDB": [],
        "Local ArangoDB": [],
        "CognoDB Cloud": []
    }
    # Keep the last iteration's full entry per platform so we don't lose
    # node_rate_per_sec, total_nodes/edges, and load_method — these are
    # used elsewhere (README, generate_report.py) and the original script
    # silently dropped them when it overwrote ingestion_results.json.
    last_full_entry = {}

    failed_platforms_by_iteration = {}

    for i in range(1, ITERATIONS + 1):
        iter_data = run_iteration(i)
        seen_this_iteration = set()

        for entry in iter_data:
            platform = entry.get("platform")
            if platform in platform_rates and "edge_rate_per_sec" in entry:
                platform_rates[platform].append(entry["edge_rate_per_sec"])
                last_full_entry[platform] = entry
                seen_this_iteration.add(platform)

        missing = set(platform_rates.keys()) - seen_this_iteration
        if missing:
            print(f"WARNING: iteration {i} produced no result for: {sorted(missing)}")
            failed_platforms_by_iteration[i] = sorted(missing)

    print(f"\n{'=' * 50}")
    print(f"INGESTION STABILITY ANALYSIS (Across {ITERATIONS} Runs)")
    print(f"{'=' * 50}")

    final_summary = []

    for platform, rates in platform_rates.items():
        if not rates:
            print(f"{platform:<18}: NO SUCCESSFUL RUNS — excluded from summary")
            continue

        median_rate = statistics.median(rates)
        min_rate = min(rates)
        max_rate = max(rates)
        successful_runs = len(rates)

        print(f"{platform:<18}: Median = {median_rate:>8.2f} edges/sec "
              f"(Range: {min_rate:.2f} - {max_rate:.2f}, {successful_runs}/{ITERATIONS} runs succeeded)")

        base_entry = last_full_entry.get(platform, {})
        final_summary.append({
            "platform": platform,
            "workload": "Ingestion",
            "edge_rate_per_sec": round(median_rate, 2),
            "node_rate_per_sec": base_entry.get("node_rate_per_sec"),
            "total_nodes": base_entry.get("total_nodes"),
            "total_edges": base_entry.get("total_edges"),
            "load_method": base_entry.get("load_method"),
            "stability_notes": (
                f"Median of {successful_runs}/{ITERATIONS} successful runs. "
                f"Min: {min_rate:.2f}, Max: {max_rate:.2f} edges/sec."
            ),
        })

    with open(INGESTION_FILE, "w") as f:
        json.dump(final_summary, f, indent=4)

    with open(STABILITY_LOG, "w") as f:
        json.dump({
            "raw_rates_by_platform": platform_rates,
            "failed_platforms_by_iteration": failed_platforms_by_iteration,
        }, f, indent=4)

    print(f"\nSaved stable medians to {INGESTION_FILE}.")
    print(f"Saved full variance log to {STABILITY_LOG}.")

    if failed_platforms_by_iteration:
        print(f"\nNote: {len(failed_platforms_by_iteration)} iteration(s) had missing platform "
              f"results — see {STABILITY_LOG} for details. Consider documenting this in your "
              f"README as an honest reliability caveat rather than re-running until it disappears.")


if __name__ == "__main__":
    main()