import os
import json
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

RESULTS_DIR = "results"
CHARTS_DIR = os.path.join(RESULTS_DIR, "charts")
SUMMARY_MD_PATH = os.path.join(RESULTS_DIR, "benchmark_summary.md")

os.makedirs(CHARTS_DIR, exist_ok=True)


def load_all_results():
    records = []

    # BUG FIX: original pattern "*_benchmark_results.json" never matched
    # ingestion_results.json (written by load_data.py), which doesn't
    # contain "_benchmark_" in its filename. Ingestion data was silently
    # excluded from every chart and table. Now explicitly includes it.
    json_files = glob.glob(os.path.join(RESULTS_DIR, "*_benchmark_results.json"))
    json_files += glob.glob(os.path.join(RESULTS_DIR, "ingestion_results.json"))

    for filepath in json_files:
        filename = os.path.basename(filepath)
        if filename == "ingestion_results.json":
            benchmark_type = "ingestion"
        else:
            benchmark_type = filename.replace("_benchmark_results.json", "")

        with open(filepath, "r") as f:
            data = json.load(f)
            for entry in data:
                entry["category"] = benchmark_type
                # Ingestion entries have no "workload" field (only
                # platform-level results) — default one so the summary
                # table doesn't show blank cells for these rows.
                entry.setdefault("workload", "Ingestion" if benchmark_type == "ingestion" else None)
                records.append(entry)

    return pd.DataFrame(records)


def generate_charts(df):
    sns.set_theme(style="whitegrid")

    # 1. Latency Workloads Chart (Lookups, Traversals, Aggregations)
    latency_df = df[df["category"] != "ingestion"].copy()
    if not latency_df.empty and "p50_ms" in latency_df.columns:
        plt.figure(figsize=(14, 7))
        sns.barplot(
            data=latency_df,
            x="workload",
            y="p50_ms",
            hue="platform",
            palette="viridis"
        )
        plt.yscale("log")
        plt.title("Workload Latency Comparison - p50 (Log Scale)", fontsize=14, fontweight="bold")
        plt.xlabel("Workload", fontsize=12)
        plt.ylabel("p50 Latency (ms)", fontsize=12)
        plt.xticks(rotation=30, ha="right")
        plt.legend(title="Platform", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, "latency_p50_comparison.png"), dpi=300)
        plt.close()
    else:
        print("Skipped latency chart: no non-ingestion rows with 'p50_ms' found.")

    # 2. Ingestion Throughput Chart
    ingest_df = df[df["category"] == "ingestion"].copy()
    # BUG FIX: field is "edge_rate_per_sec" in ingestion_results.json
    # (written by load_data.py's log_result()), not "edges_per_sec".
    if not ingest_df.empty and "edge_rate_per_sec" in ingest_df.columns:
        plt.figure(figsize=(10, 5))
        sns.barplot(
            data=ingest_df,
            x="platform",
            y="edge_rate_per_sec",
            palette="magma"
        )
        plt.title("Ingestion Throughput (Relationships / Second)", fontsize=14, fontweight="bold")
        plt.xlabel("Platform", fontsize=12)
        plt.ylabel("Relationships / sec", fontsize=12)
        plt.xticks(rotation=15)
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, "ingestion_throughput.png"), dpi=300)
        plt.close()
    else:
        print("Skipped ingestion chart: no ingestion rows with 'edge_rate_per_sec' found.")


def export_markdown_summary(df):
    # BUG FIX: matched to actual field name from log_result().
    cols = ["category", "platform", "workload", "p50_ms", "p95_ms", "edge_rate_per_sec", "success_rate"]
    available_cols = [c for c in cols if c in df.columns]

    summary_df = df[available_cols].copy()
    sort_cols = [c for c in ["category", "workload", "platform"] if c in summary_df.columns]
    if sort_cols:
        summary_df = summary_df.sort_values(by=sort_cols)

    markdown_table = summary_df.to_markdown(index=False)

    with open(SUMMARY_MD_PATH, "w") as f:
        f.write("# Database Benchmark Summary Report\n\n")
        f.write(markdown_table)

    print(f"Summary report written to {SUMMARY_MD_PATH}")
    print("\n" + markdown_table)


def main():
    df = load_all_results()
    if df.empty:
        print("No benchmark result JSON files found in results/")
        return

    found_categories = sorted(df["category"].unique().tolist())
    print(f"Loaded result categories: {found_categories}")

    generate_charts(df)
    export_markdown_summary(df)
    print(f"\nCharts successfully generated in {CHARTS_DIR}/")


if __name__ == "__main__":
    main()
