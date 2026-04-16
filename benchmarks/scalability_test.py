#!/usr/bin/env python3
"""
Scalability Test — Runs CSV case study at multiple data sizes.

Usage:
    python benchmarks/scalability_test.py
    python benchmarks/scalability_test.py --runs 10 --sizes 10000 50000 100000 500000

Generates:
    - scalability_results.csv  — Raw timing data
    - ingestion_time_vs_records.png — Plot
"""

import argparse
import csv
import json
import os
import statistics
import sys
import time

import requests

# Reuse benchmark runner
sys.path.insert(0, os.path.dirname(__file__))
from benchmark_runner import (
    run_csv_benchmark, compute_stats, METRIC_KEYS, BASE_URL
)
from generate_test_data import generate_csv

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def run_scalability_test(sizes, runs_per_size, api_url):
    """Run CSV benchmark at each data size."""
    import benchmarks.benchmark_runner as br
    br.BASE_URL = api_url

    all_results = []

    for size in sizes:
        label = f"{size // 1000}K" if size < 1_000_000 else f"{size // 1_000_000}M"
        csv_file = os.path.join(DATA_DIR, f"routes_{label}.csv")

        # Generate data if not exists
        if not os.path.exists(csv_file):
            print(f"  🔧 Generating {label} data file...")
            generate_csv(csv_file, size)

        print(f"\n{'─'*60}")
        print(f"📊 Testing {label} records ({runs_per_size} runs)...")
        print(f"{'─'*60}")

        runs = []
        for i in range(1, runs_per_size + 1):
            print(f"  Run {i}/{runs_per_size}...", end=" ", flush=True)
            metrics = run_csv_benchmark(csv_file, f"{label}_{i}")
            e2e = metrics.get("total_e2e_time", 0)
            records = metrics.get("total_records", 0)

            if "error" in metrics:
                print(f"❌ Error: {metrics['error'][:80]}")
            else:
                print(f"✅ {e2e:.2f}s, {records:,} records")

            runs.append(metrics)
            time.sleep(1)

        stats = compute_stats(runs)
        all_results.append({
            "size": size,
            "label": label,
            "stats": stats,
            "runs": runs,
        })

    return all_results


def save_scalability_results(results):
    """Save scalability test results."""
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Save raw CSV
    csv_file = os.path.join(RESULTS_DIR, "scalability_results.csv")
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "data_size", "label", "run",
            "pipeline_create_time", "schema_detect_time",
            "schema_confirm_time", "bronze_ingest_time",
            "total_e2e_time", "total_records", "throughput_rps",
            "ingestion_duration_seconds",
        ])
        for result in results:
            for i, run in enumerate(result["runs"], 1):
                writer.writerow([
                    result["size"], result["label"], i,
                    run.get("pipeline_create_time", 0),
                    run.get("schema_detect_time", 0),
                    run.get("schema_confirm_time", 0),
                    run.get("bronze_ingest_time", 0),
                    run.get("total_e2e_time", 0),
                    run.get("total_records", 0),
                    run.get("throughput_rps", 0),
                    run.get("ingestion_duration_seconds", 0),
                ])
    print(f"\n📄 Raw results saved to {csv_file}")

    # Save aggregated stats
    stats_file = os.path.join(RESULTS_DIR, "scalability_stats.json")
    stats_data = []
    for result in results:
        stats_data.append({
            "size": result["size"],
            "label": result["label"],
            "stats": result["stats"],
        })
    with open(stats_file, "w") as f:
        json.dump(stats_data, f, indent=2)
    print(f"📊 Stats saved to {stats_file}")

    # Save paper-ready markdown table
    table_file = os.path.join(RESULTS_DIR, "scalability_table.md")
    with open(table_file, "w") as f:
        f.write("# Scalability Test Results — CSV Case Study\n\n")
        f.write("| Data Size | Pipeline Create (s) | Schema Detect (s) | "
                "Bronze Ingest (s) | E2E Time (s) | Records | Throughput (r/s) |\n")
        f.write("|-----------|--------------------|--------------------|"
                "-------------------|-------------|---------|------------------|\n")

        for result in results:
            s = result["stats"]
            f.write(
                f"| {result['label']} | "
                f"{s['pipeline_create_time']['mean']:.3f} ± {s['pipeline_create_time']['std']:.3f} | "
                f"{s['schema_detect_time']['mean']:.3f} ± {s['schema_detect_time']['std']:.3f} | "
                f"{s['bronze_ingest_time']['mean']:.3f} ± {s['bronze_ingest_time']['std']:.3f} | "
                f"{s['total_e2e_time']['mean']:.3f} ± {s['total_e2e_time']['std']:.3f} | "
                f"{s['total_records']['mean']:.0f} | "
                f"{s['throughput_rps']['mean']:.1f} ± {s['throughput_rps']['std']:.1f} |\n"
            )
    print(f"📋 Scalability table saved to {table_file}")


def generate_plots(results):
    """Generate scalability plots using matplotlib."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print("⚠️  matplotlib not installed. Skipping plot generation.")
        print("   Install with: pip install matplotlib")
        return

    os.makedirs(RESULTS_DIR, exist_ok=True)

    sizes = [r["size"] for r in results]
    labels = [r["label"] for r in results]

    # ─── Plot 1: Ingestion Time vs Data Size ───
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Autonomous Data Pipeline — Scalability Analysis (CSV)",
                 fontsize=14, fontweight="bold")

    # 1a. E2E Time
    means = [r["stats"]["total_e2e_time"]["mean"] for r in results]
    stds = [r["stats"]["total_e2e_time"]["std"] for r in results]
    ax = axes[0, 0]
    ax.errorbar(sizes, means, yerr=stds, marker="o", capsize=5,
                linewidth=2, color="#2196F3", markerfacecolor="#1565C0")
    ax.set_xlabel("Number of Records")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("End-to-End Pipeline Time")
    ax.grid(True, alpha=0.3)
    ax.set_xscale("log")

    # 1b. Bronze Ingestion Time
    means = [r["stats"]["bronze_ingest_time"]["mean"] for r in results]
    stds = [r["stats"]["bronze_ingest_time"]["std"] for r in results]
    ax = axes[0, 1]
    ax.errorbar(sizes, means, yerr=stds, marker="s", capsize=5,
                linewidth=2, color="#4CAF50", markerfacecolor="#2E7D32")
    ax.set_xlabel("Number of Records")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Bronze Ingestion Time")
    ax.grid(True, alpha=0.3)
    ax.set_xscale("log")

    # 1c. Schema Detection Time
    means = [r["stats"]["schema_detect_time"]["mean"] for r in results]
    stds = [r["stats"]["schema_detect_time"]["std"] for r in results]
    ax = axes[1, 0]
    ax.errorbar(sizes, means, yerr=stds, marker="^", capsize=5,
                linewidth=2, color="#FF9800", markerfacecolor="#E65100")
    ax.set_xlabel("Number of Records")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Schema Detection Time")
    ax.grid(True, alpha=0.3)
    ax.set_xscale("log")

    # 1d. Throughput
    means = [r["stats"]["throughput_rps"]["mean"] for r in results]
    stds = [r["stats"]["throughput_rps"]["std"] for r in results]
    ax = axes[1, 1]
    ax.errorbar(sizes, means, yerr=stds, marker="D", capsize=5,
                linewidth=2, color="#9C27B0", markerfacecolor="#6A1B9A")
    ax.set_xlabel("Number of Records")
    ax.set_ylabel("Records / Second")
    ax.set_title("Ingestion Throughput")
    ax.grid(True, alpha=0.3)
    ax.set_xscale("log")

    plt.tight_layout()
    plot_file = os.path.join(RESULTS_DIR, "scalability_plots.png")
    plt.savefig(plot_file, dpi=150, bbox_inches="tight")
    print(f"📈 Plots saved to {plot_file}")
    plt.close()

    # ─── Plot 2: Single ingestion time chart (paper-ready) ───
    fig, ax = plt.subplots(figsize=(8, 5))
    means = [r["stats"]["total_e2e_time"]["mean"] for r in results]
    stds = [r["stats"]["total_e2e_time"]["std"] for r in results]

    bars = ax.bar(labels, means, yerr=stds, capsize=8,
                  color=["#2196F3", "#4CAF50", "#FF9800", "#9C27B0"],
                  edgecolor="white", linewidth=1.5, alpha=0.85)

    ax.set_xlabel("Dataset Size", fontsize=12)
    ax.set_ylabel("E2E Pipeline Time (seconds)", fontsize=12)
    ax.set_title("Ingestion Time vs. Record Count (CSV)", fontsize=14,
                 fontweight="bold")
    ax.grid(axis="y", alpha=0.3)

    # Add value labels
    for bar, mean, std in zip(bars, means, stds):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height() + std + 0.2,
                f"{mean:.2f}s", ha="center", va="bottom", fontweight="bold")

    plt.tight_layout()
    plot_file = os.path.join(RESULTS_DIR, "ingestion_time_vs_records.png")
    plt.savefig(plot_file, dpi=150, bbox_inches="tight")
    print(f"📈 Ingestion time chart saved to {plot_file}")
    plt.close()

    # ─── Plot 3: Breakdown stacked bar chart ───
    fig, ax = plt.subplots(figsize=(10, 6))

    create_means = [r["stats"]["pipeline_create_time"]["mean"] for r in results]
    detect_means = [r["stats"]["schema_detect_time"]["mean"] for r in results]
    confirm_means = [r["stats"]["schema_confirm_time"]["mean"] for r in results]
    ingest_means = [r["stats"]["bronze_ingest_time"]["mean"] for r in results]

    x = range(len(labels))
    w = 0.5

    p1 = ax.bar(x, create_means, w, label="Pipeline Setup", color="#2196F3")
    p2 = ax.bar(x, detect_means, w, bottom=create_means,
                label="Schema Detection", color="#FF9800")
    bottom2 = [c + d for c, d in zip(create_means, detect_means)]
    p3 = ax.bar(x, confirm_means, w, bottom=bottom2,
                label="Schema Confirmation", color="#4CAF50")
    bottom3 = [b + c for b, c in zip(bottom2, confirm_means)]
    p4 = ax.bar(x, ingest_means, w, bottom=bottom3,
                label="Bronze Ingestion", color="#9C27B0")

    ax.set_xlabel("Dataset Size", fontsize=12)
    ax.set_ylabel("Time (seconds)", fontsize=12)
    ax.set_title("Pipeline Time Breakdown by Phase (CSV)", fontsize=14,
                 fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend(loc="upper left")
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plot_file = os.path.join(RESULTS_DIR, "pipeline_time_breakdown.png")
    plt.savefig(plot_file, dpi=150, bbox_inches="tight")
    print(f"📈 Time breakdown chart saved to {plot_file}")
    plt.close()


def main():
    parser = argparse.ArgumentParser(
        description="Scalability test for CSV case study at multiple data sizes"
    )
    parser.add_argument("--runs", type=int, default=10,
                        help="Runs per data size (default: 10)")
    parser.add_argument("--sizes", nargs="+", type=int,
                        default=[10000, 50000, 100000, 500000],
                        help="Data sizes to test (default: 10000 50000 100000 500000)")
    parser.add_argument("--api-url", default=BASE_URL,
                        help="Backend API URL (default: http://localhost:8000)")
    args = parser.parse_args()

    print("📊 Autonomous Data Pipeline — Scalability Test")
    print(f"   API URL:  {args.api_url}")
    print(f"   Runs/size: {args.runs}")
    print(f"   Sizes:    {args.sizes}")

    # Check API
    try:
        requests.get(f"{args.api_url}/docs", timeout=5)
        print("✅ Backend API is reachable\n")
    except Exception as e:
        print(f"❌ Cannot reach backend: {e}")
        print("   Run: docker compose up -d")
        sys.exit(1)

    results = run_scalability_test(args.sizes, args.runs, args.api_url)
    save_scalability_results(results)
    generate_plots(results)

    # Print final summary
    print(f"\n{'='*60}")
    print("  SCALABILITY SUMMARY")
    print(f"{'='*60}")
    for r in results:
        s = r["stats"]
        print(f"  {r['label']:>6s}: E2E = {s['total_e2e_time']['mean']:.3f} ± "
              f"{s['total_e2e_time']['std']:.3f}s  |  "
              f"Throughput = {s['throughput_rps']['mean']:.1f} ± "
              f"{s['throughput_rps']['std']:.1f} r/s")
    print(f"{'='*60}\n")
    print("✅ Scalability test complete!")


if __name__ == "__main__":
    main()
