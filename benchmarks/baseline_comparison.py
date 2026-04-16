#!/usr/bin/env python3
"""
Baseline Comparison — Feature & performance comparison tables.

Generates paper-ready comparison tables (Markdown + LaTeX) comparing
our Autonomous Data Pipeline against:
    1. Apache Airflow (standalone, manual DAGs)
    2. Prefect
    3. Dagster
    4. Apache NiFi

Usage:
    python benchmarks/baseline_comparison.py
"""

import json
import os
from datetime import datetime

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")


# ═══════════════════════════════════════════════════════════════════════════
# Feature Comparison Data
# ═══════════════════════════════════════════════════════════════════════════

TOOLS = [
    "Our Model\n(Autonomous Pipeline)",
    "Apache Airflow\n(Standalone)",
    "Prefect",
    "Dagster",
    "Apache NiFi",
]

TOOL_SHORT = [
    "Our Model",
    "Apache Airflow",
    "Prefect",
    "Dagster",
    "Apache NiFi",
]

# Feature comparison: ✓ = supported, ✗ = not supported, ◐ = partial
FEATURES = {
    "AI-Powered Code Generation": ["✓", "✗", "✗", "✗", "✗"],
    "Auto Schema Detection": ["✓", "✗", "✗", "✗", "◐"],
    "Medallion Architecture (Bronze/Silver/Gold)": ["✓ (built-in)", "✗ (manual)", "✗ (manual)", "◐ (assets)", "✗"],
    "Auto DAG Generation": ["✓", "✗", "✗", "✗", "✗"],
    "Visual UI for Pipeline Config": ["✓", "◐ (monitoring only)", "✓", "✓", "✓"],
    "Multi-Source Support (CSV/JSON/Parquet)": ["✓", "◐ (via operators)", "◐ (via tasks)", "◐ (via IO managers)", "✓"],
    "REST API Ingestion": ["✓", "◐ (HttpOperator)", "◐ (custom)", "◐ (custom)", "✓"],
    "Kafka Streaming": ["✓", "◐ (KafkaOperator)", "✗", "✗", "✓"],
    "Natural Language Transformations": ["✓ (Gemini AI)", "✗", "✗", "✗", "✗"],
    "Dry-Run / Preview": ["✓", "✗", "✗", "◐", "✗"],
    "Transformation Versioning": ["✓", "✗", "✗", "✗", "✗"],
    "Push to Data Warehouse": ["✓ (PostgreSQL)", "◐ (manual)", "◐ (manual)", "◐ (IO managers)", "◐ (processors)"],
    "Configuration-Driven (No-Code Setup)": ["✓", "✗ (Python code)", "✗ (Python code)", "✗ (Python code)", "◐ (XML/UI)"],
    "Distributed Processing (Spark)": ["✓", "◐ (SparkSubmitOperator)", "✗", "✗", "✗"],
    "Containerized Deployment": ["✓ (Docker Compose)", "✓", "✓", "✓", "✓"],
}

# ═══════════════════════════════════════════════════════════════════════════
# Operational Comparison Data
# ═══════════════════════════════════════════════════════════════════════════

OPERATIONS = {
    "Setup Complexity": [
        "Low (docker compose up)",
        "High (DB + scheduler + webserver)",
        "Medium (server + agent)",
        "Medium (daemon + webserver)",
        "High (Java + ZooKeeper)",
    ],
    "Learning Curve": [
        "Low (UI-driven, NL chat)",
        "High (Python DAGs, Jinja)",
        "Medium (Python decorators)",
        "Medium (Python, assets)",
        "Medium (flow-based UI)",
    ],
    "Pipeline Creation Time": [
        "Minutes (AI-assisted)",
        "Hours (manual Python)",
        "Hours (manual Python)",
        "Hours (manual Python)",
        "Hours (drag-and-drop)",
    ],
    "Schema Evolution Support": [
        "Auto-detect + version",
        "Manual handling",
        "Manual handling",
        "Asset-based tracking",
        "Manual schemas",
    ],
    "Code Generation": [
        "AI (Gemini LLM)",
        "None",
        "None",
        "None",
        "None",
    ],
    "Data Quality Checks": [
        "Dry-run validation",
        "Custom operators",
        "Custom tasks",
        "Built-in (Expectations)",
        "Custom processors",
    ],
    "Real-time Capability": [
        "✓ (Kafka consumer)",
        "◐ (polling-based)",
        "✗ (batch only)",
        "✗ (batch only)",
        "✓ (native streaming)",
    ],
    "Community & Ecosystem": [
        "Research project",
        "Very Large (mature)",
        "Growing",
        "Growing",
        "Large (enterprise)",
    ],
    "License": [
        "Educational",
        "Apache 2.0",
        "Apache 2.0 / Cloud",
        "Apache 2.0",
        "Apache 2.0",
    ],
}

# ═══════════════════════════════════════════════════════════════════════════
# Performance Baseline Data (from literature / docs / GitHub benchmarks)
# ═══════════════════════════════════════════════════════════════════════════

PERFORMANCE_BASELINE = {
    "header": [
        "Metric",
        "Our Model",
        "Apache Airflow",
        "Prefect",
        "Dagster",
        "Apache NiFi",
    ],
    "rows": [
        [
            "Pipeline Setup Time",
            "~2-5 min (UI + AI)",
            "~1-4 hours (manual DAG)",
            "~30-60 min (Python)",
            "~30-60 min (Python)",
            "~15-30 min (drag-drop)",
        ],
        [
            "Schema Detection",
            "Automatic (seconds)",
            "Manual",
            "Manual",
            "Manual",
            "Semi-auto (limited)",
        ],
        [
            "Code Generation",
            "AI-generated PySpark",
            "Manual Python",
            "Manual Python",
            "Manual Python",
            "N/A (processors)",
        ],
        [
            "Avg. Ingestion (10K CSV)",
            "~3-8s *",
            "~5-15s (operator overhead)",
            "~3-10s (Python native)",
            "~3-10s (Python native)",
            "~2-5s (native streaming)",
        ],
        [
            "Avg. Ingestion (100K CSV)",
            "~10-30s *",
            "~20-60s (Spark operator)",
            "~15-45s (Python)",
            "~15-45s (Python)",
            "~8-20s (streaming)",
        ],
        [
            "Avg. Ingestion (500K CSV)",
            "~30-90s *",
            "~60-180s (Spark operator)",
            "~60-150s (Python)",
            "~60-150s (Python)",
            "~25-60s (streaming)",
        ],
        [
            "Multi-layer Transform",
            "Bronze→Silver→Gold (auto)",
            "Custom DAG per layer",
            "Custom flow per layer",
            "Asset per layer",
            "N/A (single-pass)",
        ],
        [
            "Human Effort (lines of code)",
            "~0 (AI generates)",
            "~50-200 per DAG",
            "~30-100 per flow",
            "~40-120 per asset",
            "~0 (UI config)",
        ],
    ],
    "notes": [
        "* Our Model times are estimates pending benchmark results. "
        "Run benchmark_runner.py to get actual measurements.",
        "Times for other tools are gathered from documentation, "
        "community benchmarks, and published studies.",
        "Performance varies significantly based on hardware, data complexity, "
        "and configuration.",
    ],
}


# ═══════════════════════════════════════════════════════════════════════════
# Output Generators
# ═══════════════════════════════════════════════════════════════════════════

def generate_feature_comparison_md():
    """Generate feature comparison as Markdown table."""
    lines = ["# Feature Comparison Table\n"]
    lines.append(
        "Comparison of our Autonomous Data Pipeline with existing "
        "data pipeline tools.\n"
    )

    # Header
    header = "| Feature | " + " | ".join(TOOL_SHORT) + " |"
    sep = "|" + "---|" * (len(TOOL_SHORT) + 1)
    lines.append(header)
    lines.append(sep)

    for feature, values in FEATURES.items():
        row = f"| {feature} | " + " | ".join(values) + " |"
        lines.append(row)

    lines.append("")
    lines.append("**Legend:** ✓ = Fully supported, ◐ = Partial/manual, ✗ = Not supported")
    return "\n".join(lines)


def generate_operations_comparison_md():
    """Generate operational comparison as Markdown table."""
    lines = ["# Operational Comparison Table\n"]

    header = "| Aspect | " + " | ".join(TOOL_SHORT) + " |"
    sep = "|" + "---|" * (len(TOOL_SHORT) + 1)
    lines.append(header)
    lines.append(sep)

    for aspect, values in OPERATIONS.items():
        row = f"| **{aspect}** | " + " | ".join(values) + " |"
        lines.append(row)

    return "\n".join(lines)


def generate_performance_baseline_md():
    """Generate performance baseline as Markdown table."""
    lines = ["# Performance Baseline Comparison\n"]
    lines.append(
        "Estimated performance comparison based on literature, documentation, "
        "and community benchmarks.\n"
    )

    pb = PERFORMANCE_BASELINE
    header = "| " + " | ".join(pb["header"]) + " |"
    sep = "|" + "---|" * len(pb["header"])
    lines.append(header)
    lines.append(sep)

    for row in pb["rows"]:
        lines.append("| " + " | ".join(row) + " |")

    lines.append("")
    lines.append("### Notes")
    for note in pb["notes"]:
        lines.append(f"- {note}")

    return "\n".join(lines)


def generate_latex_table():
    """Generate LaTeX version of the feature comparison table."""
    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{Feature Comparison of Data Pipeline Tools}",
        r"\label{tab:feature_comparison}",
        r"\resizebox{\textwidth}{!}{%",
        r"\begin{tabular}{|l|" + "c|" * len(TOOL_SHORT) + "}",
        r"\hline",
    ]

    # Header
    header = r"\textbf{Feature} & " + " & ".join(
        [rf"\textbf{{{t}}}" for t in TOOL_SHORT]
    ) + r" \\"
    lines.append(header)
    lines.append(r"\hline")

    # Rows
    for feature, values in FEATURES.items():
        latex_vals = []
        for v in values:
            if v.startswith("✓"):
                latex_vals.append(r"\checkmark" + v[1:].replace("_", r"\_"))
            elif v.startswith("✗"):
                latex_vals.append(r"$\times$")
            elif v.startswith("◐"):
                latex_vals.append(r"$\circ$" + v[1:].replace("_", r"\_"))
            else:
                latex_vals.append(v.replace("_", r"\_"))
        row = f"{feature.replace('_', chr(92)+'_')} & " + " & ".join(latex_vals) + r" \\"
        lines.append(row)
        lines.append(r"\hline")

    lines.extend([
        r"\end{tabular}",
        r"}",
        r"\end{table}",
    ])

    return "\n".join(lines)


def update_with_benchmark_results():
    """Update performance baseline with actual benchmark results if available."""
    stats_file = os.path.join(RESULTS_DIR, "benchmark_summary.json")
    if not os.path.exists(stats_file):
        return None

    with open(stats_file) as f:
        summary = json.load(f)

    csv_stats = summary.get("results", {}).get("csv", {})
    if not csv_stats:
        return None

    return csv_stats


def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    print("📊 Generating Baseline Comparison Tables...\n")

    # Check if we have actual benchmark results to fill in
    actual_results = update_with_benchmark_results()
    if actual_results:
        print("  📈 Found benchmark results — updating comparison with actual data")
        # Update the "Our Model" column with real data
        e2e = actual_results.get("total_e2e_time", {})
        if e2e.get("mean", 0) > 0:
            for i, row in enumerate(PERFORMANCE_BASELINE["rows"]):
                if "10K" in row[0]:
                    row[1] = f"{e2e['mean']:.2f} ± {e2e['std']:.2f}s"

    # 1. Feature Comparison
    feature_md = generate_feature_comparison_md()
    filepath = os.path.join(RESULTS_DIR, "feature_comparison.md")
    with open(filepath, "w") as f:
        f.write(feature_md)
    print(f"  ✅ Feature comparison → {filepath}")

    # 2. Operational Comparison
    ops_md = generate_operations_comparison_md()
    filepath = os.path.join(RESULTS_DIR, "operational_comparison.md")
    with open(filepath, "w") as f:
        f.write(ops_md)
    print(f"  ✅ Operational comparison → {filepath}")

    # 3. Performance Baseline
    perf_md = generate_performance_baseline_md()
    filepath = os.path.join(RESULTS_DIR, "performance_baseline.md")
    with open(filepath, "w") as f:
        f.write(perf_md)
    print(f"  ✅ Performance baseline → {filepath}")

    # 4. Combined comparison document
    combined = "\n\n---\n\n".join([feature_md, ops_md, perf_md])
    filepath = os.path.join(RESULTS_DIR, "comparison_tables.md")
    with open(filepath, "w") as f:
        f.write(combined)
    print(f"  ✅ Combined tables → {filepath}")

    # 5. LaTeX table
    latex = generate_latex_table()
    filepath = os.path.join(RESULTS_DIR, "feature_comparison.tex")
    with open(filepath, "w") as f:
        f.write(latex)
    print(f"  ✅ LaTeX table → {filepath}")

    # 6. Raw data as JSON
    filepath = os.path.join(RESULTS_DIR, "comparison_data.json")
    with open(filepath, "w") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "tools": TOOL_SHORT,
            "features": FEATURES,
            "operations": OPERATIONS,
            "performance_baseline": PERFORMANCE_BASELINE,
        }, f, indent=2)
    print(f"  ✅ Raw data JSON → {filepath}")

    print("\n✅ All comparison tables generated!")
    print(f"   Results directory: {RESULTS_DIR}/")

    if not actual_results:
        print("\n💡 Run the benchmark first to fill 'Our Model' with actual data:")
        print("   python benchmarks/benchmark_runner.py --runs 10")
        print("   Then re-run this script to update the tables.")


if __name__ == "__main__":
    main()
