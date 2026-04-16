#!/usr/bin/env python3
"""
Benchmark Runner — Runs each case study N times and collects metrics.

Usage:
    python benchmarks/benchmark_runner.py --runs 10
    python benchmarks/benchmark_runner.py --runs 10 --case-study csv
    python benchmarks/benchmark_runner.py --runs 10 --case-study kafka
    python benchmarks/benchmark_runner.py --runs 10 --case-study api

Metrics collected per run:
    - pipeline_create_time: Time to create pipeline + configure source
    - schema_detect_time:   Time for schema detection
    - schema_confirm_time:  Time for schema confirmation
    - bronze_ingest_time:   Time for bronze ingestion to complete
    - total_e2e_time:       End-to-end pipeline time
    - total_records:        Number of records ingested
    - throughput:           Records per second (total_records / bronze_ingest_time)

Requires Docker services to be running.
"""

import argparse
import csv
import json
import os
import shutil
import statistics
import sys
import time
import uuid
from datetime import datetime

import requests

# ─── Configuration ───────────────────────────────────────────────────────────
BASE_URL = os.environ.get("BENCHMARK_API_URL", "http://localhost:8000")
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

# Default data file for CSV benchmark
DEFAULT_CSV_FILE = os.path.join(DATA_DIR, "routes_10K.csv")

# Default API endpoint for REST API benchmark (JSONPlaceholder as fallback)
DEFAULT_API_ENDPOINT = "https://jsonplaceholder.typicode.com/posts"

# Default Kafka config
DEFAULT_KAFKA_BOOTSTRAP = "localhost:9093"
DEFAULT_KAFKA_TOPIC = "flight-data"


# ─── API Helpers ─────────────────────────────────────────────────────────────
def api(method, path, **kwargs):
    """Make an API call and return response."""
    url = f"{BASE_URL}{path}"
    resp = getattr(requests, method)(url, **kwargs, timeout=300)
    resp.raise_for_status()
    return resp.json()


def create_pipeline(name, source_type):
    """Create a new pipeline."""
    return api("post", "/api/pipelines/", json={
        "name": name,
        "description": f"Benchmark pipeline ({source_type})",
        "source_type": source_type,
    })


def configure_csv_source(pipeline_id, file_path):
    """Configure a CSV data source."""
    return api("post", f"/api/pipelines/{pipeline_id}/source", json={
        "source_type": "csv",
        "file_path": os.path.dirname(file_path),
        "file_format": "csv",
        "config": {
            "csv_delimiter": ",",
            "csv_header": True,
            "csv_encoding": "utf-8",
        },
    })


def configure_kafka_source(pipeline_id, bootstrap, topic):
    """Configure a Kafka data source."""
    return api("post", f"/api/pipelines/{pipeline_id}/source", json={
        "source_type": "kafka",
        "kafka_bootstrap": bootstrap,
        "kafka_topic": topic,
        "kafka_group_id": f"benchmark-{uuid.uuid4().hex[:8]}",
    })


def configure_api_source(pipeline_id, endpoint):
    """Configure a REST API data source."""
    return api("post", f"/api/pipelines/{pipeline_id}/source", json={
        "source_type": "api",
        "api_endpoint": endpoint,
        "api_method": "GET",
    })


def detect_schema(pipeline_id):
    """Run schema detection."""
    return api("post", f"/api/schemas/detect?pipeline_id={pipeline_id}")


def confirm_schema(pipeline_id, schema_id):
    """Confirm detected schema and trigger bronze ingestion."""
    return api("post", "/api/schemas/confirm", json={
        "pipeline_id": str(pipeline_id),
        "schema_id": str(schema_id),
        "field_overrides": [],
        "exclude_files": [],
        "include_files": [],
    })


def poll_ingestion_status(pipeline_id, timeout=600, poll_interval=2):
    """Poll bronze ingestion until complete or timeout."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            ingestions = api("get", f"/api/bronze/{pipeline_id}/ingestions")
            if ingestions and len(ingestions) > 0:
                latest = ingestions[0]  # sorted by created_at desc
                status = latest.get("status", "")
                if status in ("success", "failed"):
                    return latest
        except Exception:
            pass
        time.sleep(poll_interval)
    raise TimeoutError(f"Ingestion did not complete within {timeout}s")


def delete_pipeline(pipeline_id):
    """Clean up by deleting the pipeline."""
    try:
        api("delete", f"/api/pipelines/{pipeline_id}")
    except Exception:
        pass


# ─── Benchmark Core ──────────────────────────────────────────────────────────
def run_csv_benchmark(csv_file, run_id):
    """Run a single CSV case study benchmark."""
    # Prepare: copy data file to a unique temp directory 
    # (pipeline scans a directory for files)
    temp_dir = f"/tmp/benchmark_csv_{run_id}_{uuid.uuid4().hex[:8]}"
    os.makedirs(temp_dir, exist_ok=True)
    dest_file = os.path.join(temp_dir, os.path.basename(csv_file))
    shutil.copy2(csv_file, dest_file)

    metrics = {}
    pipeline_id = None

    try:
        # 1. Create pipeline
        t0 = time.time()
        pipeline = create_pipeline(
            f"bench_csv_{run_id}_{uuid.uuid4().hex[:6]}",
            "csv"
        )
        pipeline_id = pipeline["id"]

        # 2. Configure source
        configure_csv_source(pipeline_id, dest_file)
        metrics["pipeline_create_time"] = round(time.time() - t0, 4)

        # 3. Detect schema
        t1 = time.time()
        schema_result = detect_schema(pipeline_id)
        metrics["schema_detect_time"] = round(time.time() - t1, 4)
        metrics["fields_detected"] = len(schema_result.get("fields", []))
        metrics["detection_confidence"] = schema_result.get("detection_confidence", 0)

        # 4. Confirm schema + trigger ingestion
        t2 = time.time()
        schema_id = schema_result.get("schema_id")
        confirm_result = confirm_schema(pipeline_id, schema_id)
        metrics["schema_confirm_time"] = round(time.time() - t2, 4)

        # 5. Wait for bronze ingestion to complete
        t3 = time.time()
        ingestion = poll_ingestion_status(pipeline_id)
        metrics["bronze_ingest_time"] = round(time.time() - t3, 4)
        metrics["total_records"] = ingestion.get("total_records", 0)
        metrics["ingestion_status"] = ingestion.get("status", "unknown")
        metrics["ingestion_duration_seconds"] = ingestion.get("duration_seconds", 0)

        # 6. Total E2E time
        metrics["total_e2e_time"] = round(time.time() - t0, 4)

        # 7. Throughput
        if metrics["bronze_ingest_time"] > 0:
            metrics["throughput_rps"] = round(
                metrics["total_records"] / metrics["bronze_ingest_time"], 2
            )
        else:
            metrics["throughput_rps"] = 0

    except Exception as e:
        metrics["error"] = str(e)
        metrics["ingestion_status"] = "error"

    finally:
        # Cleanup
        if pipeline_id:
            delete_pipeline(pipeline_id)
        shutil.rmtree(temp_dir, ignore_errors=True)

    return metrics


def run_kafka_benchmark(bootstrap, topic, run_id):
    """Run a single Kafka case study benchmark."""
    metrics = {}
    pipeline_id = None

    try:
        # 1. Create pipeline
        t0 = time.time()
        pipeline = create_pipeline(
            f"bench_kafka_{run_id}_{uuid.uuid4().hex[:6]}",
            "kafka"
        )
        pipeline_id = pipeline["id"]

        # 2. Configure source
        configure_kafka_source(pipeline_id, bootstrap, topic)
        metrics["pipeline_create_time"] = round(time.time() - t0, 4)

        # 3. Detect schema (consumes sample messages)
        t1 = time.time()
        schema_result = detect_schema(pipeline_id)
        metrics["schema_detect_time"] = round(time.time() - t1, 4)
        metrics["fields_detected"] = len(schema_result.get("fields", []))
        metrics["detection_confidence"] = schema_result.get("detection_confidence", 0)
        metrics["total_records"] = schema_result.get("sample_row_count", 0)

        # 4. Confirm schema (Kafka uses Airflow DAG for actual ingestion)
        t2 = time.time()
        schema_id = schema_result.get("schema_id")
        confirm_result = confirm_schema(pipeline_id, schema_id)
        metrics["schema_confirm_time"] = round(time.time() - t2, 4)

        # For Kafka, bronze ingestion is via Airflow DAG, 
        # so we measure detect + confirm as the pipeline setup time
        metrics["bronze_ingest_time"] = 0  # DAG-based
        metrics["total_e2e_time"] = round(time.time() - t0, 4)
        metrics["ingestion_status"] = "confirmed_dag_pending"
        metrics["throughput_rps"] = 0

    except Exception as e:
        metrics["error"] = str(e)
        metrics["ingestion_status"] = "error"

    finally:
        if pipeline_id:
            delete_pipeline(pipeline_id)

    return metrics


def run_api_benchmark(endpoint, run_id):
    """Run a single REST API case study benchmark."""
    metrics = {}
    pipeline_id = None

    try:
        # 1. Create pipeline
        t0 = time.time()
        pipeline = create_pipeline(
            f"bench_api_{run_id}_{uuid.uuid4().hex[:6]}",
            "api"
        )
        pipeline_id = pipeline["id"]

        # 2. Configure source
        configure_api_source(pipeline_id, endpoint)
        metrics["pipeline_create_time"] = round(time.time() - t0, 4)

        # 3. Detect schema (fetches from API)
        t1 = time.time()
        schema_result = detect_schema(pipeline_id)
        metrics["schema_detect_time"] = round(time.time() - t1, 4)
        metrics["fields_detected"] = len(schema_result.get("fields", []))
        metrics["detection_confidence"] = schema_result.get("detection_confidence", 0)
        metrics["total_records"] = schema_result.get("sample_row_count", 0)

        # 4. Confirm schema (API uses Airflow DAG for actual ingestion)
        t2 = time.time()
        schema_id = schema_result.get("schema_id")
        confirm_result = confirm_schema(pipeline_id, schema_id)
        metrics["schema_confirm_time"] = round(time.time() - t2, 4)

        # For API, bronze ingestion is via Airflow DAG
        metrics["bronze_ingest_time"] = 0  # DAG-based
        metrics["total_e2e_time"] = round(time.time() - t0, 4)
        metrics["ingestion_status"] = "confirmed_dag_pending"
        metrics["throughput_rps"] = 0

    except Exception as e:
        metrics["error"] = str(e)
        metrics["ingestion_status"] = "error"

    finally:
        if pipeline_id:
            delete_pipeline(pipeline_id)

    return metrics


# ─── Results Aggregation ─────────────────────────────────────────────────────
METRIC_KEYS = [
    "pipeline_create_time", "schema_detect_time", "schema_confirm_time",
    "bronze_ingest_time", "total_e2e_time", "total_records",
    "throughput_rps", "fields_detected", "detection_confidence",
    "ingestion_duration_seconds",
]


def compute_stats(runs: list[dict]) -> dict:
    """Compute mean ± std dev for each metric across runs."""
    stats = {}
    for key in METRIC_KEYS:
        values = [r.get(key, 0) for r in runs if "error" not in r]
        if values:
            mean = statistics.mean(values)
            stdev = statistics.stdev(values) if len(values) > 1 else 0
            stats[key] = {"mean": round(mean, 4), "std": round(stdev, 4)}
        else:
            stats[key] = {"mean": 0, "std": 0}
    stats["successful_runs"] = sum(1 for r in runs if "error" not in r)
    stats["failed_runs"] = sum(1 for r in runs if "error" in r)
    return stats


def save_results(case_study: str, runs: list[dict], stats: dict):
    """Save raw run data and aggregated stats."""
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Save raw runs as CSV
    raw_file = os.path.join(RESULTS_DIR, f"{case_study}_raw_results.csv")
    if runs:
        fieldnames = ["run"] + list(runs[0].keys())
        with open(raw_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i, run in enumerate(runs, 1):
                row = {"run": i, **run}
                writer.writerow(row)
    print(f"  📄 Raw results saved to {raw_file}")

    # Save stats as JSON
    stats_file = os.path.join(RESULTS_DIR, f"{case_study}_stats.json")
    with open(stats_file, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"  📊 Stats saved to {stats_file}")

    # Save paper-ready table (Markdown)
    table_file = os.path.join(RESULTS_DIR, f"{case_study}_metrics_table.md")
    with open(table_file, "w") as f:
        f.write(f"# {case_study.upper()} Case Study — Benchmark Results\n\n")
        f.write(f"**Runs:** {stats['successful_runs']} successful, "
                f"{stats['failed_runs']} failed\n\n")
        f.write("| Metric | Mean | Std Dev | Unit |\n")
        f.write("|--------|------|---------|------|\n")

        units = {
            "pipeline_create_time": "seconds",
            "schema_detect_time": "seconds",
            "schema_confirm_time": "seconds",
            "bronze_ingest_time": "seconds",
            "total_e2e_time": "seconds",
            "total_records": "records",
            "throughput_rps": "records/sec",
            "fields_detected": "fields",
            "detection_confidence": "score",
            "ingestion_duration_seconds": "seconds",
        }

        labels = {
            "pipeline_create_time": "Pipeline Creation Time",
            "schema_detect_time": "Schema Detection Time",
            "schema_confirm_time": "Schema Confirmation Time",
            "bronze_ingest_time": "Bronze Ingestion Time",
            "total_e2e_time": "Total E2E Time",
            "total_records": "Total Records",
            "throughput_rps": "Throughput",
            "fields_detected": "Fields Detected",
            "detection_confidence": "Detection Confidence",
            "ingestion_duration_seconds": "Spark Ingestion Duration",
        }

        for key in METRIC_KEYS:
            s = stats.get(key, {"mean": 0, "std": 0})
            f.write(f"| {labels.get(key, key)} | "
                    f"{s['mean']:.4f} | ±{s['std']:.4f} | "
                    f"{units.get(key, '')} |\n")

    print(f"  📋 Paper-ready table saved to {table_file}")


def print_summary(case_study: str, stats: dict):
    """Print a formatted summary to console."""
    print(f"\n{'='*60}")
    print(f"  {case_study.upper()} Case Study — Summary")
    print(f"{'='*60}")
    print(f"  Successful runs: {stats['successful_runs']}")
    print(f"  Failed runs:     {stats['failed_runs']}")
    print()

    labels = {
        "pipeline_create_time": "Pipeline Create",
        "schema_detect_time": "Schema Detect",
        "schema_confirm_time": "Schema Confirm",
        "bronze_ingest_time": "Bronze Ingest",
        "total_e2e_time": "Total E2E",
        "total_records": "Records",
        "throughput_rps": "Throughput (r/s)",
    }

    for key in ["pipeline_create_time", "schema_detect_time", "schema_confirm_time",
                 "bronze_ingest_time", "total_e2e_time", "total_records", "throughput_rps"]:
        s = stats.get(key, {"mean": 0, "std": 0})
        print(f"  {labels.get(key, key):20s}:  {s['mean']:.4f}  ± {s['std']:.4f}")

    print(f"{'='*60}\n")


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Benchmark runner for Autonomous Data Pipeline case studies"
    )
    parser.add_argument("--runs", type=int, default=10,
                        help="Number of repetitions per case study (default: 10)")
    parser.add_argument("--case-study", choices=["csv", "kafka", "api", "all"],
                        default="all",
                        help="Which case study to run (default: all)")
    parser.add_argument("--csv-file", default=DEFAULT_CSV_FILE,
                        help="CSV file for CSV benchmark")
    parser.add_argument("--api-endpoint", default=DEFAULT_API_ENDPOINT,
                        help="REST API endpoint for API benchmark")
    parser.add_argument("--kafka-bootstrap", default=DEFAULT_KAFKA_BOOTSTRAP)
    parser.add_argument("--kafka-topic", default=DEFAULT_KAFKA_TOPIC)
    parser.add_argument("--api-url", default=BASE_URL,
                        help="Backend API URL (default: http://localhost:8000)")

    args = parser.parse_args()
    global BASE_URL
    BASE_URL = args.api_url

    case_studies = (
        ["csv", "kafka", "api"] if args.case_study == "all"
        else [args.case_study]
    )

    print("🚀 Autonomous Data Pipeline — Benchmark Runner")
    print(f"   API URL:  {BASE_URL}")
    print(f"   Runs:     {args.runs}")
    print(f"   Studies:  {', '.join(case_studies)}")
    print()

    # Check API connectivity
    try:
        resp = requests.get(f"{BASE_URL}/docs", timeout=5)
        print("✅ Backend API is reachable\n")
    except Exception as e:
        print(f"❌ Cannot reach backend at {BASE_URL}: {e}")
        print("   Make sure Docker services are running: docker compose up -d")
        sys.exit(1)

    all_results = {}

    for cs in case_studies:
        print(f"{'─'*60}")
        print(f"📦 Running {cs.upper()} case study ({args.runs} runs)...")
        print(f"{'─'*60}")

        runs = []
        for i in range(1, args.runs + 1):
            print(f"  Run {i}/{args.runs}...", end=" ", flush=True)

            if cs == "csv":
                metrics = run_csv_benchmark(args.csv_file, i)
            elif cs == "kafka":
                metrics = run_kafka_benchmark(
                    args.kafka_bootstrap, args.kafka_topic, i
                )
            elif cs == "api":
                metrics = run_api_benchmark(args.api_endpoint, i)

            status = metrics.get("ingestion_status", "unknown")
            e2e = metrics.get("total_e2e_time", 0)
            records = metrics.get("total_records", 0)

            if "error" in metrics:
                print(f"❌ Error: {metrics['error'][:80]}")
            else:
                print(f"✅ {status} — {e2e:.2f}s, {records:,} records")

            runs.append(metrics)

            # Small delay between runs to let system settle
            time.sleep(1)

        # Compute and save stats
        stats = compute_stats(runs)
        save_results(cs, runs, stats)
        print_summary(cs, stats)
        all_results[cs] = {"runs": runs, "stats": stats}

    # Save combined results summary
    summary_file = os.path.join(RESULTS_DIR, "benchmark_summary.json")
    summary = {
        "timestamp": datetime.now().isoformat(),
        "runs_per_study": args.runs,
        "results": {cs: data["stats"] for cs, data in all_results.items()},
    }
    with open(summary_file, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n📊 Combined summary saved to {summary_file}")
    print("✅ Benchmarking complete!")


if __name__ == "__main__":
    main()
