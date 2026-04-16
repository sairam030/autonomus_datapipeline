# Performance Baseline Comparison

Estimated performance comparison based on literature, documentation, and community benchmarks.

| Metric | Our Model | Apache Airflow | Prefect | Dagster | Apache NiFi |
|---|---|---|---|---|---|
| Pipeline Setup Time | ~2-5 min (UI + AI) | ~1-4 hours (manual DAG) | ~30-60 min (Python) | ~30-60 min (Python) | ~15-30 min (drag-drop) |
| Schema Detection | Automatic (seconds) | Manual | Manual | Manual | Semi-auto (limited) |
| Code Generation | AI-generated PySpark | Manual Python | Manual Python | Manual Python | N/A (processors) |
| Avg. Ingestion (10K CSV) | ~3-8s * | ~5-15s (operator overhead) | ~3-10s (Python native) | ~3-10s (Python native) | ~2-5s (native streaming) |
| Avg. Ingestion (100K CSV) | ~10-30s * | ~20-60s (Spark operator) | ~15-45s (Python) | ~15-45s (Python) | ~8-20s (streaming) |
| Avg. Ingestion (500K CSV) | ~30-90s * | ~60-180s (Spark operator) | ~60-150s (Python) | ~60-150s (Python) | ~25-60s (streaming) |
| Multi-layer Transform | Bronze→Silver→Gold (auto) | Custom DAG per layer | Custom flow per layer | Asset per layer | N/A (single-pass) |
| Human Effort (lines of code) | ~0 (AI generates) | ~50-200 per DAG | ~30-100 per flow | ~40-120 per asset | ~0 (UI config) |

### Notes
- * Our Model times are estimates pending benchmark results. Run benchmark_runner.py to get actual measurements.
- Times for other tools are gathered from documentation, community benchmarks, and published studies.
- Performance varies significantly based on hardware, data complexity, and configuration.