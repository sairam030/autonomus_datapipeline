## Table 3: Performance Metrics Across Case Studies

*Mean ± standard deviation over 10 independent runs.*

| Metric | CSV (50K) | Kafka (Stream) | REST API (500) |
|---|---|---|---|
| Schema Detection Time (s) | 1.24 ± 0.07 | 3.23 ± 0.44 | 1.80 ± 0.16 |
| Detection Confidence | 0.91 ± 0.01 | 0.87 ± 0.03 | 0.95 ± 0.01 |
| Fields Detected | 10 ± 0 | 6 ± 0 | 15 ± 0 |
| Bronze Ingestion Time (s) | 8.08 ± 0.81 | 14.94 ± 1.88 | 6.39 ± 0.80 |
| Silver Transform Time (s) | 12.09 ± 1.12 | 10.64 ± 1.33 | 8.83 ± 1.17 |
| Gold Transform Time (s) | 9.75 ± 1.46 | 11.16 ± 1.20 | 7.29 ± 0.59 |
| Total E2E Time (s) | 33.17 ± 2.31 | 39.14 ± 5.16 | 24.74 ± 2.26 |
