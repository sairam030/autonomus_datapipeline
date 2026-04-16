## Table 4: CSV Scalability (10K to 500K records)

*Mean ± std over 10 runs per data size.*

| Data Size | Schema Detect (s) | Bronze Ingest (s) | Silver Transform (s) | Gold Transform (s) | Total E2E (s) | Throughput (rec/s) |
|---|---|---|---|---|---|---|
| 10K | 0.57 ± 0.04 | 3.31 ± 0.45 | 5.09 ± 0.64 | 3.26 ± 0.33 | 12.64 ± 1.15 | 2898 ± 336 |
| 50K | 1.21 ± 0.07 | 8.46 ± 1.23 | 12.06 ± 1.61 | 10.20 ± 1.69 | 33.20 ± 1.69 | 6040 ± 254 |
| 100K | 2.14 ± 0.12 | 15.30 ± 1.08 | 22.15 ± 1.82 | 16.73 ± 1.82 | 57.31 ± 4.43 | 6279 ± 750 |
| 500K | 8.38 ± 0.91 | 63.32 ± 5.88 | 86.77 ± 5.02 | 58.31 ± 5.37 | 217.78 ± 12.69 | 8089 ± 664 |
