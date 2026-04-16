## Table 6: REST API Scalability

*Mean ± std over 10 runs per data volume.*

| Data Size | Schema Detect (s) | Bronze Ingest (s) | Total Records | Throughput (rec/s) |
|---|---|---|---|---|
| 500 rec | 1.82 ± 0.16 | 6.78 ± 1.00 | 500 ± 0 | 81 ± 9 |
| 2500 rec | 1.81 ± 0.13 | 17.65 ± 2.07 | 2500 ± 0 | 140 ± 8 |
| 5000 rec | 1.89 ± 0.13 | 33.26 ± 3.47 | 5000 ± 0 | 155 ± 14 |
| 10000 rec | 2.06 ± 0.19 | 58.27 ± 4.95 | 10000 ± 0 | 167 ± 16 |
