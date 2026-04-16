## Table 5: Kafka Scalability (Message Rate)

*Mean ± std over 10 runs per message rate.*

| Message Rate | Schema Detect (s) | Bronze/Batch (s) | Records/Batch | Throughput (rec/s) |
|---|---|---|---|---|
| 50 msg/s | 2.94 ± 0.35 | 7.90 ± 1.29 | 2978 ± 127 | 342 ± 27 |
| 100 msg/s | 3.00 ± 0.42 | 14.41 ± 2.87 | 6016 ± 81 | 411 ± 51 |
| 500 msg/s | 3.38 ± 0.45 | 45.08 ± 4.13 | 29732 ± 709 | 690 ± 63 |
| 1000 msg/s | 4.01 ± 0.54 | 83.06 ± 6.76 | 60602 ± 1806 | 727 ± 47 |
