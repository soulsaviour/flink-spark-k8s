# sales_by_region: local Spark runs

Produced by `scripts/benchmark_spark_local.py`. Synthetic orders over 30
days (about 90% completed, 1% duplicated), written as Parquet, then
summarised by the job. Every run checks the summary accounts for each
distinct completed order.

## Environment

| | |
|---|---|
| Date | 2026-10-30 05:32 UTC |
| Spark | 4.2.1 |
| Java | openjdk version "21.0.10" 2026-01-20 |
| Python | 3.12.3 |
| CPUs | 1 |
| Memory | 3.9 GiB |
| Master | local[*], driver memory 1536m |

## Results

| Orders generated | Input rows | Completed orders | Summary rows | Generate | Job | Rows/s |
|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 100,953 | 90,188 | 150 | 9.3 s | 26.7 s | 3,784 |
| 1,000,000 | 1,009,934 | 900,687 | 150 | 5.7 s | 17.2 s | 58,880 |
| 3,000,000 | 3,029,930 | 2,700,679 | 150 | 8.3 s | 19.8 s | 153,375 |

Timings include Parquet I/O on local disk. The first run in a session pays
for JVM warm-up, and small inputs are dominated by fixed costs (scheduling
the shuffle, writing one directory per day), so throughput only means
something at the larger sizes. On one CPU these timings reflect a single
core; on the cluster, executors split the work by partition.
