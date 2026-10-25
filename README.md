# flink-spark-k8s

Apache Flink and Apache Spark on Kubernetes, each run by its operator: a
stateful Flink streaming job with checkpoints, savepoint-based upgrades and
HA, and a PySpark batch job that summarises orders from S3. The Spark job has
tests, and both engines have results from real runs in `results/`.

## Layout

```
operators/install.sh            cert-manager, Flink operator 1.16.1, Spark operator 2.5.2
operators/*-values.yaml         operator Helm values
flink/state-machine.yaml        FlinkDeployment: StateMachineExample, RocksDB, HA
flink/state-pvc.yaml            shared volume for checkpoints, savepoints, HA data
spark/spark-pi.yaml             SparkApplication smoke test
spark/sales-by-region.yaml      SparkApplication for the PySpark job, S3 via IRSA
spark/jobs/                     sales_by_region.py and a synthetic data generator
spark/Dockerfile                Spark 4.2 image with the jobs and the S3A connector
tests/                          Spark job tests (local Spark session)
scripts/                        local benchmark and Flink run scripts
results/                        output of those runs
```

## Installing the operators

```bash
operators/install.sh
```

Installs cert-manager (the Flink operator's webhook needs it), then both
operators with pinned chart versions. Each operator only watches its own job
namespace (`flink`, `spark-jobs`) and creates the service account and RBAC
the job pods use there.

## Flink

`flink/state-machine.yaml` runs Flink's StateMachineExample in application
mode:

- RocksDB state with periodic checkpoints, so a restart resumes from the last
  checkpoint instead of reprocessing everything.
- `upgradeMode: savepoint`: when the spec changes, the operator takes a
  savepoint, stops the job and starts the new version from it.
- Kubernetes HA: a lost JobManager pod comes back from the latest checkpoint.

```bash
kubectl apply -f flink/state-pvc.yaml -f flink/state-machine.yaml
kubectl -n flink get flinkdeployment state-machine
```

The PVC must be ReadWriteMany (EFS, NFS) on a multi-node cluster, because the
JobManager and every TaskManager write to it. S3 is the usual production
choice.

## Spark

`spark-pi.yaml` checks that submission, RBAC and executors work.
`sales-by-region.yaml` runs `spark/jobs/sales_by_region.py`: completed orders
only, duplicates and bad amounts dropped, regions normalised, then orders,
revenue and average order value per region and day, written as Parquet
partitioned by day. It runs with dynamic allocation, retries on failure, and
reads S3 through IRSA rather than access keys.

```bash
docker build -t ghcr.io/<you>/spark-jobs:0.1.0 spark/
kubectl apply -f spark/sales-by-region.yaml
kubectl -n spark-jobs get sparkapplication sales-by-region -w
```

The image bakes in `hadoop-aws` 3.5.0 (matching Spark 4.2's Hadoop) and the
AWS SDK bundle version it was built against, so jobs don't download jars at
start-up.

## Tests and results

```bash
pip install -r requirements-dev.txt
pytest
```

The tests run the job on a local Spark session: the cleaning and aggregation
rules, CSV in to partitioned Parquet out, and the shape of the generated data.

`results/spark-local.md` and `results/flink-local.md` come from
`scripts/benchmark_spark_local.py` and `scripts/run_flink_local.sh`, run on a
single-CPU machine:

- **Spark**: the job summarised 3 million generated orders in about 20
  seconds, and every run checks the summary accounts for each distinct
  completed order.
- **Flink**: WordCount counted 10 million words in about 33 seconds with the
  totals matching exactly, and the StateMachine job completed 29 of 30
  checkpoints in a minute, averaging 145 ms each.

They're single-machine numbers, useful as a baseline and a sanity check rather
than as cluster performance. Re-run the scripts to produce your own.
