# Flink examples: local runs

Produced by `scripts/run_flink_local.sh` on a local standalone cluster
(one JobManager, one TaskManager with two slots, parallelism 2).

## Environment

| | |
|---|---|
| Date | 2026-10-30 05:44 UTC |
| Flink | Version: 2.2.1, Commit ID: 450c63e |
| Java | openjdk version "21.0.10" 2026-01-20 |
| CPUs | 1 |
| Memory | 3.9 GiB |

## WordCount (batch mode)

| Words in | Distinct words | Words counted | Job runtime | Words/s |
|---:|---:|---:|---:|---:|
| 10,000,000 | 5,000 | 10,000,000 | 32.9 s | 303,601 |

The counts add up to exactly the number of words generated.

## StateMachineExample (streaming, RocksDB state, checkpointing)

Ran for 60 s with a checkpoint every 2 s (the example sets its own interval).

| Checkpoints completed | Failed | Avg duration | Max duration | Last checkpoint size |
|---:|---:|---:|---:|---:|
| 29 | 1 | 145 ms | 690 ms | 84 KiB |

Flink's REST record counters stayed at zero for this example's generator
source, so no throughput figure is given here.
