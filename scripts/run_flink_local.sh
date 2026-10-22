#!/usr/bin/env bash
# Runs two Flink examples on a local standalone cluster and writes the results
# to results/flink-local.md:
#   1. WordCount over a generated text file, in batch mode, checking that the
#      counts add up to the number of words written.
#   2. StateMachineExample for RUN_SECONDS with RocksDB checkpoints every 10 s,
#      then throughput and checkpoint statistics from the REST API.
#
#   FLINK_HOME=/opt/flink-2.2.1 scripts/run_flink_local.sh
set -euo pipefail
cd "$(dirname "$0")/.."

: "${FLINK_HOME:?set FLINK_HOME to a Flink 2.2 distribution}"
RUN_SECONDS=${RUN_SECONDS:-60}
WORDS=${WORDS:-10000000}

work=$(mktemp -d)
export FLINK_CONF_DIR=$work/conf
mkdir -p "$FLINK_CONF_DIR"
cp "$FLINK_HOME"/conf/* "$FLINK_CONF_DIR"/
# Start from the distribution's config, which carries the --add-opens JVM
# flags Flink needs on Java 17+, and override only what this run needs.
python3 - "$FLINK_CONF_DIR/config.yaml" "$work" <<'PY'
import sys

import yaml

path, work = sys.argv[1], sys.argv[2]
config = yaml.safe_load(open(path))


def put(dotted, value):
    node = config
    *parents, leaf = dotted.split(".")
    for key in parents:
        node = node.setdefault(key, {})
    node[leaf] = value


for key, value in {
    "jobmanager.rpc.address": "localhost",
    "jobmanager.bind-host": "localhost",
    "jobmanager.memory.process.size": "1024m",
    "taskmanager.host": "localhost",
    "taskmanager.bind-host": "localhost",
    "taskmanager.numberOfTaskSlots": 2,
    "taskmanager.memory.process.size": "1536m",
    "parallelism.default": 2,
    "rest.address": "localhost",
    "rest.bind-address": "localhost",
    "rest.port": 8081,
    "execution.checkpointing.interval": "10s",
    "execution.checkpointing.dir": f"file://{work}/checkpoints",
    "state.backend.type": "rocksdb",
}.items():
    put(key, value)
yaml.safe_dump(config, open(path, "w"), sort_keys=False)
PY

"$FLINK_HOME/bin/start-cluster.sh" > /dev/null
trap '"$FLINK_HOME/bin/stop-cluster.sh" > /dev/null 2>&1 || true; rm -rf "$work"' EXIT
for _ in $(seq 1 60); do
  curl -fs localhost:8081/overview > /dev/null && break
  sleep 1
done

echo "== WordCount over $WORDS words"
python3 - "$work/words.txt" "$WORDS" <<'PY'
import random
import sys

path, words = sys.argv[1], int(sys.argv[2])
vocabulary = [f"word{i}" for i in range(5000)]
rng = random.Random(42)
with open(path, "w") as fh:
    for _ in range(words // 10):
        fh.write(" ".join(rng.choices(vocabulary, k=10)) + "\n")
PY
wordcount_log=$("$FLINK_HOME/bin/flink" run "$FLINK_HOME/examples/streaming/WordCount.jar" \
  --input "$work/words.txt" --output "$work/wordcount" --execution-mode BATCH 2>&1)
wordcount_ms=$(grep -oE 'Job Runtime: [0-9]+ ms' <<< "$wordcount_log" | grep -oE '[0-9]+')

echo "== StateMachineExample for ${RUN_SECONDS}s"
submit_log=$("$FLINK_HOME/bin/flink" run -d "$FLINK_HOME/examples/streaming/StateMachineExample.jar" 2>&1)
job_id=$(grep -oE 'JobID [0-9a-f]{32}' <<< "$submit_log" | awk '{print $2}')
sleep "$RUN_SECONDS"
curl -fs "localhost:8081/jobs/$job_id" > "$work/job.json"
curl -fs "localhost:8081/jobs/$job_id/checkpoints" > "$work/checkpoints.json"
curl -fs "localhost:8081/jobs/$job_id/checkpoints/config" > "$work/checkpoint-config.json"
"$FLINK_HOME/bin/flink" cancel "$job_id" > /dev/null

python3 - "$work" "$WORDS" "$wordcount_ms" "$RUN_SECONDS" "$("$FLINK_HOME/bin/flink" --version 2>/dev/null | head -1)" <<'PY'
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

work, words, wordcount_ms, run_seconds, flink_version = sys.argv[1:6]
words, wordcount_ms, run_seconds = int(words), int(wordcount_ms), int(run_seconds)

counts = {}
for part in Path(work, "wordcount").rglob("*"):
    if part.is_file() and not part.name.startswith("."):
        for line in part.read_text().splitlines():
            word, count = line.strip("()").split(",")
            counts[word] = counts.get(word, 0) + int(count)
assert sum(counts.values()) == words, f"counted {sum(counts.values())} words, wrote {words}"

job = json.load(open(Path(work, "job.json")))
checkpoints = json.load(open(Path(work, "checkpoints.json")))
interval_s = json.load(open(Path(work, "checkpoint-config.json")))["interval"] / 1000
vertices = [
    (v["name"], v["parallelism"], v["metrics"]["read-records"], v["metrics"]["write-records"])
    for v in job["vertices"]
]
# Events handed from the generator to the keyed state machine operator.
records = max(read for _, _, read, _ in vertices)
completed = checkpoints["counts"]["completed"]
failed = checkpoints["counts"]["failed"]
duration = checkpoints["summary"]["end_to_end_duration"]
latest = checkpoints["latest"]["completed"] or {}

java = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr.splitlines()[0]
mem_kb = next(int(l.split()[1]) for l in open("/proc/meminfo") if l.startswith("MemTotal"))

lines = [
    "# Flink examples: local runs",
    "",
    "Produced by `scripts/run_flink_local.sh` on a local standalone cluster",
    "(one JobManager, one TaskManager with two slots, parallelism 2).",
    "",
    "## Environment",
    "",
    "| | |",
    "|---|---|",
    f"| Date | {dt.datetime.now(dt.UTC):%Y-%m-%d %H:%M UTC} |",
    f"| Flink | {flink_version} |",
    f"| Java | {java} |",
    f"| CPUs | {os.cpu_count()} |",
    f"| Memory | {mem_kb / 1024 / 1024:.1f} GiB |",
    "",
    "## WordCount (batch mode)",
    "",
    "| Words in | Distinct words | Words counted | Job runtime | Words/s |",
    "|---:|---:|---:|---:|---:|",
    f"| {words:,} | {len(counts):,} | {sum(counts.values()):,} | {wordcount_ms / 1000:.1f} s"
    f" | {words / (wordcount_ms / 1000):,.0f} |",
    "",
    "The counts add up to exactly the number of words generated.",
    "",
    "## StateMachineExample (streaming, RocksDB state, checkpointing)",
    "",
    f"Ran for {run_seconds} s with a checkpoint every {interval_s:g} s (the example sets"
    " its own interval).",
    "",
    "| Checkpoints completed | Failed | Avg duration | Max duration | Last checkpoint size |",
    "|---:|---:|---:|---:|---:|",
    f"| {completed} | {failed} | {duration['avg']} ms | {duration['max']} ms"
    f" | {latest.get('checkpointed_size', latest.get('state_size', 0)) / 1024:,.0f} KiB |",
    "",
    *(
        [
            "| Operator | Parallelism | Records in | Records out |",
            "|---|---:|---:|---:|",
            *[f"| {name} | {par} | {rin:,} | {rout:,} |" for name, par, rin, rout in vertices],
        ]
        if records
        else [
            "Flink's REST record counters stayed at zero for this example's generator",
            "source, so no throughput figure is given here.",
        ]
    ),
    "",
]
Path("results/flink-local.md").write_text("\n".join(lines))
print("\n".join(lines))
PY
