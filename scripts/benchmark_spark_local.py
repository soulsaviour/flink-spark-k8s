"""Generates orders at a few sizes, runs the sales_by_region job on each and
records timings in results/spark-local.md.

  python scripts/benchmark_spark_local.py --sizes 100000 1000000 3000000

Each run also checks that the summary accounts for every distinct completed
order, so a fast but wrong result can't slip through.
"""

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "spark" / "jobs"))

import pyspark  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from generate_sales import generate  # noqa: E402
from sales_by_region import run  # noqa: E402


def environment() -> dict:
    java = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr
    mem_kb = next(
        int(line.split()[1]) for line in open("/proc/meminfo") if line.startswith("MemTotal")
    )
    return {
        "Date": dt.datetime.now(dt.UTC).strftime("%Y-%m-%d %H:%M UTC"),
        "Spark": pyspark.__version__,
        "Java": java.splitlines()[0],
        "Python": sys.version.split()[0],
        "CPUs": os.cpu_count(),
        "Memory": f"{mem_kb / 1024 / 1024:.1f} GiB",
        "Master": "local[*], driver memory 1536m",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[100_000, 1_000_000, 3_000_000])
    parser.add_argument("--out", default=str(ROOT / "results" / "spark-local.md"))
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("sales-by-region-benchmark")
        .config("spark.driver.memory", "1536m")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    results = []
    for size in args.sizes:
        work = Path(tempfile.mkdtemp(prefix="sales-bench-"))
        try:
            started = time.perf_counter()
            generate(spark, size, days=30).write.parquet(str(work / "orders"))
            generate_s = time.perf_counter() - started

            orders = spark.read.parquet(str(work / "orders"))
            input_rows = orders.count()
            expected = orders.where("status = 'completed'").select("order_id").distinct().count()

            started = time.perf_counter()
            summary_rows = run(spark, str(work / "orders"), str(work / "summary"), "parquet")
            job_s = time.perf_counter() - started

            counted = spark.read.parquet(str(work / "summary")).agg({"orders": "sum"}).first()[0]
            assert counted == expected, f"summary counts {counted} orders, expected {expected}"
            results.append((size, input_rows, expected, summary_rows, generate_s, job_s))
            print(f"{size:>10,} rows: generate {generate_s:.1f}s, job {job_s:.1f}s", flush=True)
        finally:
            shutil.rmtree(work, ignore_errors=True)
    spark.stop()

    lines = [
        "# sales_by_region: local Spark runs",
        "",
        "Produced by `scripts/benchmark_spark_local.py`. Synthetic orders over 30",
        "days (about 90% completed, 1% duplicated), written as Parquet, then",
        "summarised by the job. Every run checks the summary accounts for each",
        "distinct completed order.",
        "",
        "## Environment",
        "",
        "| | |",
        "|---|---|",
        *[f"| {k} | {v} |" for k, v in environment().items()],
        "",
        "## Results",
        "",
        "| Orders generated | Input rows | Completed orders | Summary rows "
        "| Generate | Job | Rows/s |",
        "|---:|---:|---:|---:|---:|---:|---:|",
        *[
            f"| {size:,} | {rows:,} | {done:,} | {out:,} | {gen:.1f} s | {job:.1f} s"
            f" | {rows / job:,.0f} |"
            for size, rows, done, out, gen, job in results
        ],
        "",
        "Timings include Parquet I/O on local disk. The first run in a session pays",
        "for JVM warm-up, and small inputs are dominated by fixed costs (scheduling",
        "the shuffle, writing one directory per day), so throughput only means",
        "something at the larger sizes. On one CPU these timings reflect a single",
        "core; on the cluster, executors split the work by partition.",
        "",
    ]
    Path(args.out).write_text("\n".join(lines))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
