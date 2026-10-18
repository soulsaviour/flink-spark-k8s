"""Writes synthetic orders for tests and benchmarks.

  spark-submit generate_sales.py --rows 1000000 --days 30 --output <path> [--format parquet]

About 90% of orders are completed, and 1% are duplicated, so the
aggregation has something to clean up.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

REGIONS = ["north", "south", "east", "west", "central"]
START_EPOCH = 1790812800  # 2026-10-01T00:00:00Z


def generate(spark: SparkSession, rows: int, days: int, seed: int = 7) -> DataFrame:
    region_index = (F.abs(F.hash("id", F.lit(seed))) % len(REGIONS) + 1).cast("int")
    status = (
        F.when(F.rand(seed + 1) < 0.9, "completed")
        .when(F.rand(seed + 2) < 0.5, "cancelled")
        .otherwise("refunded")
    )
    orders = spark.range(rows).select(
        F.format_string("O-%010d", "id").alias("order_id"),
        F.element_at(F.array(*[F.lit(r) for r in REGIONS]), region_index).alias("region"),
        (F.round(F.rand(seed) * 495 + 5, 2)).cast("decimal(12,2)").alias("amount"),
        status.alias("status"),
        F.timestamp_seconds(
            F.lit(START_EPOCH) + (F.rand(seed + 3) * days * 86400).cast("long")
        ).alias("ordered_at"),
    )
    return orders.unionByName(orders.sample(fraction=0.01, seed=seed))


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--output", required=True)
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.appName("generate-sales").getOrCreate()
    try:
        writer = generate(spark, args.rows, args.days).write.mode("overwrite")
        if args.format == "csv":
            writer.option("header", True).csv(args.output)
        else:
            writer.parquet(args.output)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
