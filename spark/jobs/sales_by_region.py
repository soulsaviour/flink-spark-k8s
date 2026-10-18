"""Orders, revenue and average order value per region and day.

  spark-submit sales_by_region.py --input <path> --output <path> [--input-format csv|parquet]

Only completed orders count. Duplicate order IDs, missing amounts and negative
amounts are dropped, and blank regions are reported as "unknown". Output is
Parquet partitioned by day.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType

SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("region", StringType()),
        StructField("amount", DecimalType(12, 2)),
        StructField("status", StringType()),
        StructField("ordered_at", TimestampType()),
    ]
)


def aggregate(orders: DataFrame) -> DataFrame:
    completed = (
        orders.where(F.col("status") == "completed")
        .where(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        .dropDuplicates(["order_id"])
        .withColumn(
            "region", F.coalesce(F.nullif(F.lower(F.trim("region")), F.lit("")), F.lit("unknown"))
        )
        .withColumn("day", F.to_date("ordered_at"))
    )
    return (
        completed.groupBy("day", "region")
        .agg(
            F.count("*").alias("orders"),
            F.sum("amount").alias("revenue"),
            F.round(F.avg("amount"), 2).alias("avg_order_value"),
        )
        .orderBy("day", "region")
    )


def run(spark: SparkSession, input_path: str, output_path: str, input_format: str = "csv") -> int:
    """Reads orders, writes the summary, returns the number of summary rows."""
    reader = spark.read.schema(SCHEMA)
    if input_format == "csv":
        orders = reader.option("header", True).csv(input_path)
    else:
        orders = reader.parquet(input_path)

    summary = aggregate(orders).cache()
    summary.write.mode("overwrite").partitionBy("day").parquet(output_path)
    return summary.count()


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--input-format", choices=["csv", "parquet"], default="csv")
    args = parser.parse_args(argv)

    spark = (
        SparkSession.builder.appName("sales-by-region")
        # Days are cut in UTC no matter where the job runs.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        rows = run(spark, args.input, args.output, args.input_format)
        print(f"wrote {rows} summary rows to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
