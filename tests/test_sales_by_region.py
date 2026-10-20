import csv
import datetime as dt
from decimal import Decimal

from generate_sales import generate
from sales_by_region import SCHEMA, aggregate, run

ORDERS = [
    ("O-1", "North", Decimal("100.00"), "completed", dt.datetime(2026, 10, 1, 9, 0)),
    ("O-2", "north ", Decimal("50.50"), "completed", dt.datetime(2026, 10, 1, 18, 30)),
    ("O-2", "north ", Decimal("50.50"), "completed", dt.datetime(2026, 10, 1, 18, 30)),
    ("O-3", "South", Decimal("75.00"), "cancelled", dt.datetime(2026, 10, 1, 10, 0)),
    ("O-4", "South", Decimal("20.00"), "completed", dt.datetime(2026, 10, 2, 11, 0)),
    ("O-5", "", Decimal("10.00"), "completed", dt.datetime(2026, 10, 2, 12, 0)),
    ("O-6", "East", None, "completed", dt.datetime(2026, 10, 2, 13, 0)),
    ("O-7", "East", Decimal("-5.00"), "completed", dt.datetime(2026, 10, 2, 14, 0)),
]

EXPECTED = [
    ("2026-10-01", "north", 2, Decimal("150.50"), Decimal("75.25")),
    ("2026-10-02", "south", 1, Decimal("20.00"), Decimal("20.00")),
    ("2026-10-02", "unknown", 1, Decimal("10.00"), Decimal("10.00")),
]


def as_tuples(rows):
    return [
        (r.day.isoformat(), r.region, r.orders, r.revenue, r.avg_order_value)
        for r in sorted(rows, key=lambda r: (r.day, r.region))
    ]


def test_aggregate_cleans_and_summarises(spark):
    # Duplicate O-2 counted once; cancelled, missing and negative amounts
    # dropped; regions normalised; blank region reported as "unknown".
    result = aggregate(spark.createDataFrame(ORDERS, SCHEMA)).collect()
    assert as_tuples(result) == EXPECTED


def test_run_reads_csv_and_writes_partitioned_parquet(spark, tmp_path):
    source = tmp_path / "orders.csv"
    with source.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["order_id", "region", "amount", "status", "ordered_at"])
        for order_id, region, amount, status, ordered_at in ORDERS:
            writer.writerow(
                [order_id, region, "" if amount is None else amount, status, ordered_at.isoformat()]
            )

    output = tmp_path / "summary"
    assert run(spark, str(source), str(output)) == 3

    assert sorted(p.name for p in output.iterdir() if p.is_dir()) == [
        "day=2026-10-01",
        "day=2026-10-02",
    ]
    assert as_tuples(spark.read.parquet(str(output)).collect()) == EXPECTED


def test_generated_data_has_the_expected_shape(spark):
    orders = generate(spark, rows=10_000, days=7)
    total = orders.count()
    distinct = orders.select("order_id").distinct().count()

    assert distinct == 10_000
    assert 10_050 <= total <= 10_150  # about 1% duplicates
    assert {r.region for r in orders.select("region").distinct().collect()} <= {
        "north",
        "south",
        "east",
        "west",
        "central",
    }
    completed = orders.where("status = 'completed'").count() / total
    assert 0.85 < completed < 0.95
