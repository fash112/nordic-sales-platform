"""
Bronze -> Silver: reconcile four divergent country feeds into one conformed table.

This is the layer that earns the platform. Bronze holds each country's file
exactly as it arrived; Silver produces a single schema with typed columns,
parsed dates, normalised decimals, deduplicated rows, and a quality verdict on
every record.

Design decisions worth defending in review
------------------------------------------
1. Bronze is read as STRING throughout. Letting Spark infer types across four
   inconsistent formats produces silent nulls -- a Swedish "4 802,03" inferred
   as a double becomes null, and the revenue is quietly wrong. Parse explicitly.

2. Bad records are FLAGGED, not dropped. A dropped row is invisible; a flagged
   row is countable. Silver keeps everything and marks dq_status, so Gold can
   filter and the dbt tests can assert on how much was excluded and why.

3. Deduplication uses the natural key (country, order_id) and keeps the first
   occurrence by ingestion order. Exact duplicates in these feeds are re-sent
   files rather than genuine repeat orders, so last-write-wins would be
   equivalent -- but the rule is stated rather than left to chance.

4. Negative quantities are RETAINED. They are returns. Filtering them inflates
   revenue, which is the kind of error nobody notices until quarter close.

Run locally:
    pip install pyspark
    python src/silver/build_silver.py --raw data/raw --out data/silver

On Databricks: import as a notebook, or run via Jobs with the same arguments.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# Every country lands on exactly this schema. Adding a country means writing a
# reader that produces these columns -- nothing downstream changes.
CONFORMED = [
    "country",
    "order_id",
    "order_date",
    "customer_no",
    "customer_name",
    "sku",
    "product_name",
    "category",
    "quantity",
    "unit_price_local",
    "currency",
    "channel",
]

CURRENCY = {"SE": "SEK", "NO": "NOK", "DK": "DKK", "FI": "EUR"}

# Static FX to a reporting currency. In production this joins to a rate table
# keyed on date; hardcoding it here keeps the example runnable while making the
# limitation explicit rather than hidden.
FX_TO_EUR = {"SEK": 0.087, "NOK": 0.085, "DKK": 0.134, "EUR": 1.0}


def spark_session(app: str = "nordic-silver") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Parsing helpers -- each returns a Column, so they compose into select()
# ---------------------------------------------------------------------------

def parse_decimal(col: F.Column, decimal_sep: str, thousands_sep: str | None) -> F.Column:
    """Normalise a locale-formatted number string to a Spark decimal.

    Order matters: strip thousands separators BEFORE swapping the decimal
    separator, or a Danish '1.234,56' loses its decimal point.

    Two things this deliberately does NOT do:

    * It does not use a bare .cast(). Under Spark's ANSI mode a malformed value
      raises and kills the job. The whole design of this layer is that bad rows
      are flagged and counted, not fatal -- so the value is validated with a
      regex first and yields NULL when it does not match. assess_quality then
      records it as unparseable_price.

    * It does not assume the thousands separator is a plain space. Locale
      exports use U+0020, U+00A0 (non-breaking) and U+202F (narrow no-break)
      more or less interchangeably, so all whitespace is stripped when the
      separator is space-like.
    """
    cleaned = F.trim(col)

    if thousands_sep is not None:
        if thousands_sep.isspace() or thousands_sep in ("\u00a0", "\u202f"):
            # Any Unicode whitespace, not just the one character we expected.
            cleaned = F.regexp_replace(cleaned, r"\s", "")
        else:
            # translate, NOT regexp_replace. A '.' separator is a regex wildcard
            # and would strip every character in the string, silently emptying
            # the field. translate matches literal characters.
            cleaned = F.translate(cleaned, thousands_sep, "")

    if decimal_sep != ".":
        cleaned = F.translate(cleaned, decimal_sep, ".")

    # Guard before cast: anything not a clean signed decimal becomes NULL.
    is_numeric = cleaned.rlike(r"^-?\d+(\.\d+)?$")
    return F.when(is_numeric, cleaned.cast(T.DecimalType(12, 2))).otherwise(F.lit(None))


def normalise_null(col: F.Column, markers: list[str]) -> F.Column:
    """Collapse country-specific null markers to a real NULL."""
    out = F.trim(col)
    out = F.when(out == "", None).otherwise(out)
    for m in markers:
        out = F.when(out == m, None).otherwise(out)
    return out


# ---------------------------------------------------------------------------
# Per-country readers
# ---------------------------------------------------------------------------

def read_sweden(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "utf-8")
        .csv(f"{path}/sweden/*.csv")
    )
    return df.select(
        F.lit("SE").alias("country"),
        F.col("order_id").cast("string").alias("order_id"),
        F.to_date("order_date", "yyyy-MM-dd").alias("order_date"),
        F.col("cust_id").alias("customer_no"),
        F.col("kundnamn").alias("customer_name"),
        F.col("artikelnr").alias("sku"),
        F.col("produkt").alias("product_name"),
        F.col("kategori").alias("category"),
        F.col("antal").cast("int").alias("quantity"),
        # space thousands, comma decimal. Any whitespace variant is handled.
        parse_decimal(F.col("pris"), ",", " ").alias("unit_price_local"),
        F.lit(CURRENCY["SE"]).alias("currency"),
        normalise_null(F.col("kanal"), []).alias("channel"),
    )


def read_norway(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("sep", ",")
        # Reading latin-1 as utf-8 is the classic mojibake bug. Declare it.
        .option("encoding", "ISO-8859-1")
        .csv(f"{path}/norway/*.csv")
    )
    return df.select(
        F.lit("NO").alias("country"),
        F.col("OrdreNr").cast("string").alias("order_id"),
        F.to_date("Dato", "dd.MM.yyyy").alias("order_date"),
        F.col("KundeNr").alias("customer_no"),
        F.col("KundeNavn").alias("customer_name"),
        F.col("VareNr").alias("sku"),
        F.col("Produkt").alias("product_name"),
        F.col("Kategori").alias("category"),
        F.col("Antall").cast("int").alias("quantity"),
        parse_decimal(F.col("Pris"), ".", None).alias("unit_price_local"),
        F.lit(CURRENCY["NO"]).alias("currency"),
        normalise_null(F.col("Kanal"), ["NULL"]).alias("channel"),
    )


def read_denmark(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.json(f"{path}/denmark/*.jsonl")
    return df.select(
        F.lit("DK").alias("country"),
        F.col("ordre_id").cast("string").alias("order_id"),
        F.to_date("dato", "dd/MM/yyyy").alias("order_date"),
        F.col("kunde_nr").alias("customer_no"),
        F.col("kunde_navn").alias("customer_name"),
        F.col("vare_nr").alias("sku"),
        F.col("produkt").alias("product_name"),
        F.col("kategori").alias("category"),
        F.col("antal").cast("int").alias("quantity"),
        # dot thousands, comma decimal
        parse_decimal(F.col("pris"), ",", ".").alias("unit_price_local"),
        F.lit(CURRENCY["DK"]).alias("currency"),
        normalise_null(F.col("kanal"), []).alias("channel"),
    )


def read_finland(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        # utf-8 handles the BOM; the leading \ufeff is stripped from the header below.
        .option("encoding", "utf-8")
        .csv(f"{path}/finland/*.tsv")
    )
    # Strip a BOM if it attached itself to the first column name.
    first = df.columns[0]
    if first.startswith("\ufeff"):
        df = df.withColumnRenamed(first, first.replace("\ufeff", ""))

    return df.select(
        F.lit("FI").alias("country"),
        F.col("tilaus_id").cast("string").alias("order_id"),
        F.to_date("paivamaara", "yyyyMMdd").alias("order_date"),
        F.col("asiakas_id").alias("customer_no"),
        F.col("asiakas_nimi").alias("customer_name"),
        F.col("tuote_koodi").alias("sku"),
        F.col("tuote").alias("product_name"),
        F.col("luokka").alias("category"),
        F.col("maara").cast("int").alias("quantity"),
        parse_decimal(F.col("hinta"), ",", None).alias("unit_price_local"),
        F.lit(CURRENCY["FI"]).alias("currency"),
        normalise_null(F.col("kanava"), ["-"]).alias("channel"),
    )


READERS = {
    "SE": read_sweden,
    "NO": read_norway,
    "DK": read_denmark,
    "FI": read_finland,
}


# ---------------------------------------------------------------------------
# Quality assessment and conforming
# ---------------------------------------------------------------------------

def assess_quality(df: DataFrame) -> DataFrame:
    """Attach a dq_status and a human-readable reason to every row.

    Rows are never dropped here. Gold decides what to exclude; Silver only
    states what it found, so the exclusion is countable and explainable.
    """
    reasons = F.array_compact(
        F.array(
            F.when(F.col("order_id").isNull(), F.lit("missing_order_id")),
            F.when(F.col("order_date").isNull(), F.lit("unparseable_date")),
            F.when(F.col("unit_price_local").isNull(), F.lit("unparseable_price")),
            F.when(F.col("unit_price_local") < 0, F.lit("negative_price")),
            F.when(F.col("quantity").isNull(), F.lit("missing_quantity")),
            F.when(F.col("quantity") == 0, F.lit("zero_quantity")),
            F.when(F.col("customer_no").isNull(), F.lit("missing_customer")),
            F.when(F.col("sku").isNull(), F.lit("missing_sku")),
            # Mojibake detection: the U+FFFD replacement character surviving into
            # a name means an encoding was mis-declared somewhere upstream.
            F.when(F.col("customer_name").contains("\ufffd"), F.lit("encoding_damage")),
        )
    )

    return df.withColumn("dq_reasons", reasons).withColumn(
        "dq_status",
        F.when(F.size("dq_reasons") == 0, F.lit("pass")).otherwise(F.lit("fail")),
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per (country, order_id), first by ingestion order.

    row_number over a window rather than dropDuplicates, because the rule needs
    to be explicit and auditable -- dropDuplicates gives no control over which
    row survives.
    """
    w = Window.partitionBy("country", "order_id").orderBy("_ingest_seq")
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .withColumn("is_duplicate", F.col("_rn") > 1)
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_silver(spark: SparkSession, raw_path: str) -> DataFrame:
    frames = []
    for code, reader in READERS.items():
        df = reader(spark, raw_path)
        missing = set(CONFORMED) - set(df.columns)
        if missing:
            raise ValueError(f"reader {code} did not produce {sorted(missing)}")
        frames.append(df.select(*CONFORMED))

    union = frames[0]
    for f in frames[1:]:
        union = union.unionByName(f)

    union = union.withColumn(
        "_ingest_seq", F.monotonically_increasing_id()
    ).withColumn("ingested_at", F.current_timestamp())

    deduped = deduplicate(union)
    assessed = assess_quality(deduped)

    # Revenue in local currency, plus a conformed EUR figure so the four
    # countries can be compared without the reader doing mental arithmetic.
    fx = F.create_map(*[x for kv in FX_TO_EUR.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])

    return (
        assessed.withColumn(
            "revenue_local",
            (F.col("quantity") * F.col("unit_price_local")).cast(T.DecimalType(14, 2)),
        )
        .withColumn(
            "revenue_eur",
            (F.col("revenue_local") * fx[F.col("currency")]).cast(T.DecimalType(14, 2)),
        )
        .withColumn("is_return", F.col("quantity") < 0)
        .drop("_ingest_seq")
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw")
    ap.add_argument("--out", default="data/silver")
    ap.add_argument("--format", default="parquet", choices=["parquet", "delta"])
    args = ap.parse_args()

    spark = spark_session()
    silver = build_silver(spark, args.raw)

    (
        silver.write.mode("overwrite")
        .partitionBy("country")
        .format(args.format)
        .save(args.out)
    )

    total = silver.count()
    failed = silver.filter(F.col("dq_status") == "fail").count()
    print(f"silver rows: {total:,}")
    print(f"dq failures: {failed:,} ({failed / total:.2%})")
    silver.groupBy("country", "dq_status").count().orderBy("country", "dq_status").show()
    (
        silver.filter(F.col("dq_status") == "fail")
        .select(F.explode("dq_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
