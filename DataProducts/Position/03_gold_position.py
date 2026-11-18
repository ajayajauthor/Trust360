# =====================================================================================
# Trust360 | Position Data Product | GOLD LAYER
# -------------------------------------------------------------------------------------
# Datasets
#   gold_position              (MV, UC)      : SCD Type 2 position dimension
#   gold_position_scd2_check   (private MV)  : integrity guard (fails the flow if broken)
#   dq_position_summary        (MV, UC)      : data-quality / load summary per snapshot
#
# WHAT  : Builds SCD Type 2 history from the validated snapshots in silver_position,
#         keyed by Position_ID and versioned by datephoto ("gaps-and-islands"):
#           1. hash all tracked business attributes (key and technical columns excluded)
#           2. order every position's snapshots by datephoto
#           3. a new version starts when the hash differs from the previous snapshot
#           4. effective_end_date = next version start - 1 day, open-ended = 9999-12-31
# WHY   : The source is a series of full daily snapshots that can arrive late / out of
#         order and can be re-delivered. A declarative recomputation from the immutable
#         Silver history is deterministic: reruns are idempotent, late snapshots are
#         spliced into the correct position, A->B->A changes produce 3 versions, unchanged
#         snapshots never create versions and absent positions are NOT expired.
#         (See README.md for the AUTO CDC / AUTO CDC FROM SNAPSHOT / MERGE evaluation.)
# WHERE : Third file of the pipeline (reads silver_position / silver_position_validated).
# =====================================================================================

from typing import List

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# -------------------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------------------
BUSINESS_KEY = "Position_ID"
SNAPSHOT_COL = "datephoto"
OPEN_END_DATE = spark.conf.get("position.scd_open_end_date", "9999-12-31")
# Optional explicit list of tracked attributes (comma separated). Empty = every business
# column except the key (schema-evolution aware).
TRACKED_COLUMNS_CONF = spark.conf.get("position.scd_tracked_columns", "")

# Columns that must never reach Gold nor influence the change hash.
TECHNICAL_COLUMNS = {"datephoto", "source_file", "source_file_modification_time",
                     "ingestion_timestamp", "_rescued_data"}

NULL_SENTINEL = "\u0000<NULL>\u0000"  # makes NULL distinct from '' and from 'NULL'
HASH_SEPARATOR = "\u0001"


# -------------------------------------------------------------------------------------
# Reusable helpers
# -------------------------------------------------------------------------------------
def attribute_columns(df: DataFrame) -> List[str]:
    """Business attributes carried into Gold (everything except key + technical)."""
    return [c for c in df.columns if c not in TECHNICAL_COLUMNS and c != BUSINESS_KEY]


def tracked_columns(df: DataFrame) -> List[str]:
    if TRACKED_COLUMNS_CONF.strip():
        return [c.strip() for c in TRACKED_COLUMNS_CONF.split(",") if c.strip()]
    return attribute_columns(df)


def attribute_hash(cols: List[str]) -> Column:
    """Deterministic SHA-256 of the tracked attributes with consistent NULL handling."""
    return F.sha2(
        F.concat_ws(HASH_SEPARATOR, *[F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL)) for c in cols]),
        256,
    )


def build_scd2(snapshots: DataFrame) -> DataFrame:
    """Collapse a (Position_ID, datephoto) snapshot history into SCD Type 2 versions."""
    attrs = attribute_columns(snapshots)
    by_key_in_time = Window.partitionBy(BUSINESS_KEY).orderBy(SNAPSHOT_COL)

    versions = (
        snapshots.withColumn("record_hash", attribute_hash(tracked_columns(snapshots)))
        .withColumn("_prev_hash", F.lag("record_hash").over(by_key_in_time))
        # Scenario 1 (new position) or Scenario 3 (changed attributes) -> version start.
        # Scenario 2 (unchanged) rows are dropped here, so no redundant versions.
        # Scenario 4 (absent from a snapshot) simply has no row -> version stays open.
        .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("record_hash")))
        .withColumn("effective_start_date", F.col(SNAPSHOT_COL))
        .withColumn("_next_start", F.lead("effective_start_date").over(by_key_in_time))
    )

    return versions.select(
        F.sha2(F.concat_ws("|", F.col(BUSINESS_KEY), F.col("effective_start_date").cast("string")), 256)
        .alias("position_sk"),
        BUSINESS_KEY,
        *attrs,
        "record_hash",
        F.row_number().over(by_key_in_time).alias("version_number"),
        "effective_start_date",
        # Scenario 3: previous version expires the day before the new snapshot date.
        F.coalesce(F.date_sub("_next_start", 1), F.to_date(F.lit(OPEN_END_DATE))).alias("effective_end_date"),
        F.col("_next_start").isNull().alias("is_current"),
    )


# -------------------------------------------------------------------------------------
# gold_position: SCD Type 2 dimension (no datephoto / technical metadata)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="gold_position",
    comment=(
        "Position dimension, SCD Type 2 on business key Position_ID. One row per position "
        "version; effective_start_date / effective_end_date (inclusive, open-ended = "
        f"{OPEN_END_DATE}) and is_current. Built from silver_position snapshots (datephoto)."
    ),
    cluster_by=["Position_ID", "effective_end_date"],  # BOOLEAN cannot be a clustering key
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect_all_or_fail(
    {
        "gold_position_id_not_null": "Position_ID IS NOT NULL",
        "gold_valid_interval": "effective_start_date <= effective_end_date",
        "gold_current_is_open_ended": f"is_current = (effective_end_date = DATE'{OPEN_END_DATE}')",
    }
)
def gold_position():
    snapshots = spark.read.table("silver_position").drop("source_file", "ingestion_timestamp")
    return build_scd2(snapshots)


# -------------------------------------------------------------------------------------
# Integrity guard: exactly one current row per Position_ID and no overlapping versions.
# Stays empty while Gold is healthy; any row fails this flow and is logged.
# -------------------------------------------------------------------------------------
@dp.materialized_view(name="gold_position_scd2_check", private=True)
@dp.expect_or_fail("one_current_version_per_position", "current_versions = 1")
@dp.expect_or_fail("no_overlapping_versions", "overlapping_versions = 0")
def gold_position_scd2_check():
    g = spark.read.table("gold_position")
    w = Window.partitionBy("Position_ID").orderBy("effective_start_date")
    per_key = (
        g.withColumn("_prev_end", F.lag("effective_end_date").over(w))
        .groupBy("Position_ID")
        .agg(
            F.sum(F.col("is_current").cast("int")).alias("current_versions"),
            F.sum(F.when(F.col("_prev_end") >= F.col("effective_start_date"), 1).otherwise(0))
            .alias("overlapping_versions"),
        )
    )
    return per_key.filter("current_versions <> 1 OR overlapping_versions <> 0")


# -------------------------------------------------------------------------------------
# dq_position_summary: data-quality & SCD summary per snapshot (refreshed every run)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="dq_position_summary",
    comment=(
        "Per-datephoto DQ and load summary for the Position pipeline: Bronze volume, Silver "
        "valid/rejected by rule, Gold new/changed/unchanged positions, plus current totals."
    ),
    table_properties={"quality": "gold"},
)
def dq_position_summary():
    def has(code: str) -> Column:
        return F.when(F.array_contains("error_codes", code), 1).otherwise(0)

    def has_prefix(prefix: str) -> Column:
        return F.when(F.exists("error_codes", lambda c: c.startswith(prefix)), 1).otherwise(0)

    bronze = (
        spark.read.table("bronze_position").groupBy("datephoto")
        .agg(F.count(F.lit(1)).alias("bronze_records_ingested"),
             F.countDistinct("source_file").alias("source_files"))
    )

    silver = (
        spark.read.table("silver_position_validated").groupBy("datephoto")
        .agg(
            F.count(F.lit(1)).alias("effective_records"),
            F.sum(F.col("is_valid").cast("int")).alias("silver_valid_records"),
            F.sum((~F.col("is_valid")).cast("int")).alias("rejected_records"),
            F.sum(has_prefix("POSITION_TITLE")).alias("invalid_position_title_records"),
            F.sum(has_prefix("DURATION_TYPE")).alias("invalid_duration_type_records"),
            F.sum(F.when(F.array_contains("error_codes", "POSITION_ID_NULL")
                         | F.array_contains("error_codes", "POSITION_ID_BLANK"), 1).otherwise(0))
            .alias("null_blank_position_id_records"),
            F.sum(has("POSITION_ID_DUPLICATE")).alias("duplicate_position_id_records"),
            F.sum(has("MALFORMED_RECORD")).alias("malformed_records"),
            F.sum(F.when(F.size("error_codes") > 1, 1).otherwise(0)).alias("multi_rule_failures"),
        )
    )

    gold = spark.read.table("gold_position")
    versions_started = (
        gold.groupBy(F.col("effective_start_date").alias("datephoto"))
        .agg(
            F.sum(F.when(F.col("version_number") == 1, 1).otherwise(0)).alias("new_positions"),
            F.sum(F.when(F.col("version_number") > 1, 1).otherwise(0)).alias("changed_positions"),
        )
    )
    totals = gold.agg(
        F.sum(F.col("is_current").cast("int")).alias("total_current_positions"),
        F.sum((~F.col("is_current")).cast("int")).alias("total_historical_versions"),
        F.count(F.lit(1)).alias("total_gold_versions"),
    )

    return (
        bronze.join(silver, "datephoto", "left")
        .join(versions_started, "datephoto", "left")
        .na.fill(0, ["new_positions", "changed_positions"])
        .withColumn(
            "unchanged_positions",
            F.coalesce(F.col("silver_valid_records"), F.lit(0))
            - F.col("new_positions") - F.col("changed_positions"),
        )
        .crossJoin(totals)
        .withColumn("summary_generated_at", F.current_timestamp())
    )
