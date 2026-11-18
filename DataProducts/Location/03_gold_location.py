# =====================================================================================
# Trust360 | Location Data Product | GOLD LAYER
# -------------------------------------------------------------------------------------
# Datasets
#   gold_location_snapshot_changes (private MV) : one row per Location_ID per datephoto with
#                                                 change_type NEW / CHANGED / UNCHANGED
#   gold_location                  (MV, UC)     : SCD Type 2 Location dimension
#   gold_location_scd2_check       (private MV) : integrity guard (fails the flow if broken)
#   dq_location_summary            (MV, UC)     : data-quality / load summary per snapshot
#
# WHAT  : Builds SCD Type 2 history from the validated snapshots in silver_location, keyed
#         by Location_ID and versioned by datephoto ("gaps-and-islands"):
#           1. SHA-256 hash of the tracked attributes (Location_ID + technical cols excluded,
#              NULL-safe via sentinel)
#           2. order every Location's snapshots by datephoto
#           3. a version starts when the hash differs from the previous valid snapshot
#           4. effective_end_date = next version start - 1 day; open-ended = 9999-12-31
# WHY   : The source is a series of full daily snapshots that can arrive late / out of
#         order and can be re-delivered. A declarative recomputation from the Silver history
#         is deterministic: reruns are idempotent, late snapshots are spliced into the right
#         position, A->B->A produces 3 versions, unchanged snapshots never create versions
#         and Locations absent from a snapshot are NOT expired (no deactivation rule given).
#         See README.md for the AUTO CDC / AUTO CDC FROM SNAPSHOT / MERGE evaluation.
# WHERE : Fourth (last) file of the pipeline.
# =====================================================================================

from typing import List

from pyspark import pipelines as dp
from pyspark.sql import Column, Window
from pyspark.sql import functions as F

# -------------------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------------------
BUSINESS_KEY = "Location_ID"
SNAPSHOT_COL = "datephoto"
OPEN_END_DATE = spark.conf.get("location.scd_open_end_date", "9999-12-31")
# Tracked attributes = the Location business columns carried into Gold (key excluded).
TRACKED_COLUMNS: List[str] = [
    c.strip()
    for c in spark.conf.get(
        "location.scd_tracked_columns", "Location_Title,Country_ID,Country_Title,Locality,Postal_Code"
    ).split(",")
    if c.strip() and c.strip() != BUSINESS_KEY
]

NULL_SENTINEL = "\u0000<NULL>\u0000"  # makes NULL distinct from '' and from the text 'NULL'
HASH_SEPARATOR = "\u0001"


# -------------------------------------------------------------------------------------
# Reusable helpers
# -------------------------------------------------------------------------------------
def attribute_hash(cols: List[str]) -> Column:
    """Deterministic SHA-256 of the tracked attributes with consistent NULL handling."""
    return F.sha2(
        F.concat_ws(HASH_SEPARATOR, *[F.coalesce(F.col(c).cast("string"), F.lit(NULL_SENTINEL)) for c in cols]),
        256,
    )


def key_in_time() -> Window:
    return Window.partitionBy(BUSINESS_KEY).orderBy(SNAPSHOT_COL)


# -------------------------------------------------------------------------------------
# Snapshot-level change classification (shared by gold_location and the DQ summary)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="gold_location_snapshot_changes",
    comment="Pipeline-private: Location_ID x datephoto with record_hash and change_type.",
    private=True,
)
def gold_location_snapshot_changes():
    snapshots = spark.read.table("silver_location").select(BUSINESS_KEY, SNAPSHOT_COL, *TRACKED_COLUMNS)
    w = key_in_time()
    return (
        snapshots.withColumn("record_hash", attribute_hash(TRACKED_COLUMNS))
        .withColumn("_prev_hash", F.lag("record_hash").over(w))
        .withColumn(
            "change_type",
            F.when(F.col("_prev_hash").isNull(), F.lit("NEW"))            # Scenario 1
            .when(F.col("_prev_hash") != F.col("record_hash"), F.lit("CHANGED"))  # Scenario 3
            .otherwise(F.lit("UNCHANGED")),                                # Scenario 2
        )
        .drop("_prev_hash")
    )


# -------------------------------------------------------------------------------------
# gold_location: SCD Type 2 dimension (no datephoto / technical metadata)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="gold_location",
    comment=(
        "Location dimension, SCD Type 2 on business key Location_ID. One row per Location "
        "version; effective_start_date / effective_end_date (inclusive, open-ended = "
        f"{OPEN_END_DATE}) and is_current. Built from silver_location snapshots (datephoto)."
    ),
    cluster_by=["Location_ID", "effective_end_date"],  # BOOLEAN cannot be a clustering key
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect_all_or_fail(
    {
        "gold_location_id_not_null": "Location_ID IS NOT NULL",
        "gold_valid_interval": "effective_start_date <= effective_end_date",
        "gold_current_is_open_ended": f"is_current = (effective_end_date = DATE'{OPEN_END_DATE}')",
    }
)
def gold_location():
    w = key_in_time()
    versions = (
        spark.read.table("gold_location_snapshot_changes")
        .filter("change_type <> 'UNCHANGED'")              # Scenario 2: no new version
        .withColumn("_next_start", F.lead(SNAPSHOT_COL).over(w))
    )
    return versions.select(
        BUSINESS_KEY,
        *TRACKED_COLUMNS,
        F.col(SNAPSHOT_COL).alias("effective_start_date"),
        # Scenario 3: previous version expires the day before the new snapshot date
        F.coalesce(F.date_sub("_next_start", 1), F.to_date(F.lit(OPEN_END_DATE))).alias("effective_end_date"),
        F.col("_next_start").isNull().alias("is_current"),
    )


# -------------------------------------------------------------------------------------
# Integrity guard: exactly one current row per Location_ID and no overlapping versions.
# Stays empty while Gold is healthy; any row fails this flow and is logged.
# -------------------------------------------------------------------------------------
@dp.materialized_view(name="gold_location_scd2_check", private=True)
@dp.expect_or_fail("one_current_version_per_location", "current_versions = 1")
@dp.expect_or_fail("no_overlapping_versions", "overlapping_versions = 0")
def gold_location_scd2_check():
    g = spark.read.table("gold_location")
    w = Window.partitionBy(BUSINESS_KEY).orderBy("effective_start_date")
    per_key = (
        g.withColumn("_prev_end", F.lag("effective_end_date").over(w))
        .groupBy(BUSINESS_KEY)
        .agg(
            F.sum(F.col("is_current").cast("int")).alias("current_versions"),
            F.sum(F.when(F.col("_prev_end") >= F.col("effective_start_date"), 1).otherwise(0))
            .alias("overlapping_versions"),
        )
    )
    return per_key.filter("current_versions <> 1 OR overlapping_versions <> 0")


# -------------------------------------------------------------------------------------
# dq_location_summary: data-quality & SCD summary per snapshot (refreshed every run)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="dq_location_summary",
    comment=(
        "Per-datephoto DQ and load summary for the Location pipeline: Bronze volume, Silver "
        "valid/rejected by rule, Gold new/changed/unchanged Locations, plus current totals."
    ),
    table_properties={"quality": "gold"},
)
def dq_location_summary():
    def has_rule(rule_name: str) -> Column:
        return F.sum(F.when(F.array_contains("rule_names", rule_name), 1).otherwise(0))

    bronze = (
        spark.read.table("bronze_location").groupBy("datephoto")
        .agg(F.count(F.lit(1)).alias("bronze_records_ingested"),
             F.countDistinct("source_file").alias("source_files"))
    )

    silver = (
        spark.read.table("silver_location_validated").groupBy("datephoto")
        .agg(
            F.count(F.lit(1)).alias("effective_records"),
            F.sum(F.col("is_valid").cast("int")).alias("silver_valid_records"),
            F.sum((~F.col("is_valid")).cast("int")).alias("rejected_records"),
            has_rule("R1_LOCATION_ID").alias("invalid_location_id_records"),
            has_rule("R2_LOCATION_TITLE").alias("invalid_location_title_records"),
            has_rule("R3_COUNTRY_ID").alias("invalid_country_id_records"),
            has_rule("R4_COUNTRY_TITLE").alias("invalid_country_title_records"),
            has_rule("R5_LOCALITY").alias("invalid_locality_records"),
            has_rule("R6_POSTAL_CODE").alias("invalid_postal_code_records"),
            has_rule("R7_DUPLICATE_LOCATION_ID").alias("duplicate_location_id_records"),
            has_rule("R0_RECORD_STRUCTURE").alias("malformed_records"),
        )
    )

    changes = (
        spark.read.table("gold_location_snapshot_changes").groupBy("datephoto")
        .agg(
            F.sum(F.when(F.col("change_type") == "NEW", 1).otherwise(0)).alias("new_locations"),
            F.sum(F.when(F.col("change_type") == "CHANGED", 1).otherwise(0)).alias("changed_locations"),
            F.sum(F.when(F.col("change_type") == "UNCHANGED", 1).otherwise(0)).alias("unchanged_locations"),
        )
    )

    gold_totals = spark.read.table("gold_location").agg(
        F.sum(F.col("is_current").cast("int")).alias("total_current_locations"),
        F.sum((~F.col("is_current")).cast("int")).alias("total_historical_versions"),
        F.count(F.lit(1)).alias("total_gold_versions"),
    )

    zero_fill = [
        "effective_records", "silver_valid_records", "rejected_records",
        "invalid_location_id_records", "invalid_location_title_records",
        "invalid_country_id_records", "invalid_country_title_records",
        "invalid_locality_records", "invalid_postal_code_records",
        "duplicate_location_id_records", "malformed_records",
        "new_locations", "changed_locations", "unchanged_locations",
    ]
    return (
        bronze.join(silver, "datephoto", "full")
        .join(changes, "datephoto", "full")
        .fillna(0, subset=zero_fill + ["bronze_records_ingested", "source_files"])
        .crossJoin(gold_totals)
        .withColumn("summary_generated_at", F.current_timestamp())
        .orderBy("datephoto")
    )
