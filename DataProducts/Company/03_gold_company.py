# =====================================================================================
# Trust360 | Company Data Product | GOLD LAYER
# -------------------------------------------------------------------------------------
# Datasets
#   gold_company_snapshot_changes (private MV) : one row per Company_ID per datephoto with
#                                                change_type NEW / CHANGED / UNCHANGED
#   gold_company                  (MV, UC)     : SCD Type 2 Company dimension
#   gold_company_scd2_check       (private MV) : integrity guard (fails the flow if broken)
#   dq_company_summary            (MV, UC)     : data-quality / load summary per snapshot
#
# WHAT  : Builds SCD Type 2 history from the validated snapshots in silver_company, keyed
#         by Company_ID and versioned by datephoto ("gaps-and-islands"):
#           1. SHA-256 hash of the tracked attributes Company_Name, Location_ID,
#              Is_InternalCompany (Company_ID + technical cols excluded, NULL-safe)
#           2. order every Company's valid snapshots by datephoto
#           3. a version starts when the hash differs from the previous valid snapshot
#           4. effective_end_date = next version start - 1 day; open-ended = 9999-12-31
# WHY   : The source is a series of full daily snapshots that can arrive late / out of
#         order and can be re-delivered. A declarative recomputation from the Silver history
#         is deterministic: reruns are idempotent, late snapshots are spliced into the right
#         position, A->B->A produces 3 versions, unchanged snapshots never create versions
#         and Companies absent from a snapshot are NOT expired (no deactivation rule given).
#         See README.md for the AUTO CDC / AUTO CDC FROM SNAPSHOT / MERGE evaluation.
# WHERE : Third (last) file of the pipeline.
# =====================================================================================

from typing import List

from pyspark import pipelines as dp
from pyspark.sql import Column, Window
from pyspark.sql import functions as F

# -------------------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------------------
BUSINESS_KEY = "Company_ID"
SNAPSHOT_COL = "datephoto"
OPEN_END_DATE = spark.conf.get("company.scd_open_end_date", "9999-12-31")
# Tracked attributes (key excluded). These are also the attributes published in Gold.
TRACKED_COLUMNS: List[str] = [
    c.strip()
    for c in spark.conf.get(
        "company.scd_tracked_columns", "Company_Name,Location_ID,Is_InternalCompany"
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
# Snapshot-level change classification (shared by gold_company and the DQ summary)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="gold_company_snapshot_changes",
    comment="Pipeline-private: Company_ID x datephoto with record_hash and change_type.",
    private=True,
)
def gold_company_snapshot_changes():
    snapshots = spark.read.table("silver_company").select(BUSINESS_KEY, SNAPSHOT_COL, *TRACKED_COLUMNS)
    w = key_in_time()
    return (
        snapshots.withColumn("record_hash", attribute_hash(TRACKED_COLUMNS))
        .withColumn("_prev_hash", F.lag("record_hash").over(w))
        .withColumn(
            "change_type",
            F.when(F.col("_prev_hash").isNull(), F.lit("NEW"))                    # Scenario 1
            .when(F.col("_prev_hash") != F.col("record_hash"), F.lit("CHANGED"))  # Scenario 3
            .otherwise(F.lit("UNCHANGED")),                                        # Scenario 2
        )
        .drop("_prev_hash")
    )


# -------------------------------------------------------------------------------------
# gold_company: SCD Type 2 dimension (no datephoto / technical metadata)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="gold_company",
    comment=(
        "Company dimension, SCD Type 2 on business key Company_ID. One row per Company "
        "version; effective_start_date / effective_end_date (inclusive, open-ended = "
        f"{OPEN_END_DATE}) and is_current. Built from silver_company snapshots (datephoto)."
    ),
    cluster_by=["Company_ID", "effective_end_date"],  # BOOLEAN cannot be a clustering key
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect_all_or_fail(
    {
        "gold_company_id_not_null": "Company_ID IS NOT NULL",
        "gold_valid_interval": "effective_start_date <= effective_end_date",
        "gold_current_is_open_ended": f"is_current = (effective_end_date = DATE'{OPEN_END_DATE}')",
    }
)
def gold_company():
    w = key_in_time()
    versions = (
        spark.read.table("gold_company_snapshot_changes")
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
# Integrity guard: exactly one current row per Company_ID and no overlapping versions.
# Stays empty while Gold is healthy; any row fails this flow and is logged.
# -------------------------------------------------------------------------------------
@dp.materialized_view(name="gold_company_scd2_check", private=True)
@dp.expect_or_fail("one_current_version_per_company", "current_versions = 1")
@dp.expect_or_fail("no_overlapping_versions", "overlapping_versions = 0")
def gold_company_scd2_check():
    g = spark.read.table("gold_company")
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
# dq_company_summary: data-quality & SCD summary per snapshot (refreshed every run)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="dq_company_summary",
    comment=(
        "Per-datephoto DQ and load summary for the Company pipeline: Bronze volume, Silver "
        "valid/rejected by rule, Location reference misses, Gold new/changed/unchanged "
        "Companies, plus current totals."
    ),
    table_properties={"quality": "gold"},
)
def dq_company_summary():
    def has_rule(rule_name: str) -> Column:
        return F.sum(F.when(F.array_contains("rule_names", rule_name), 1).otherwise(0))

    def has_code(code: str) -> Column:
        return F.sum(F.when(F.array_contains("error_codes", code), 1).otherwise(0))

    bronze = (
        spark.read.table("bronze_company").groupBy("datephoto")
        .agg(F.count(F.lit(1)).alias("bronze_records_ingested"),
             F.countDistinct("source_file").alias("source_files"))
    )

    silver = (
        spark.read.table("silver_company_validated").groupBy("datephoto")
        .agg(
            F.count(F.lit(1)).alias("effective_records"),
            F.sum(F.col("is_valid").cast("int")).alias("silver_valid_records"),
            F.sum((~F.col("is_valid")).cast("int")).alias("rejected_records"),
            has_rule("R1_COMPANY_ID").alias("invalid_company_id_records"),
            has_rule("R2_COMPANY_NAME").alias("invalid_company_name_records"),
            has_rule("R3_LOCATION_ID").alias("invalid_location_id_records"),
            has_rule("R4_IS_INTERNAL_COMPANY").alias("invalid_is_internal_company_records"),
            has_rule("R5_DUPLICATE_COMPANY_ID").alias("duplicate_company_id_records"),
            has_rule("R6_MANDATORY_FIELD").alias("other_mandatory_field_records"),
            has_rule("R0_RECORD_STRUCTURE").alias("malformed_records"),
            F.sum(F.when(F.col("location_ref_status") == "NOT_FOUND", 1).otherwise(0))
            .alias("location_ref_not_found_records"),
            has_code("LOCATION_ID_NOT_IN_REFERENCE").alias("location_ref_rejected_records"),
        )
    )

    changes = (
        spark.read.table("gold_company_snapshot_changes").groupBy("datephoto")
        .agg(
            F.sum(F.when(F.col("change_type") == "NEW", 1).otherwise(0)).alias("new_companies"),
            F.sum(F.when(F.col("change_type") == "CHANGED", 1).otherwise(0)).alias("changed_companies"),
            F.sum(F.when(F.col("change_type") == "UNCHANGED", 1).otherwise(0)).alias("unchanged_companies"),
        )
    )

    gold_totals = spark.read.table("gold_company").agg(
        F.sum(F.col("is_current").cast("int")).alias("total_current_companies"),
        F.sum((~F.col("is_current")).cast("int")).alias("total_historical_versions"),
        F.count(F.lit(1)).alias("total_gold_versions"),
    )

    zero_fill = [
        "effective_records", "silver_valid_records", "rejected_records",
        "invalid_company_id_records", "invalid_company_name_records",
        "invalid_location_id_records", "invalid_is_internal_company_records",
        "duplicate_company_id_records", "other_mandatory_field_records", "malformed_records",
        "location_ref_not_found_records", "location_ref_rejected_records",
        "new_companies", "changed_companies", "unchanged_companies",
    ]
    return (
        bronze.join(silver, "datephoto", "full")
        .join(changes, "datephoto", "full")
        .fillna(0, subset=zero_fill + ["bronze_records_ingested", "source_files"])
        .crossJoin(gold_totals)
        .withColumn("summary_generated_at", F.current_timestamp())
    )
