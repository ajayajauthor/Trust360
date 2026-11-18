# =====================================================================================
# Trust360 | Position Data Product | SILVER LAYER
# -------------------------------------------------------------------------------------
# Datasets
#   silver_position_validated (private MV)  : every effective Bronze record + all DQ flags
#   silver_position           (MV, UC)      : records that pass ALL validation rules
#   silver_position_errors    (MV, UC)      : records that fail >= 1 rule, one row per
#                                             source record with ALL failure reasons
#
# WHAT  : Standardises whitespace (for validation only), applies Position_Title /
#         DurationType / Position_ID / mandatory-field / duplicate-Position_ID /
#         malformed-row rules and splits valid vs invalid records.
# WHY   : Duplicate Position_ID detection needs the complete snapshot (all files of a
#         datephoto), and a later file can retroactively turn an earlier "valid" row into
#         a duplicate. Materialized views recompute declaratively from Bronze, so both
#         Silver tables are always mutually consistent, idempotent and free of duplicate
#         error rows on reruns. On serverless they refresh incrementally when possible.
# WHERE : Second file of the pipeline (reads bronze_position, feeds 03_gold_position.py).
# =====================================================================================

from typing import List, Tuple

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# -------------------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------------------
# Scope of Position_ID uniqueness. "snapshot" (default): a Position_ID must be unique
# across ALL files that share the same datephoto (each daily file is a full snapshot, so
# the same Position_ID legitimately re-appears every day). "global": unique over history.
ID_DUPLICATE_SCOPE = spark.conf.get("position.id_duplicate_scope", "snapshot").lower()

# Explicitly forbidden DurationType values (comma separated, case-insensitive).
FORBIDDEN_DURATION_TYPES = [
    v.strip().upper()
    for v in spark.conf.get("position.forbidden_duration_types", "INVALID_TYPE").split(",")
    if v.strip()
]

# Technical (non-business) columns produced by Bronze / Auto Loader.
TECHNICAL_COLUMNS = [
    "datephoto",
    "source_file",
    "source_file_modification_time",
    "ingestion_timestamp",
    "_rescued_data",
]

# Mandatory business fields (Rule 4).
COL_ID, COL_TITLE, COL_DURATION = "Position_ID", "Position_Title", "DurationType"

# -------------------------------------------------------------------------------------
# Reusable validation patterns (Java regex, evaluated natively by Spark - no UDFs)
# -------------------------------------------------------------------------------------
# DurationType: one or more alphabetic words optionally separated by a single space,
# '-' or '_' (e.g. Permanent, Fixed-Term, Part Time). Digits / symbols are rejected.
DURATION_TYPE_REGEX = r"^[A-Za-z]+(?:[ _-][A-Za-z]+)*$"
# Leading/trailing whitespace incl. NBSP, zero-width space and BOM.
EDGE_WHITESPACE_REGEX = r"^[\s\u00A0\u200B\uFEFF]+|[\s\u00A0\u200B\uFEFF]+$"


# -------------------------------------------------------------------------------------
# Reusable helper functions
# -------------------------------------------------------------------------------------
def normalize_ws(c: Column) -> Column:
    """Strip leading/trailing (incl. unicode) whitespace. Inner spaces are kept."""
    return F.regexp_replace(c, EDGE_WHITESPACE_REGEX, "")


def mandatory_rules(raw: Column, norm: Column, prefix: str, label: str) -> List[Tuple[str, str, Column]]:
    """NULL / blank rules shared by every mandatory field (Rule 4)."""
    return [
        (f"{prefix}_NULL", f"Null {label}", raw.isNull()),
        (f"{prefix}_BLANK", f"Blank {label}", raw.isNotNull() & (norm == "")),
    ]


DERIVED_COLUMNS = ["error_codes", "rule_names", "is_valid"]


def business_columns(df: DataFrame) -> List[str]:
    """All source CSV columns (schema-evolution aware): everything that is not a technical,
    helper (_*) or derived validation column. New CSV columns flow through automatically."""
    return [
        c for c in df.columns
        if c not in TECHNICAL_COLUMNS and c not in DERIVED_COLUMNS and not c.startswith("_")
    ]


def build_rules() -> List[Tuple[str, str, Column]]:
    """Rule registry: (error_code, rule_name, failure_condition). Conditions reference the
    normalised helper columns _id_n / _title_n / _duration_n and _id_dup_count.
    Add a new rule by appending one tuple - no other code changes required."""
    id_raw, id_n = F.col(COL_ID), F.col("_id_n")
    title_raw, title_n = F.col(COL_TITLE), F.col("_title_n")
    dur_raw, dur_n = F.col(COL_DURATION), F.col("_duration_n")
    dur_forbidden = F.upper(dur_n).isin(FORBIDDEN_DURATION_TYPES)
    return (
        # Rule 1 + 4: Position_Title
        mandatory_rules(title_raw, title_n, "POSITION_TITLE", "Position_Title")
        # Rule 2 + 4: DurationType
        + mandatory_rules(dur_raw, dur_n, "DURATION_TYPE", "DurationType")
        + [
            ("DURATION_TYPE_INVALID_VALUE", "Invalid DurationType value", (dur_n != "") & dur_forbidden),
            ("DURATION_TYPE_INVALID_FORMAT", "Invalid DurationType format",
             (dur_n != "") & ~dur_forbidden & ~dur_n.rlike(DURATION_TYPE_REGEX)),
        ]
        # Rule 3 + 4: Position_ID
        + mandatory_rules(id_raw, id_n, "POSITION_ID", "Position_ID")
        + [("POSITION_ID_DUPLICATE", "Duplicate Position_ID", (id_n != "") & (F.col("_id_dup_count") > 1))]
        # Structural: CSV row that did not match the header/schema
        + [("MALFORMED_RECORD", "Malformed CSV record", F.col("_rescued_data").isNotNull())]
    )


def collect_failures(rules: List[Tuple[str, str, Column]], pick: int) -> Column:
    """ARRAY of every failed rule (index 0 = code, 1 = name) - captures ALL failures."""
    return F.filter(
        F.array(*[F.when(rule[2], F.lit(rule[pick])) for rule in rules]),
        lambda x: x.isNotNull(),
    )


# -------------------------------------------------------------------------------------
# Private intermediate: effective Bronze records + validation results (computed once)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_position_validated",
    comment="Pipeline-private: latest version of every Bronze record with all DQ flags.",
    private=True,
)
@dp.expect("position_title_valid", "NOT exists(error_codes, c -> c LIKE 'POSITION_TITLE%')")
@dp.expect("duration_type_valid", "NOT exists(error_codes, c -> c LIKE 'DURATION_TYPE%')")
@dp.expect("position_id_not_null_or_blank", "NOT exists(error_codes, c -> c IN ('POSITION_ID_NULL','POSITION_ID_BLANK'))")
@dp.expect("position_id_unique", "NOT array_contains(error_codes, 'POSITION_ID_DUPLICATE')")
@dp.expect("record_well_formed", "NOT array_contains(error_codes, 'MALFORMED_RECORD')")
def silver_position_validated():
    bronze = spark.read.table("bronze_position")

    # 1. Idempotency: if a file was re-delivered (overwritten) and re-ingested, keep only
    #    its latest version so records are never double counted or double rejected.
    latest_version = Window.partitionBy("source_file").orderBy(
        F.col("source_file_modification_time").desc(), F.col("ingestion_timestamp").desc()
    )
    effective = (
        bronze.withColumn("_file_version_rank", F.dense_rank().over(latest_version))
        .filter("_file_version_rank = 1")
        .drop("_file_version_rank")
    )

    # 2. Whitespace standardisation for validation only (raw columns untouched).
    normalized = (
        effective.withColumn("_id_n", normalize_ws(F.col(COL_ID)))
        .withColumn("_title_n", normalize_ws(F.col(COL_TITLE)))
        .withColumn("_duration_n", normalize_ws(F.col(COL_DURATION)))
    )

    # 3. Duplicate detection across ALL files of the scope (not per file). Every
    #    occurrence of a duplicated Position_ID is flagged - no arbitrary survivor.
    dup_keys = ["_id_n"] if ID_DUPLICATE_SCOPE == "global" else ["datephoto", "_id_n"]
    with_dups = normalized.withColumn(
        "_id_dup_count",
        F.when(F.col("_id_n") != "", F.count(F.lit(1)).over(Window.partitionBy(*dup_keys))),
    )

    # 4. Evaluate every rule and keep ALL failure reasons.
    rules = build_rules()
    return (
        with_dups.withColumn("error_codes", collect_failures(rules, 0))
        .withColumn("rule_names", collect_failures(rules, 1))
        .withColumn("is_valid", F.size("error_codes") == 0)
    )


# -------------------------------------------------------------------------------------
# silver_position: validated position records
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_position",
    comment=(
        "Validated Position records (all DQ rules passed). One row per Position_ID per "
        "datephoto. Business values are whitespace-trimmed; lineage columns retained."
    ),
    cluster_by=["datephoto", "Position_ID"],
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect_all_or_fail(
    {
        "silver_position_id_present": "Position_ID IS NOT NULL AND Position_ID <> ''",
        "silver_position_title_present": "Position_Title IS NOT NULL AND Position_Title <> ''",
        "silver_duration_type_present": "DurationType IS NOT NULL AND DurationType <> ''",
    }
)
def silver_position():
    validated = spark.read.table("silver_position_validated").filter("is_valid")
    return validated.select(
        *[normalize_ws(F.col(c)).alias(c) for c in business_columns(validated)],
        "datephoto",
        "source_file",
        "ingestion_timestamp",
    )


# -------------------------------------------------------------------------------------
# silver_position_errors: rejected records (one row per source record, all reasons)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_position_errors",
    comment=(
        "Position records that failed >= 1 DQ rule. Original (raw) values preserved. "
        "error_reason = all failed rule codes, rule_name = readable rule names, "
        "run_date / error_timestamp = pipeline run that evaluated the rules."
    ),
    cluster_by=["datephoto"],
    table_properties={"quality": "silver", "delta.enableDeletionVectors": "true"},
)
def silver_position_errors():
    validated = spark.read.table("silver_position_validated").filter(~F.col("is_valid"))
    return validated.select(
        *business_columns(validated),              # original values exactly as received
        "_rescued_data",
        "datephoto",
        "source_file",
        "ingestion_timestamp",
        F.array_join("error_codes", ", ").alias("error_reason"),
        F.array_join("rule_names", ", ").alias("rule_name"),
        "error_codes",
        F.size("error_codes").alias("error_count"),
        F.current_date().alias("run_date"),
        F.current_timestamp().alias("error_timestamp"),
    )
