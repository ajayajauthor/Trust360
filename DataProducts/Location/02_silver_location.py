# =====================================================================================
# Trust360 | Location Data Product | SILVER LAYER
# -------------------------------------------------------------------------------------
# Datasets
#   silver_location_validated (private MV)  : every effective Bronze record + all DQ results
#   silver_location           (MV, UC)      : records that pass ALL validation rules
#   silver_location_errors    (MV, UC)      : records that fail >= 1 rule, one row per
#                                             source record with ALL failure reasons
#
# WHAT  : Standardises whitespace (for validation; Bronze untouched), applies the
#         configurable Location rules R1-R8 and splits valid vs. rejected records.
# WHY   : Duplicate Location_ID detection needs the complete snapshot (all files of a
#         datephoto), and a later file can retroactively turn an earlier "valid" row into
#         a duplicate. Materialized views recompute declaratively from Bronze, so both
#         Silver tables are always consistent, idempotent and free of duplicate error rows
#         on reruns / file re-deliveries. On serverless they refresh incrementally when the
#         change set allows it.
# WHERE : Third file of the pipeline (reads bronze_location + ref_location_country, feeds
#         03_gold_location.py).
#
# Processing sequence (matches the specification)
#   1 read bronze_location (latest version of every file only)
#   2 standardise whitespace into helper columns _n_<col>
#   3-8 Location_ID / Location_Title / Country_ID / Country_Title / Locality / Postal_Code
#   9 duplicate Location_ID detection (all occurrences, no survivor)
#   10 collect every failure reason   11 split   12 silver_location   13 silver_location_errors
# =====================================================================================

from typing import Dict, List, Tuple

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


# -------------------------------------------------------------------------------------
# Configuration (pipeline settings -> Configuration). Defaults are the agreed initial rules.
# -------------------------------------------------------------------------------------
def _conf(key: str, default: str) -> str:
    return spark.conf.get(f"location.{key}", default)


def _conf_list(key: str, default: str) -> List[str]:
    return [v.strip() for v in _conf(key, default).split(",") if v.strip()]


def _conf_bool(key: str, default: str) -> bool:
    return _conf(key, default).strip().lower() in ("true", "1", "yes", "y")


# Source header -> Silver column name ("Postal Code" contains a space in the CSV header).
COLUMN_ALIASES: Dict[str, str] = dict(
    pair.split(":", 1) for pair in _conf_list("column_aliases", "Postal Code:Postal_Code")
)
# Rule 8: mandatory fields (NULL / empty / whitespace-only -> rejected)
MANDATORY_COLUMNS = _conf_list(
    "mandatory_columns", "Location_ID,Location_Title,Country_ID,Country_Title,Locality,Postal_Code"
)
# Rule 1: agreed Location identifier format (LOC + 3..6 digits, e.g. LOC001; LOC-001 invalid)
LOCATION_ID_REGEX = _conf("location_id_regex", r"^LOC[0-9]{3,6}$")
# Rule 7: uniqueness scope. "snapshot" (default) = unique across ALL files of the same
# datephoto (every daily file is a full snapshot, so an ID legitimately re-appears daily).
# "global" = unique across the complete history.
DUPLICATE_SCOPE = _conf("duplicate_scope", "snapshot").lower()
# Rule 2: optional extra title format (empty = only NULL/blank checks)
LOCATION_TITLE_REGEX = _conf("location_title_regex", "")
# Rule 3: country code format + validation mode ("reference" = must exist & be active in
# ref_location_country; "format" = regex only)
COUNTRY_ID_REGEX = _conf("country_id_regex", r"^[A-Z]{2}$")
COUNTRY_VALIDATION_MODE = _conf("country_validation_mode", "reference").lower()
COUNTRY_ID_UPPERCASE = _conf_bool("country_id_uppercase", "true")
# Rule 4: optional consistency check Country_Title vs reference name (off by default: not
# part of the agreed rules; enable after business confirmation)
COUNTRY_TITLE_MATCH_REFERENCE = _conf_bool("country_title_match_reference", "false")
# Rule 6: "reference" = country-specific regex from ref_location_country (falls back to the
# default regex for unknown countries); "default" = default regex only; "none" = NULL/blank only
POSTAL_CODE_VALIDATION_MODE = _conf("postal_code_validation_mode", "reference").lower()
POSTAL_CODE_DEFAULT_REGEX = _conf("postal_code_default_regex", r"^[A-Za-z0-9][A-Za-z0-9 -]{1,9}$")
# Structural: reject rows Auto Loader could not parse cleanly
REJECT_MALFORMED_RECORDS = _conf_bool("reject_malformed_records", "true")

TECHNICAL_COLUMNS = [
    "datephoto", "datephoto_source", "source_file", "source_file_modification_time",
    "ingestion_timestamp", "_rescued_data", "_corrupt_record",
]
DERIVED_COLUMNS = ["error_details", "error_codes", "rule_names", "is_valid"]

# Field -> (error-code prefix, rule name). Unlisted mandatory fields use R8.
FIELD_RULES: Dict[str, Tuple[str, str]] = {
    "Location_ID": ("LOCATION_ID", "R1_LOCATION_ID"),
    "Location_Title": ("LOCATION_TITLE", "R2_LOCATION_TITLE"),
    "Country_ID": ("COUNTRY_ID", "R3_COUNTRY_ID"),
    "Country_Title": ("COUNTRY_TITLE", "R4_COUNTRY_TITLE"),
    "Locality": ("LOCALITY", "R5_LOCALITY"),
    "Postal_Code": ("POSTAL_CODE", "R6_POSTAL_CODE"),
}
RULE_DUPLICATE = "R7_DUPLICATE_LOCATION_ID"
RULE_MANDATORY_OTHER = "R8_MANDATORY_FIELD"
RULE_STRUCTURE = "R0_RECORD_STRUCTURE"

# Whitespace incl. NBSP, zero-width space and BOM
_WS = r"[\s\u00A0\u200B\uFEFF]"
EDGE_WHITESPACE_REGEX = rf"^{_WS}+|{_WS}+$"
INNER_WHITESPACE_REGEX = rf"{_WS}+"
# Free-text fields whose inner whitespace runs are collapsed to one space
COLLAPSE_INNER_WHITESPACE = {"Location_Title", "Country_Title", "Locality"}


# -------------------------------------------------------------------------------------
# Reusable helper functions (native Spark expressions only - no Python UDFs)
# -------------------------------------------------------------------------------------
def n(col_name: str) -> str:
    """Name of the whitespace-standardised helper column."""
    return f"_n_{col_name}"


def standardize(col_name: str) -> Column:
    """Trim leading/trailing whitespace; collapse inner runs for free text; upper-case
    Country_ID. Used for validation and for the values published in silver_location."""
    c = F.regexp_replace(F.col(f"`{col_name}`"), EDGE_WHITESPACE_REGEX, "")
    if col_name in COLLAPSE_INNER_WHITESPACE:
        c = F.regexp_replace(c, INNER_WHITESPACE_REGEX, " ")
    if col_name == "Country_ID" and COUNTRY_ID_UPPERCASE:
        c = F.upper(c)
    return c


def business_columns(df: DataFrame) -> List[str]:
    """All source CSV columns (schema-evolution aware): everything that is not technical,
    helper (_*) or derived. New CSV columns flow through automatically."""
    return [
        c for c in df.columns
        if c not in TECHNICAL_COLUMNS and c not in DERIVED_COLUMNS and not c.startswith("_")
    ]


def is_blank(c: str) -> Column:
    return F.col(f"`{c}`").isNotNull() & (F.col(n(c)) == "")


def has_value(c: str) -> Column:
    return F.col(n(c)).isNotNull() & (F.col(n(c)) != "")


def rule(code: str, rule_name: str, failed: Column) -> Tuple[str, str, Column]:
    """One rule = (error_code, rule_name, failure condition). NULL conditions count as pass."""
    return code, rule_name, F.coalesce(failed, F.lit(False))


def mandatory_rules(columns: List[str]) -> List[Tuple[str, str, Column]]:
    """Rule 8 (+ NULL/blank part of R1-R6): NULL, empty or whitespace-only."""
    out = []
    for c in columns:
        prefix, rule_name = FIELD_RULES.get(c, (c.upper(), RULE_MANDATORY_OTHER))
        out.append(rule(f"{prefix}_NULL", rule_name, F.col(f"`{c}`").isNull()))
        out.append(rule(f"{prefix}_BLANK", rule_name, is_blank(c)))
    return out


def build_rules() -> List[Tuple[str, str, Column]]:
    """Rule registry. Add a rule by appending one tuple - no other code changes needed."""
    rules = mandatory_rules(MANDATORY_COLUMNS)

    # Rule 1: Location_ID format, Rule 7: duplicates (every occurrence)
    rules.append(rule("LOCATION_ID_INVALID_FORMAT", "R1_LOCATION_ID",
                      has_value("Location_ID") & ~F.col(n("Location_ID")).rlike(LOCATION_ID_REGEX)))
    rules.append(rule("LOCATION_ID_DUPLICATE", RULE_DUPLICATE, F.col("_location_id_occurrences") > 1))

    # Rule 2: optional title format
    if LOCATION_TITLE_REGEX:
        rules.append(rule("LOCATION_TITLE_INVALID_FORMAT", "R2_LOCATION_TITLE",
                          has_value("Location_Title") & ~F.col(n("Location_Title")).rlike(LOCATION_TITLE_REGEX)))

    # Rule 3: Country_ID format and (optionally) reference membership
    cid_ok_format = F.col(n("Country_ID")).rlike(COUNTRY_ID_REGEX)
    rules.append(rule("COUNTRY_ID_INVALID_FORMAT", "R3_COUNTRY_ID", has_value("Country_ID") & ~cid_ok_format))
    if COUNTRY_VALIDATION_MODE == "reference":
        rules.append(rule("COUNTRY_ID_NOT_IN_REFERENCE", "R3_COUNTRY_ID",
                          has_value("Country_ID") & cid_ok_format
                          & ~F.coalesce(F.col("_ref_is_active"), F.lit(False))))

    # Rule 4: optional Country_Title vs reference name
    if COUNTRY_TITLE_MATCH_REFERENCE:
        rules.append(rule("COUNTRY_TITLE_REFERENCE_MISMATCH", "R4_COUNTRY_TITLE",
                          has_value("Country_Title") & F.col("_ref_country_name").isNotNull()
                          & (F.lower(F.col(n("Country_Title"))) != F.lower(F.col("_ref_country_name")))))

    # Rule 5: Locality - NULL/blank only. No geographic consistency check against the
    # country (e.g. Paris / AU) because no authoritative locality reference was provided.

    # Rule 6: Postal_Code (always STRING; format is country specific and configurable)
    if POSTAL_CODE_VALIDATION_MODE in ("reference", "default"):
        rules.append(rule("POSTAL_CODE_INVALID_FORMAT", "R6_POSTAL_CODE",
                          has_value("Postal_Code") & ~F.expr(f"`{n('Postal_Code')}` RLIKE _postal_code_regex")))

    # Structural: malformed CSV row (wrong number of fields / rescued values)
    if REJECT_MALFORMED_RECORDS:
        rules.append(rule("MALFORMED_RECORD", RULE_STRUCTURE,
                          F.col("_rescued_data").isNotNull() | F.col("_corrupt_record").isNotNull()))
    # Snapshot date is required for history (only possible with datephoto_fallback=none)
    rules.append(rule("DATEPHOTO_MISSING", RULE_STRUCTURE, F.col("datephoto").isNull()))
    return rules


def collect_failures(rules: List[Tuple[str, str, Column]]) -> Column:
    """ARRAY<STRUCT<error_code, rule_name>> of EVERY failed rule for the record."""
    return F.filter(
        F.array(*[
            F.when(cond, F.struct(F.lit(code).alias("error_code"), F.lit(rname).alias("rule_name")))
            for code, rname, cond in rules
        ]),
        lambda x: x.isNotNull(),
    )


def ensure_columns(df: DataFrame, cols: List[str]) -> DataFrame:
    """Missing columns (e.g. not yet delivered) become NULL so the rules still evaluate."""
    for c in cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df


# -------------------------------------------------------------------------------------
# Private intermediate: effective Bronze records + validation results (computed once)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_location_validated",
    comment="Pipeline-private: latest version of every Bronze Location record with all DQ results.",
    private=True,
)
@dp.expect("location_id_valid", "NOT array_contains(rule_names, 'R1_LOCATION_ID')")
@dp.expect("location_title_valid", "NOT array_contains(rule_names, 'R2_LOCATION_TITLE')")
@dp.expect("country_id_valid", "NOT array_contains(rule_names, 'R3_COUNTRY_ID')")
@dp.expect("country_title_valid", "NOT array_contains(rule_names, 'R4_COUNTRY_TITLE')")
@dp.expect("locality_valid", "NOT array_contains(rule_names, 'R5_LOCALITY')")
@dp.expect("postal_code_valid", "NOT array_contains(rule_names, 'R6_POSTAL_CODE')")
@dp.expect("location_id_unique", "NOT array_contains(rule_names, 'R7_DUPLICATE_LOCATION_ID')")
@dp.expect("record_well_formed", "NOT array_contains(rule_names, 'R0_RECORD_STRUCTURE')")
def silver_location_validated():
    bronze = spark.read.table("bronze_location")

    # 1. Idempotency: if a file was re-delivered (overwritten), keep only its latest version
    #    so records are never double counted or double rejected.
    latest_version = Window.partitionBy("source_file").orderBy(
        F.col("source_file_modification_time").desc(), F.col("ingestion_timestamp").desc()
    )
    effective = (
        bronze.withColumn("_file_version_rank", F.dense_rank().over(latest_version))
        .filter("_file_version_rank = 1")
        .drop("_file_version_rank")
    )

    # Source header -> Silver names (values unchanged), guarantee rule columns exist
    for src, dst in COLUMN_ALIASES.items():
        if src in effective.columns:
            effective = effective.withColumnRenamed(src, dst)
    effective = ensure_columns(effective, MANDATORY_COLUMNS + list(FIELD_RULES) + ["_rescued_data", "_corrupt_record"])

    # 2. Whitespace standardisation into helper columns (raw columns untouched)
    biz = business_columns(effective)
    normalized = effective.select("*", *[standardize(c).alias(n(c)) for c in biz])

    # Country reference (small -> broadcast) for Rules 3, 4 and 6
    ref = F.broadcast(
        spark.read.table("ref_location_country").select(
            F.col("country_code").alias("_ref_country_code"),
            F.col("country_name").alias("_ref_country_name"),
            F.col("postal_code_regex").alias("_ref_postal_code_regex"),
            F.col("is_active").alias("_ref_is_active"),
        )
    )
    enriched = (
        normalized.join(ref, F.col(n("Country_ID")) == F.col("_ref_country_code"), "left")
        .withColumn(
            "_postal_code_regex",
            F.coalesce(F.col("_ref_postal_code_regex"), F.lit(POSTAL_CODE_DEFAULT_REGEX))
            if POSTAL_CODE_VALIDATION_MODE == "reference" else F.lit(POSTAL_CODE_DEFAULT_REGEX),
        )
    )

    # 9. Duplicate detection across ALL files of the scope (not per file). Every occurrence
    #    of a duplicated Location_ID is flagged - no arbitrary survivor is chosen.
    dup_keys = [n("Location_ID")] if DUPLICATE_SCOPE == "global" else ["datephoto", n("Location_ID")]
    with_dups = enriched.withColumn(
        "_location_id_occurrences",
        F.when(has_value("Location_ID"), F.count(F.lit(1)).over(Window.partitionBy(*dup_keys))),
    )

    # 3-10. Evaluate every rule and keep ALL failure reasons
    return (
        with_dups.withColumn("error_details", collect_failures(build_rules()))
        .withColumn("error_codes", F.transform("error_details", lambda x: x["error_code"]))
        .withColumn("rule_names", F.array_distinct(F.transform("error_details", lambda x: x["rule_name"])))
        .withColumn("is_valid", F.size("error_details") == 0)
        .drop("_ref_country_code", "_ref_postal_code_regex")
    )


# -------------------------------------------------------------------------------------
# 12. silver_location: validated, standardised records (one row per Location_ID per datephoto)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_location",
    comment=(
        "Validated Location records: pass Rules R0-R8. Values whitespace-standardised "
        "(Country_ID upper-case); Postal_Code kept as STRING. One row per Location_ID per datephoto."
    ),
    cluster_by=["datephoto", "Location_ID"],
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect_all_or_fail(
    {
        "silver_location_id_present": "Location_ID IS NOT NULL AND Location_ID <> ''",
        "silver_datephoto_present": "datephoto IS NOT NULL",
    }
)
def silver_location():
    v = spark.read.table("silver_location_validated").filter("is_valid")
    biz = business_columns(v)
    return v.select(
        *[F.col(n(c)).alias(c) for c in biz],
        "datephoto",
        "source_file",
        "ingestion_timestamp",
    )


# -------------------------------------------------------------------------------------
# 13. silver_location_errors: rejected records with ORIGINAL values and all reasons
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_location_errors",
    comment=(
        "Rejected Location records: original (raw) values, metadata and every failed rule. "
        "One row per source record; error_reason / rule_name are comma separated lists."
    ),
    cluster_by=["datephoto"],
    table_properties={"quality": "silver"},
)
def silver_location_errors():
    v = spark.read.table("silver_location_validated").filter("NOT is_valid")
    biz = business_columns(v)
    return v.select(
        *biz,                                   # original values (not trimmed)
        "_rescued_data",
        "_corrupt_record",
        "datephoto",
        "source_file",
        "source_file_modification_time",
        "ingestion_timestamp",
        F.array_join("error_codes", ", ").alias("error_reason"),
        F.array_join("rule_names", ", ").alias("rule_name"),
        "error_codes",
        "rule_names",
        F.size("error_codes").alias("error_count"),
        F.current_timestamp().alias("error_timestamp"),
        F.current_date().alias("run_date"),
    )
