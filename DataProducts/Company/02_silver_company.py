# =====================================================================================
# Trust360 | Company Data Product | SILVER LAYER
# -------------------------------------------------------------------------------------
# Datasets
#   silver_company_validated (private MV) : every effective Bronze record + all DQ results
#   silver_company           (MV, UC)     : records that pass ALL validation rules
#   silver_company_errors    (MV, UC)     : records that fail >= 1 rule, one row per source
#                                           record with ALL failure reasons
#
# WHAT  : Standardises whitespace (for validation; Bronze untouched), applies the
#         configurable Company rules R0-R7 and splits valid vs. rejected records.
# WHY   : Duplicate Company_ID detection needs the complete snapshot (all files of a
#         datephoto), and a later file can retroactively turn an earlier "valid" row into
#         a duplicate. Materialized views recompute declaratively from Bronze, so both
#         Silver tables are always consistent, idempotent and free of duplicate error rows
#         on reruns / file re-deliveries. On serverless they refresh incrementally when the
#         change set allows it.
# WHERE : Second file of the pipeline (reads bronze_company and - optionally - the approved
#         Location reference table; feeds 03_gold_company.py).
#
# Processing sequence (matches the specification)
#   1  read bronze_company (latest version of every file only)
#   2  standardise whitespace into helper columns _n_<col>
#   3  R1 Company_ID   4 R2 Company_Name   5 R3 Location_ID   6 R4 Is_InternalCompany
#   7  R5 duplicate Company_ID (all occurrences, no survivor)
#   8  R7 optional Location reference validation (off | warn | reject)
#   9  collect every failure reason  10 split  11 silver_company  12 silver_company_errors
#   R6 = mandatory NULL/blank check for any additional configured mandatory column
#   R0 = structural checks (malformed CSV row, unresolved datephoto)
# =====================================================================================

from typing import Dict, List, Tuple

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


# -------------------------------------------------------------------------------------
# Configuration (pipeline settings -> Configuration). Defaults are the agreed initial rules.
# -------------------------------------------------------------------------------------
def _conf(key: str, default: str) -> str:
    return spark.conf.get(f"company.{key}", default)


def _conf_list(key: str, default: str) -> List[str]:
    return [v.strip() for v in _conf(key, default).split(",") if v.strip()]


def _conf_bool(key: str, default: str) -> bool:
    return _conf(key, default).strip().lower() in ("true", "1", "yes", "y")


# Rule 6: mandatory fields (NULL / empty / whitespace-only -> rejected)
MANDATORY_COLUMNS = _conf_list("mandatory_columns", "Company_ID,Company_Name,Location_ID,Is_InternalCompany")
# Rule 1: permitted characters (alphanumeric only: COMP-001 / COMP 001 invalid; 123ABC valid)
COMPANY_ID_REGEX = _conf("company_id_regex", r"^[A-Za-z0-9]+$")
# Rule 1: optional stricter naming convention, e.g. ^COMP[0-9]{3,}$ (empty = not enforced)
COMPANY_ID_CONVENTION_REGEX = _conf("company_id_convention_regex", "")
# Rule 5: uniqueness scope. "snapshot" (default) = unique across ALL files of the same
# datephoto (every daily file is a full snapshot, so an ID legitimately re-appears daily).
# "global" = unique across the complete history.
DUPLICATE_SCOPE = _conf("duplicate_scope", "snapshot").lower()
# Rule 2: optional extra name format (empty = only NULL/blank checks; names are free text)
COMPANY_NAME_REGEX = _conf("company_name_regex", "")
# Rule 3: agreed Location identifier format (LOC + 3..6 digits, aligned with Location product)
LOCATION_ID_REGEX = _conf("location_id_regex", r"^LOC[0-9]{3,6}$")
# Rule 7: Location reference validation: "off" | "warn" (default, flag only) | "reject"
LOCATION_REF_MODE = _conf("location_ref_mode", "warn").lower()
LOCATION_REF_TABLE = _conf("location_ref_table", "team_2.hr_location.gold_location")
# "any_version" (default): Location_ID exists in any SCD2 version; "current": current only
LOCATION_REF_MATCH = _conf("location_ref_match", "any_version").lower()
# Rule 4: allowed values + case sensitivity (y/n valid only when case_sensitive=false)
INTERNAL_ALLOWED_VALUES = _conf_list("internal_company_allowed_values", "Y,N")
INTERNAL_CASE_SENSITIVE = _conf_bool("internal_company_case_sensitive", "true")
# Structural: reject rows Auto Loader could not parse cleanly
REJECT_MALFORMED_RECORDS = _conf_bool("reject_malformed_records", "true")

REF_ENABLED = LOCATION_REF_MODE in ("warn", "reject")

TECHNICAL_COLUMNS = [
    "datephoto", "datephoto_source", "source_file", "source_file_modification_time",
    "ingestion_timestamp", "_rescued_data", "_corrupt_record",
]
DERIVED_COLUMNS = ["error_details", "error_codes", "rule_names", "is_valid", "location_ref_status"]

# Field -> (error-code prefix, rule name). Unlisted mandatory fields use R6.
FIELD_RULES: Dict[str, Tuple[str, str]] = {
    "Company_ID": ("COMPANY_ID", "R1_COMPANY_ID"),
    "Company_Name": ("COMPANY_NAME", "R2_COMPANY_NAME"),
    "Location_ID": ("LOCATION_ID", "R3_LOCATION_ID"),
    "Is_InternalCompany": ("INTERNAL_COMPANY", "R4_IS_INTERNAL_COMPANY"),
}
RULE_DUPLICATE = "R5_DUPLICATE_COMPANY_ID"
RULE_MANDATORY_OTHER = "R6_MANDATORY_FIELD"
RULE_LOCATION_REF = "R7_LOCATION_REFERENCE"
RULE_STRUCTURE = "R0_RECORD_STRUCTURE"

# Whitespace incl. NBSP, zero-width space and BOM
_WS = r"[\s\u00A0\u200B\uFEFF]"
EDGE_WHITESPACE_REGEX = rf"^{_WS}+|{_WS}+$"
INNER_WHITESPACE_REGEX = rf"{_WS}+"
# Free-text fields whose inner whitespace runs are collapsed to one space
COLLAPSE_INNER_WHITESPACE = {"Company_Name"}


# -------------------------------------------------------------------------------------
# Reusable helper functions (native Spark expressions only - no Python UDFs)
# -------------------------------------------------------------------------------------
def n(col_name: str) -> str:
    """Name of the whitespace-standardised helper column."""
    return f"_n_{col_name}"


def standardize(col_name: str) -> Column:
    """Trim leading/trailing whitespace; collapse inner runs for free text; upper-case
    Is_InternalCompany when case-insensitive. Used for validation and for the values
    published in silver_company (Bronze keeps the original value)."""
    c = F.regexp_replace(F.col(f"`{col_name}`"), EDGE_WHITESPACE_REGEX, "")
    if col_name in COLLAPSE_INNER_WHITESPACE:
        c = F.regexp_replace(c, INNER_WHITESPACE_REGEX, " ")
    if col_name == "Is_InternalCompany" and not INTERNAL_CASE_SENSITIVE:
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
    """Delivered (not NULL) but empty or whitespace-only."""
    return F.col(f"`{c}`").isNotNull() & (F.col(n(c)) == "")


def has_value(c: str) -> Column:
    return F.col(n(c)).isNotNull() & (F.col(n(c)) != "")


def rule(code: str, rule_name: str, failed: Column) -> Tuple[str, str, Column]:
    """One rule = (error_code, rule_name, failure condition). NULL conditions count as pass."""
    return code, rule_name, F.coalesce(failed, F.lit(False))


def format_rule(col_name: str, code: str, regex: str) -> Tuple[str, str, Column]:
    """Reusable regex rule, evaluated only when the field has a value (NULL/blank are
    reported by the mandatory rules, so a record never gets both)."""
    _, rule_name = FIELD_RULES.get(col_name, (col_name.upper(), RULE_MANDATORY_OTHER))
    return rule(code, rule_name, has_value(col_name) & ~F.col(n(col_name)).rlike(regex))


def mandatory_rules(columns: List[str]) -> List[Tuple[str, str, Column]]:
    """Rule 6 (+ NULL/blank part of R1-R4): NULL, empty or whitespace-only."""
    out = []
    for c in columns:
        prefix, rule_name = FIELD_RULES.get(c, (c.upper(), RULE_MANDATORY_OTHER))
        out.append(rule(f"{prefix}_NULL", rule_name, F.col(f"`{c}`").isNull()))
        out.append(rule(f"{prefix}_BLANK", rule_name, is_blank(c)))
    return out


def build_rules() -> List[Tuple[str, str, Column]]:
    """Rule registry. Add a rule by appending one tuple - no other code changes needed."""
    rules = mandatory_rules(MANDATORY_COLUMNS)

    # Rule 1: Company_ID characters + optional naming convention
    rules.append(format_rule("Company_ID", "COMPANY_ID_INVALID_FORMAT", COMPANY_ID_REGEX))
    if COMPANY_ID_CONVENTION_REGEX:
        rules.append(rule("COMPANY_ID_NAMING_CONVENTION", "R1_COMPANY_ID",
                          has_value("Company_ID") & F.col(n("Company_ID")).rlike(COMPANY_ID_REGEX)
                          & ~F.col(n("Company_ID")).rlike(COMPANY_ID_CONVENTION_REGEX)))

    # Rule 2: Company_Name - NULL/blank only unless an explicit format is configured
    if COMPANY_NAME_REGEX:
        rules.append(format_rule("Company_Name", "COMPANY_NAME_INVALID_FORMAT", COMPANY_NAME_REGEX))

    # Rule 3: Location_ID format
    rules.append(format_rule("Location_ID", "LOCATION_ID_INVALID_FORMAT", LOCATION_ID_REGEX))

    # Rule 4: Is_InternalCompany allowed values (Y/N; case handled in standardize())
    allowed = INTERNAL_ALLOWED_VALUES if INTERNAL_CASE_SENSITIVE else [v.upper() for v in INTERNAL_ALLOWED_VALUES]
    rules.append(rule("INTERNAL_COMPANY_INVALID_VALUE", "R4_IS_INTERNAL_COMPANY",
                      has_value("Is_InternalCompany") & ~F.col(n("Is_InternalCompany")).isin(*allowed)))

    # Rule 5: duplicates (every occurrence is flagged)
    rules.append(rule("COMPANY_ID_DUPLICATE", RULE_DUPLICATE, F.col("_company_id_occurrences") > 1))

    # Rule 7: Location reference - hard rejection only when explicitly configured
    if LOCATION_REF_MODE == "reject":
        rules.append(rule("LOCATION_ID_NOT_IN_REFERENCE", RULE_LOCATION_REF,
                          F.col("location_ref_status") == "NOT_FOUND"))

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


def location_reference() -> DataFrame:
    """Distinct approved Location_IDs (small dimension -> broadcast)."""
    ref = spark.read.table(LOCATION_REF_TABLE)
    if LOCATION_REF_MATCH == "current" and "is_current" in ref.columns:
        ref = ref.filter("is_current")
    return F.broadcast(
        ref.select(F.trim(F.col("Location_ID")).alias("_ref_location_id")).distinct()
    )


# -------------------------------------------------------------------------------------
# Private intermediate: effective Bronze records + validation results (computed once)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_company_validated",
    comment="Pipeline-private: latest version of every Bronze Company record with all DQ results.",
    private=True,
)
# Warn-only expectations: per-rule pass/fail counts per update in the pipeline event log
@dp.expect("company_id_valid", "NOT array_contains(rule_names, 'R1_COMPANY_ID')")
@dp.expect("company_name_valid", "NOT array_contains(rule_names, 'R2_COMPANY_NAME')")
@dp.expect("location_id_valid", "NOT array_contains(rule_names, 'R3_LOCATION_ID')")
@dp.expect("is_internal_company_valid", "NOT array_contains(rule_names, 'R4_IS_INTERNAL_COMPANY')")
@dp.expect("company_id_unique", "NOT array_contains(rule_names, 'R5_DUPLICATE_COMPANY_ID')")
@dp.expect("mandatory_fields_present", "NOT array_contains(rule_names, 'R6_MANDATORY_FIELD')")
@dp.expect("location_in_reference", "location_ref_status <> 'NOT_FOUND'")
@dp.expect("record_well_formed", "NOT array_contains(rule_names, 'R0_RECORD_STRUCTURE')")
def silver_company_validated():
    bronze = spark.read.table("bronze_company")

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
    effective = ensure_columns(
        effective, MANDATORY_COLUMNS + list(FIELD_RULES) + ["_rescued_data", "_corrupt_record"]
    )

    # 2. Whitespace standardisation into helper columns (raw columns untouched)
    biz = business_columns(effective)
    normalized = effective.select("*", *[standardize(c).alias(n(c)) for c in biz])

    # 8. Optional Location reference lookup (FOUND / NOT_FOUND / NOT_CHECKED)
    if REF_ENABLED:
        normalized = (
            normalized.join(location_reference(), F.col(n("Location_ID")) == F.col("_ref_location_id"), "left")
            .withColumn(
                "location_ref_status",
                F.when(~has_value("Location_ID") | ~F.col(n("Location_ID")).rlike(LOCATION_ID_REGEX),
                       F.lit("NOT_CHECKED"))
                .when(F.col("_ref_location_id").isNotNull(), F.lit("FOUND"))
                .otherwise(F.lit("NOT_FOUND")),
            )
            .drop("_ref_location_id")
        )
    else:
        normalized = normalized.withColumn("location_ref_status", F.lit("NOT_CHECKED"))

    # 7. Duplicate detection across ALL files of the scope (not per file). Every occurrence
    #    of a duplicated Company_ID is flagged - no arbitrary survivor is chosen.
    dup_keys = [n("Company_ID")] if DUPLICATE_SCOPE == "global" else ["datephoto", n("Company_ID")]
    with_dups = normalized.withColumn(
        "_company_id_occurrences",
        F.when(has_value("Company_ID"), F.count(F.lit(1)).over(Window.partitionBy(*dup_keys))),
    )

    # 3-9. Evaluate every rule and keep ALL failure reasons
    return (
        with_dups.withColumn("error_details", collect_failures(build_rules()))
        .withColumn("error_codes", F.transform("error_details", lambda x: x["error_code"]))
        .withColumn("rule_names", F.array_distinct(F.transform("error_details", lambda x: x["rule_name"])))
        .withColumn("is_valid", F.size("error_details") == 0)
    )


# -------------------------------------------------------------------------------------
# 11. silver_company: validated, standardised records (one row per Company_ID per datephoto)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_company",
    comment=(
        "Validated Company records: pass Rules R0-R7. Values whitespace-standardised. "
        "One row per Company_ID per datephoto. location_ref_status = FOUND / NOT_FOUND "
        "(warning only unless company.location_ref_mode=reject) / NOT_CHECKED."
    ),
    cluster_by=["datephoto", "Company_ID"],
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect_all_or_fail(
    {
        "silver_company_id_present": "Company_ID IS NOT NULL AND Company_ID <> ''",
        "silver_datephoto_present": "datephoto IS NOT NULL",
    }
)
def silver_company():
    v = spark.read.table("silver_company_validated").filter("is_valid")
    biz = business_columns(v)
    return v.select(
        *[F.col(n(c)).alias(c) for c in biz],
        "location_ref_status",
        "datephoto",
        "source_file",
        "ingestion_timestamp",
    )


# -------------------------------------------------------------------------------------
# 12. silver_company_errors: rejected records with ORIGINAL values and all reasons
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_company_errors",
    comment=(
        "Rejected Company records: original (raw) values, metadata and every failed rule. "
        "One row per source record; error_reason / rule_name are comma separated lists."
    ),
    cluster_by=["datephoto"],
    table_properties={"quality": "silver"},
)
def silver_company_errors():
    v = spark.read.table("silver_company_validated").filter("NOT is_valid")
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
        "location_ref_status",
        F.current_timestamp().alias("error_timestamp"),
        F.current_date().alias("run_date"),
    )
