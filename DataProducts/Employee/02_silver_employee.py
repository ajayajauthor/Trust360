# =====================================================================================
# Trust360 | Employee Data Product | SILVER LAYER
# -------------------------------------------------------------------------------------
# Datasets
#   silver_employee_validated (private MV)  : every effective Bronze record + all DQ flags
#   silver_employee           (MV, UC)      : records that pass ALL validation rules
#   silver_employee_errors    (MV, UC)      : records that fail >= 1 rule, one row per
#                                             source record with ALL failure reasons
#
# WHAT  : Standardises whitespace (validation only), applies Email / Company_ID / IGG /
#         mandatory-field / duplicate-IGG / malformed-row rules and splits valid vs invalid.
# WHY   : Duplicate-IGG detection needs the complete snapshot (all files of a datephoto),
#         and a later file can retroactively turn an earlier "valid" row into a duplicate.
#         Materialized views recompute declaratively from Bronze, so both Silver tables are
#         always consistent, idempotent and free of duplicate error rows on reruns.
#         On serverless they refresh incrementally when the change set allows it.
# WHERE : Second file of the pipeline (reads bronze_employee, feeds 03_gold_employee.py).
# =====================================================================================

from typing import List, Tuple

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# -------------------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------------------
# Scope of IGG uniqueness. "snapshot" (default): an IGG must be unique across ALL files
# that share the same datephoto (each daily file is a full employee snapshot, so the same
# IGG legitimately re-appears every day). "global": unique across the whole history.
IGG_DUPLICATE_SCOPE = spark.conf.get("employee.igg_duplicate_scope", "snapshot").lower()

# Technical (non-business) columns produced by Bronze / Auto Loader.
TECHNICAL_COLUMNS = [
    "datephoto",
    "source_file",
    "source_file_modification_time",
    "ingestion_timestamp",
    "_rescued_data",
]

# Mandatory business fields (Rule 4).
COL_EMAIL, COL_COMPANY_ID, COL_IGG = "Email", "Company_ID", "IGG"

# -------------------------------------------------------------------------------------
# Reusable validation patterns (Java regex, evaluated natively by Spark - no UDFs)
# -------------------------------------------------------------------------------------
# Email: RFC-5322 "dot-atom" local part (no leading/trailing/consecutive dots), '@',
# one or more domain labels (alnum, inner hyphens, max 63 chars) and an alphabetic TLD of
# 2-63 chars (.com, .org, .fr, .co.uk ...). Max total length 254.
EMAIL_REGEX = (
    r"^[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+(\.[A-Za-z0-9!#$%&'*+/=?^_`{|}~-]+)*"
    r"@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}$"
)
EMAIL_MAX_LENGTH = 254
# Company_ID: letters and/or digits only, at least one character.
COMPANY_ID_REGEX = r"^[A-Za-z0-9]+$"
# Leading/trailing whitespace incl. NBSP, zero-width space and BOM.
EDGE_WHITESPACE_REGEX = r"^[\s\u00A0\u200B\uFEFF]+|[\s\u00A0\u200B\uFEFF]+$"


# -------------------------------------------------------------------------------------
# Reusable helper functions
# -------------------------------------------------------------------------------------
def normalize_ws(c: Column) -> Column:
    """Strip leading/trailing (incl. unicode) whitespace. Inner spaces are kept on purpose
    so that values like 'ABC 123' are still detected as invalid."""
    return F.regexp_replace(c, EDGE_WHITESPACE_REGEX, "")


def is_valid_email(c: Column) -> Column:
    """Reusable email-format check on an already-normalised column."""
    return c.rlike(EMAIL_REGEX) & (F.length(c) <= EMAIL_MAX_LENGTH)


def is_valid_company_id(c: Column) -> Column:
    return c.rlike(COMPANY_ID_REGEX)


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
    normalised helper columns _email_n / _company_id_n / _igg_n and _igg_dup_count.
    Add a new rule by appending one tuple - no other code changes required."""
    email_raw, email_n = F.col(COL_EMAIL), F.col("_email_n")
    comp_raw, comp_n = F.col(COL_COMPANY_ID), F.col("_company_id_n")
    igg_raw, igg_n = F.col(COL_IGG), F.col("_igg_n")
    return (
        # Rule 1 + 4: Email
        mandatory_rules(email_raw, email_n, "EMAIL", "Email")
        + [("EMAIL_INVALID_FORMAT", "Invalid Email", (email_n != "") & ~is_valid_email(email_n))]
        # Rule 2 + 4: Company_ID
        + mandatory_rules(comp_raw, comp_n, "COMPANY_ID", "Company_ID")
        + [("COMPANY_ID_INVALID_FORMAT", "Invalid Company_ID", (comp_n != "") & ~is_valid_company_id(comp_n))]
        # Rule 3 + 4: IGG
        + mandatory_rules(igg_raw, igg_n, "IGG", "IGG")
        + [("IGG_DUPLICATE", "Duplicate IGG", (igg_n != "") & (F.col("_igg_dup_count") > 1))]
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
    name="silver_employee_validated",
    comment="Pipeline-private: latest version of every Bronze record with all DQ flags.",
    private=True,
)
@dp.expect("email_valid", "NOT exists(error_codes, c -> c LIKE 'EMAIL%')")
@dp.expect("company_id_valid", "NOT exists(error_codes, c -> c LIKE 'COMPANY_ID%')")
@dp.expect("igg_not_null_or_blank", "NOT exists(error_codes, c -> c IN ('IGG_NULL','IGG_BLANK'))")
@dp.expect("igg_unique", "NOT array_contains(error_codes, 'IGG_DUPLICATE')")
@dp.expect("record_well_formed", "NOT array_contains(error_codes, 'MALFORMED_RECORD')")
def silver_employee_validated():
    bronze = spark.read.table("bronze_employee")

    # 1. Idempotency: if a file was re-delivered (overwritten) or re-ingested, keep only
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
        effective.withColumn("_email_n", normalize_ws(F.col(COL_EMAIL)))
        .withColumn("_company_id_n", normalize_ws(F.col(COL_COMPANY_ID)))
        .withColumn("_igg_n", normalize_ws(F.col(COL_IGG)))
    )

    # 3. Duplicate detection across ALL files of the scope (not per file). Every
    #    occurrence of a duplicated IGG is flagged - no arbitrary survivor is chosen.
    dup_keys = ["_igg_n"] if IGG_DUPLICATE_SCOPE == "global" else ["datephoto", "_igg_n"]
    with_dups = normalized.withColumn(
        "_igg_dup_count",
        F.when(F.col("_igg_n") != "", F.count(F.lit(1)).over(Window.partitionBy(*dup_keys))),
    )

    # 4. Evaluate every rule and keep ALL failure reasons.
    rules = build_rules()
    return (
        with_dups.withColumn("error_codes", collect_failures(rules, 0))
        .withColumn("rule_names", collect_failures(rules, 1))
        .withColumn("is_valid", F.size("error_codes") == 0)
    )


# -------------------------------------------------------------------------------------
# silver_employee: validated employee records
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_employee",
    comment=(
        "Validated Employee records (all DQ rules passed). One row per IGG per datephoto. "
        "Business values are whitespace-trimmed; technical lineage columns retained."
    ),
    cluster_by=["datephoto", "IGG"],
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect_all_or_fail(
    {
        "silver_email_present": "Email IS NOT NULL AND Email <> ''",
        "silver_company_id_present": "Company_ID IS NOT NULL AND Company_ID <> ''",
        "silver_igg_present": "IGG IS NOT NULL AND IGG <> ''",
    }
)
def silver_employee():
    validated = spark.read.table("silver_employee_validated").filter("is_valid")
    return validated.select(
        *[normalize_ws(F.col(c)).alias(c) for c in business_columns(validated)],
        "datephoto",
        "source_file",
        "ingestion_timestamp",
    )


# -------------------------------------------------------------------------------------
# silver_employee_errors: rejected records (one row per source record, all reasons)
# -------------------------------------------------------------------------------------
@dp.materialized_view(
    name="silver_employee_errors",
    comment=(
        "Employee records that failed >= 1 DQ rule. Original (raw) values preserved. "
        "error_reason = all failed rule codes, rule_name = readable rule names, "
        "run_date / error_timestamp = pipeline run that evaluated the rules."
    ),
    cluster_by=["datephoto"],
    table_properties={"quality": "silver", "delta.enableDeletionVectors": "true"},
)
def silver_employee_errors():
    validated = spark.read.table("silver_employee_validated").filter(~F.col("is_valid"))
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
