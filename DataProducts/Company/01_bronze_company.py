# =====================================================================================
# Trust360 | Company Data Product | BRONZE LAYER
# -------------------------------------------------------------------------------------
# Dataset   : bronze_company  (Streaming Table, Unity Catalog managed)
# Source    : /Volumes/team_2/source/hr_data/OUT/YYYY/MM/DD/Company*.csv
# Pipeline  : Lakeflow Spark Declarative Pipeline "Trust360_Company_Medallion"
#
# WHAT  : Incrementally ingests every Company CSV file with Auto Loader (cloudFiles) and
#         appends the raw rows, unchanged, plus technical metadata columns.
# WHY   : Bronze is the immutable, replayable raw history. Nothing is dropped, renamed,
#         trimmed, cast or validated here, so Silver/Gold can always be rebuilt from Bronze.
# WHERE : First file of the pipeline (upstream of 02_silver_company.py).
#
# Design notes
#   * Auto Loader tracks processed files in the pipeline-managed checkpoint (exactly once
#     per file version); new files anywhere below the source root are discovered
#     automatically on every update. Only files whose NAME matches company.file_glob are
#     read, so Employee/Location/Position files in the same folders are ignored.
#   * Every CSV column is read as STRING (cloudFiles.inferColumnTypes=false): values are
#     kept exactly as delivered (no implicit casting, leading zeros preserved).
#   * Unquoted empty fields (",,") are read as NULL (Spark CSV default); whitespace-only
#     values are preserved as delivered (no trimming in Bronze).
#   * Schema evolution: new CSV columns are added automatically (addNewColumns; the
#     pipeline restarts the flow once). Malformed rows are never dropped:
#       - _rescued_data   : values that do not fit the schema (extra / mismatching fields)
#       - _corrupt_record : the full raw line when the row has a wrong number of fields
#   * datephoto is derived from the file location / name, never from the processing date,
#     so every row of a file gets the same snapshot date.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F

# -------------------------------------------------------------------------------------
# Configuration (pipeline settings -> Configuration). No credentials are needed: access is
# governed by Unity Catalog READ VOLUME on the source volume.
# -------------------------------------------------------------------------------------
SOURCE_ROOT = spark.conf.get("company.source_root", "/Volumes/team_2/source/hr_data/OUT")
FILE_NAME_GLOB = spark.conf.get("company.file_glob", "Company*.csv")
CSV_DELIMITER = spark.conf.get("company.csv_delimiter", ",")
CSV_ENCODING = spark.conf.get("company.csv_encoding", "UTF-8")

# datephoto naming convention (agreed: OUT/YYYY/MM/DD/<file>.csv) + configurable fallbacks
DATEPHOTO_FOLDER_REGEX = spark.conf.get("company.datephoto_folder_regex", r"/(\d{4}/\d{2}/\d{2})/[^/]+$")
DATEPHOTO_FOLDER_FORMAT = spark.conf.get("company.datephoto_folder_format", "yyyy/MM/dd")
DATEPHOTO_FILENAME_REGEX = spark.conf.get("company.datephoto_filename_regex", r"_(\d{8})(?:_[^/]*)?\.csv$")
DATEPHOTO_FILENAME_FORMAT = spark.conf.get("company.datephoto_filename_format", "yyyyMMdd")
# "file_modification_time" (default): use the file delivery date when neither convention
# matches. "none": leave datephoto NULL -> the row is kept in Bronze and rejected in Silver.
DATEPHOTO_FALLBACK = spark.conf.get("company.datephoto_fallback", "file_modification_time").lower()

CORRUPT_RECORD_COLUMN = "_corrupt_record"


# -------------------------------------------------------------------------------------
# Reusable helpers (native Spark expressions, no UDFs)
# -------------------------------------------------------------------------------------
def _try_to_date(value: Column, fmt: str) -> Column:
    """to_date that yields NULL (instead of failing in ANSI mode) for '' or invalid dates."""
    return F.when(value != "", F.call_function("try_to_date", value, F.lit(fmt)))


def _from_folder(file_path: Column) -> Column:
    return _try_to_date(F.regexp_extract(file_path, DATEPHOTO_FOLDER_REGEX, 1), DATEPHOTO_FOLDER_FORMAT)


def _from_file_name(file_path: Column) -> Column:
    return _try_to_date(F.regexp_extract(file_path, DATEPHOTO_FILENAME_REGEX, 1), DATEPHOTO_FILENAME_FORMAT)


def derive_datephoto(file_path: Column, file_modification_time: Column) -> Column:
    """Snapshot date of a file, by priority:
    1. Folder convention     .../YYYY/MM/DD/<file>.csv      (agreed OUT layout)
    2. File-name convention  <name>_YYYYMMDD[_xxx].csv
    3. Fallback              file delivery date (_metadata.file_modification_time) or NULL
    """
    fallback = (
        F.to_date(file_modification_time)
        if DATEPHOTO_FALLBACK == "file_modification_time"
        else F.lit(None).cast("date")
    )
    return F.coalesce(_from_folder(file_path), _from_file_name(file_path), fallback)


def datephoto_source(file_path: Column) -> Column:
    """Audit column: which rule produced datephoto."""
    return (
        F.when(_from_folder(file_path).isNotNull(), F.lit("FOLDER"))
        .when(_from_file_name(file_path).isNotNull(), F.lit("FILE_NAME"))
        .when(F.lit(DATEPHOTO_FALLBACK == "file_modification_time"), F.lit("FILE_MODIFICATION_TIME"))
        .otherwise(F.lit("UNRESOLVED"))
    )


# -------------------------------------------------------------------------------------
# bronze_company
# -------------------------------------------------------------------------------------
@dp.table(
    name="bronze_company",
    comment=(
        "Raw Company CSV records ingested incrementally with Auto Loader from "
        f"{SOURCE_ROOT} ({FILE_NAME_GLOB}). All source columns preserved as delivered (STRING, "
        "original header names); metadata: datephoto, datephoto_source, source_file, "
        "source_file_modification_time, ingestion_timestamp, _rescued_data, _corrupt_record."
    ),
    cluster_by=["datephoto"],
    table_properties={
        "quality": "bronze",
        # allow future headers with spaces/special characters to be preserved unchanged
        "delta.columnMapping.mode": "name",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
        # Bronze is the raw history: protect it from accidental full refreshes
        "pipelines.reset.allowed": "false",
    },
)
# Monitoring only (warn): Bronze never drops rows
@dp.expect("bronze_datephoto_resolved", "datephoto IS NOT NULL")
@dp.expect("bronze_record_well_formed", "_rescued_data IS NULL AND _corrupt_record IS NULL")
def bronze_company():
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        # --- CSV parsing -----------------------------------------------------------
        .option("header", "true")
        .option("sep", CSV_DELIMITER)
        .option("encoding", CSV_ENCODING)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", "false")
        .option("ignoreLeadingWhiteSpace", "false")   # keep values exactly as delivered
        .option("ignoreTrailingWhiteSpace", "false")
        .option("mode", "PERMISSIVE")                # never drop malformed rows
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        # --- Schema management -----------------------------------------------------
        .option("cloudFiles.inferColumnTypes", "false")  # every value kept as STRING
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaHints", f"{CORRUPT_RECORD_COLUMN} STRING")
        # --- File discovery --------------------------------------------------------
        .option("pathGlobFilter", FILE_NAME_GLOB)
        .option("cloudFiles.includeExistingFiles", "true")
        # A re-delivered (overwritten) file is re-ingested as a new version; Silver keeps
        # only the latest version of each file, so nothing is double counted.
        .option("cloudFiles.allowOverwrites", "true")
        .load(SOURCE_ROOT)
    )

    file_path = F.col("_metadata.file_path")
    file_mtime = F.col("_metadata.file_modification_time")
    return (
        raw.withColumn("datephoto", derive_datephoto(file_path, file_mtime))
        .withColumn("datephoto_source", datephoto_source(file_path))
        .withColumn("source_file", file_path)
        .withColumn("source_file_modification_time", file_mtime)
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
