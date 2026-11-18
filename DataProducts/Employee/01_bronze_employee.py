# =====================================================================================
# Trust360 | Employee Data Product | BRONZE LAYER
# -------------------------------------------------------------------------------------
# Dataset   : bronze_employee  (Streaming Table, Unity Catalog managed)
# Source    : /Volumes/team_2/source/hr_data/OUT/YYYY/MM/DD/Employee.csv
# Pipeline  : Lakeflow Spark Declarative Pipeline "Trust360_Employee_Medallion"
#
# WHAT  : Incrementally ingests every Employee CSV file with Auto Loader (cloudFiles) and
#         appends the raw rows, unchanged, plus technical metadata columns.
# WHY   : Bronze is the immutable, replayable raw history. Nothing is dropped, renamed
#         or cast here, so Silver/Gold can always be rebuilt from Bronze.
# WHERE : First file of the pipeline (upstream of 02_silver_employee.py).
#
# Design notes
#   * Auto Loader tracks processed files in the pipeline-managed checkpoint, so each file
#     is ingested exactly once; new files are discovered automatically on every update.
#   * All CSV columns are read as STRING (cloudFiles.inferColumnTypes = false) so values
#     are preserved exactly as delivered (e.g. leading zeros in IDs are not lost).
#   * Schema evolution: new CSV columns are added automatically (addNewColumns). Values
#     that do not fit the schema / malformed fragments land in _rescued_data instead of
#     being dropped (PERMISSIVE mode).
#   * datephoto is derived from the file location (OUT/YYYY/MM/DD/<Product>.csv), never
#     from the processing date, so every row of a file gets the same snapshot date.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import Column
from pyspark.sql import functions as F

# -------------------------------------------------------------------------------------
# Configuration (override in pipeline settings -> Configuration; no hardcoded secrets)
# -------------------------------------------------------------------------------------
SOURCE_ROOT = spark.conf.get("employee.source_root", "/Volumes/team_2/source/hr_data/OUT")
FILE_NAME_GLOB = spark.conf.get("employee.file_glob", "Employee*.csv")
CSV_DELIMITER = spark.conf.get("employee.csv_delimiter", ",")
CSV_ENCODING = spark.conf.get("employee.csv_encoding", "UTF-8")

# Folder convention agreed for OUT: <root>/YYYY/MM/DD/<Product>.csv
SOURCE_PATH = f"{SOURCE_ROOT.rstrip('/')}/*/*/*/{FILE_NAME_GLOB}"


def _safe_to_date(value: Column, fmt: str) -> Column:
    """to_date that returns NULL (instead of failing in ANSI mode) when nothing matched."""
    return F.when(value != "", F.to_date(value, fmt))


def derive_datephoto(file_path: Column, file_modification_time: Column) -> Column:
    """Snapshot date of the file, by priority:
    1. Folder convention  .../YYYY/MM/DD/<file>.csv            (current OUT layout)
    2. File-name convention <Product>_YYYY-MM-DD_<SSS>.csv      (legacy IN layout)
    3. File delivery date  (_metadata.file_modification_time)   (last-resort fallback)
    """
    from_folder = F.regexp_extract(file_path, r"/(\d{4}/\d{2}/\d{2})/[^/]+$", 1)
    from_name = F.regexp_extract(file_path, r"_(\d{4}-\d{2}-\d{2})(?:_[^/]*)?\.csv$", 1)
    return F.coalesce(
        _safe_to_date(from_folder, "yyyy/MM/dd"),
        _safe_to_date(from_name, "yyyy-MM-dd"),
        F.to_date(file_modification_time),
    )


@dp.table(
    name="bronze_employee",
    comment=(
        "Raw Employee CSV records ingested incrementally with Auto Loader from "
        "/Volumes/team_2/source/hr_data/OUT. All source columns preserved as STRING; "
        "metadata: datephoto, source_file, source_file_modification_time, ingestion_timestamp."
    ),
    cluster_by=["datephoto"],
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.enableRowTracking": "true",
        "delta.enableDeletionVectors": "true",
    },
)
@dp.expect("bronze_datephoto_resolved", "datephoto IS NOT NULL")
@dp.expect("bronze_record_not_malformed", "_rescued_data IS NULL")
def bronze_employee():
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
        .option("mode", "PERMISSIVE")  # never drop malformed rows -> _rescued_data
        # --- Schema management -----------------------------------------------------
        .option("cloudFiles.inferColumnTypes", "false")  # keep raw STRING values
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # --- File discovery --------------------------------------------------------
        # A re-delivered (overwritten) file is a new version of the snapshot and is
        # re-ingested; Silver keeps only the latest version of each file.
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(SOURCE_PATH)
    )

    return (
        raw.withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("source_file_modification_time", F.col("_metadata.file_modification_time"))
        .withColumn(
            "datephoto",
            derive_datephoto(F.col("_metadata.file_path"), F.col("_metadata.file_modification_time")),
        )
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
