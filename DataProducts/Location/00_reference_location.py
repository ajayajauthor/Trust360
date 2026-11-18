# =====================================================================================
# Trust360 | Location Data Product | REFERENCE DATA
# -------------------------------------------------------------------------------------
# Dataset   : ref_location_country  (Materialized View, Unity Catalog managed)
#
# WHAT  : Country reference used by Silver Rule 3 (Country_ID), the optional Rule 4
#         consistency check (Country_Title) and Rule 6 (country-specific Postal_Code).
# WHY   : Country / postal rules must be maintainable without code changes. Two modes:
#           * location.country_reference_table = <catalog.schema.table>  (recommended for
#             production: a governed, business-owned UC table with the columns
#             country_code, country_name, postal_code_regex, is_active)
#           * empty (default): the seed list below is used.
# WHERE : First file of the pipeline; read by 02_silver_location.py.
#
# ASSUMPTIONS (business confirmation required)
#   * Country_ID = ISO 3166-1 alpha-2. Seeded with the 6 countries present in the source.
#   * postal_code_regex reflects the convention observed in the source extract (5 or 6
#     digits for every country, e.g. AU 71105), NOT the official national formats
#     (AU is officially 4 digits). Replace with official formats once confirmed.
# =====================================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

COUNTRY_REFERENCE_TABLE = spark.conf.get("location.country_reference_table", "").strip()

# (country_code, country_name, postal_code_regex, is_active)
SOURCE_POSTAL_CONVENTION = r"^[0-9]{5,6}$"
COUNTRY_SEED = [
    ("AU", "Australia", SOURCE_POSTAL_CONVENTION, True),
    ("DE", "Germany", SOURCE_POSTAL_CONVENTION, True),
    ("FR", "France", SOURCE_POSTAL_CONVENTION, True),
    ("IN", "India", SOURCE_POSTAL_CONVENTION, True),
    ("SG", "Singapore", SOURCE_POSTAL_CONVENTION, True),
    ("US", "United States", SOURCE_POSTAL_CONVENTION, True),
]
REFERENCE_SCHEMA = "country_code STRING, country_name STRING, postal_code_regex STRING, is_active BOOLEAN"


@dp.materialized_view(
    name="ref_location_country",
    comment=(
        "Country reference for Location validation (ISO 3166-1 alpha-2 code, name, "
        "postal-code regex, active flag). Source: location.country_reference_table or seed."
    ),
    table_properties={"quality": "reference"},
)
@dp.expect_all_or_fail(
    {
        "ref_country_code_present": "country_code IS NOT NULL",
        "ref_country_code_alpha2": "country_code RLIKE '^[A-Z]{2}$'",
    }
)
def ref_location_country():
    if COUNTRY_REFERENCE_TABLE:
        ref = spark.read.table(COUNTRY_REFERENCE_TABLE)
    else:
        ref = spark.createDataFrame(COUNTRY_SEED, REFERENCE_SCHEMA)
    return ref.select(
        F.upper(F.trim("country_code")).alias("country_code"),
        F.trim("country_name").alias("country_name"),
        F.col("postal_code_regex").cast("string").alias("postal_code_regex"),
        F.coalesce(F.col("is_active").cast("boolean"), F.lit(True)).alias("is_active"),
    )
