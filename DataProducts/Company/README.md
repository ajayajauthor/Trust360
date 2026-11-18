# Trust360 – Company Data Product (Lakeflow Spark Declarative Pipeline)

| Pipeline | Id | Target |
| --- | --- | --- |
| Trust360_Company_Medallion | `7d5a7e79-a623-4529-813a-893437536bf0` | `team_2.hr_company` |
| Trust360_Company_Medallion_TEST | `a8d9a54d-bf2a-4d57-bc4b-67d902db381f` | `team_2.hr_company_test` (source `/Volumes/team_2/hr_company_test/test_data/OUT`) |

Both run on serverless, channel CURRENT, Python, and use the same three source files. The design matches the Location / Employee / Position data products.

## 1. Architecture

```
/Volumes/team_2/source/hr_data/OUT/YYYY/MM/DD/Company*.csv   (Employee/Location/Position files ignored)
        |  Auto Loader (cloudFiles, incremental, exactly-once per file version, schema evolution)
        v
BRONZE  bronze_company               Streaming table: all CSV columns as STRING, unchanged
        (01_bronze_company.py)       + datephoto, datephoto_source, source_file, source_file_modification_time,
                                       ingestion_timestamp, _rescued_data, _corrupt_record
        |  batch read (materialized views)
        v
SILVER  silver_company_validated     private MV: latest version of each file, whitespace-standardised helpers,
        (02_silver_company.py)         Location reference lookup, duplicate count, error_codes[] / rule_names[] / is_valid
          |-- silver_company          valid rows (standardised values), 1 row per Company_ID per datephoto
          '-- silver_company_errors   rejected rows, ORIGINAL values, ALL reasons, 1 row per source record
        |
        v
GOLD    gold_company_snapshot_changes private MV: hash + NEW / CHANGED / UNCHANGED per Company_ID x datephoto
        (03_gold_company.py)
          |-- gold_company            SCD Type 2: Company_ID, Company_Name, Location_ID, Is_InternalCompany,
          |                           effective_start_date, effective_end_date, is_current   (no datephoto)
          |-- gold_company_scd2_check private guard MV: 1 current row per Company_ID, no overlaps (expect_or_fail)
          '-- dq_company_summary      per-datephoto DQ + SCD metrics, refreshed on every run
```

## 2. Files

| File | Datasets | Role |
| --- | --- | --- |
| 01_bronze_company.py | bronze_company | Raw incremental ingestion |
| 02_silver_company.py | silver_company_validated (private), silver_company, silver_company_errors | Cleansing, validation, error routing |
| 03_gold_company.py | gold_company_snapshot_changes (private), gold_company, gold_company_scd2_check (private), dq_company_summary | SCD2 + monitoring |
| explorations/test_company_pipeline | (notebook, not pipeline source) | End-to-end test suite for the TEST pipeline |

## 3. Layer design

### Bronze
* Auto Loader settings: `header=true`, `sep`/`encoding` configurable, `mode=PERMISSIVE`, `columnNameOfCorruptRecord=_corrupt_record`, `cloudFiles.inferColumnTypes=false` (every value stays a STRING exactly as delivered), `schemaEvolutionMode=addNewColumns`, `pathGlobFilter=Company*.csv`.
* `datephoto` is set from, in priority order: the folder `YYYY/MM/DD`, then a file name `<name>_YYYYMMDD[_x].csv`, then the fallback `company.datephoto_fallback` (`file_modification_time` by default, or `none`, which leaves it NULL so Silver rejects it with DATEPHOTO_MISSING). Every row in a file gets the same value. `datephoto_source` records which rule was used.
* `cloudFiles.allowOverwrites=true`: if a file is re-delivered with the same path, it is ingested again as a new version. Silver uses only the latest version of each file.
* Bronze is protected with `pipelines.reset.allowed=false`, so a full refresh cannot wipe the raw history. The pipeline manages the checkpoint and schema location.
* Unquoted empty fields (`,,`) are read as NULL. Whitespace-only values are kept as delivered.

### Silver rules

| Rule name | Error codes | Condition (after trimming whitespace; Bronze is not changed) |
| --- | --- | --- |
| R1_COMPANY_ID | COMPANY_ID_NULL / _BLANK / _INVALID_FORMAT / _NAMING_CONVENTION | NULL; empty or whitespace-only; not `company.company_id_regex` (default `^[A-Za-z0-9]+$`); optional convention regex (off by default) |
| R2_COMPANY_NAME | COMPANY_NAME_NULL / _BLANK (/ _INVALID_FORMAT) | NULL / blank. Free text is allowed (punctuation, accents). Optional regex is off by default |
| R3_LOCATION_ID | LOCATION_ID_NULL / _BLANK / _INVALID_FORMAT | not `^LOC[0-9]{3,6}$` (the same format as the Location product), so LOC-246 and LOC 246 are invalid |
| R4_IS_INTERNAL_COMPANY | INTERNAL_COMPANY_NULL / _BLANK / _INVALID_VALUE | not in `Y,N`. Case-sensitive by default, so `y` is invalid. With `case_sensitive=false`, `y`/`n` are valid and published upper-case |
| R5_DUPLICATE_COMPANY_ID | COMPANY_ID_DUPLICATE | Company_ID appears more than once across ALL files with the same datephoto. Every occurrence is rejected and no survivor is picked |
| R6_MANDATORY_FIELD | `<COL>_NULL` / `<COL>_BLANK` | any extra column added to `company.mandatory_columns` |
| R7_LOCATION_REFERENCE | LOCATION_ID_NOT_IN_REFERENCE | only when `company.location_ref_mode=reject` |
| R0_RECORD_STRUCTURE | MALFORMED_RECORD, DATEPHOTO_MISSING | `_corrupt_record` / `_rescued_data` is set; datephoto could not be resolved |

* **Location reference**: the mode is configurable and defaults to **warn**. Every record gets `location_ref_status` = FOUND / NOT_FOUND / NOT_CHECKED, checked against `team_2.hr_location.gold_location` (any SCD2 version by default; set `location_ref_match=current` for current versions only). With `warn`, NOT_FOUND records stay valid and are counted in monitoring. With `reject`, they are a hard rejection. With `off`, the lookup is skipped.
* **Duplicate scope**: the default is `snapshot`, because every daily file is a full snapshot and the same Company_ID legitimately appears every day. `global` makes Company_ID unique across the whole history.
* **Error table** columns: all original columns with raw values, `_rescued_data`, `_corrupt_record`, `datephoto`, `source_file`, `source_file_modification_time`, `ingestion_timestamp`, `error_reason` (all codes, comma-separated), `rule_name` (all rules), `error_codes[]`, `rule_names[]`, `error_count`, `location_ref_status`, `error_timestamp`, `run_date`.
* Idempotency: both Silver tables are materialized views derived from Bronze. Reruns and file re-deliveries therefore never create duplicate error rows (this is tested). `error_timestamp` and `run_date` show the latest refresh that evaluated the record.

### Gold – SCD Type 2 (evaluation of approaches)

| Approach | Limitation for this use case |
| --- | --- |
| AUTO CDC (`create_auto_cdc_flow`, SCD2, `sequence_by=datephoto`) | Needs an append-only streaming source. Silver has to be an MV, because a later file can turn an earlier valid row into a duplicate, and an MV cannot be streamed. Output columns are `__START_AT/__END_AT`, with end = next start rather than day - 1. Corrections need a full refresh. |
| AUTO CDC FROM SNAPSHOT | Treats keys missing from a snapshot as deletes, which breaks Scenario 4; rejected rows would also look like deletes. Snapshots must be processed in increasing order, so late or out-of-order snapshots are not supported. |
| Delta MERGE in a foreachBatch sink | Imperative. Out-of-order snapshots require re-splicing history by hand, and idempotency and the one-current-row rule must be hand-coded. |
| **Chosen: declarative gaps-and-islands MV** | Deterministic recomputation from the Silver history. It is idempotent, handles late and out-of-order snapshots and A→B→A changes, hashes NULL-safely, and never auto-expires absent companies. Cost grows with history (companies × snapshots); serverless incremental refresh reduces this. |

Rules: hash = SHA-256 of `Company_Name, Location_ID, Is_InternalCompany` (Company_ID and technical columns excluded; NULL replaced by a sentinel). A new version starts when the hash differs from the previous *valid* snapshot. `effective_end_date` = next start - 1 day, the open end is `9999-12-31`, and `is_current` means there is no later version. A company missing from a snapshot is not expired, because no deactivation rule was provided.

## 4. Setup and deployment

1. Unity Catalog objects (already created):
   `CREATE SCHEMA IF NOT EXISTS team_2.hr_company;`
   `CREATE SCHEMA IF NOT EXISTS team_2.hr_company_test;`
   `CREATE VOLUME IF NOT EXISTS team_2.hr_company_test.test_data;`
   Pipeline tables are created by the pipeline itself. Do not create them manually.
2. Permissions for the pipeline run-as principal:
   * `USE CATALOG` on `team_2`; `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW` on `team_2.hr_company`
   * `USE SCHEMA` on `team_2.source` and `READ VOLUME` on `team_2.source.hr_data`
   * `USE SCHEMA` on `team_2.hr_location` and `SELECT` on `team_2.hr_location.gold_location` (only if location_ref_mode is not `off`)
   * Consumers: `SELECT` on `gold_company`, `silver_company`, `silver_company_errors`, `dq_company_summary`
   * No credentials are stored in code.
3. Pipeline configuration keys (`company.*`): `source_root`, `file_glob`, `csv_delimiter`, `csv_encoding`, `datephoto_folder_regex/_format`, `datephoto_filename_regex/_format`, `datephoto_fallback`, `mandatory_columns`, `company_id_regex`, `company_id_convention_regex`, `company_name_regex`, `location_id_regex`, `internal_company_allowed_values`, `internal_company_case_sensitive`, `duplicate_scope`, `location_ref_mode`, `location_ref_table`, `location_ref_match`, `reject_malformed_records`, `scd_open_end_date`, `scd_tracked_columns`.
4. Run: start an update from the pipeline editor. For production, set `development=false` and schedule the pipeline with a Lakeflow Job, either with a file-arrival trigger on the OUT folder or a daily schedule after delivery.

## 5. Monitoring

* `dq_company_summary` (one row per datephoto): bronze_records_ingested, source_files, silver_valid_records, rejected_records, invalid_company_id / company_name / location_id / is_internal_company records, duplicate_company_id_records, malformed_records, location_ref_not_found_records, location_ref_rejected_records, new / changed / unchanged companies, total_current_companies, total_historical_versions, total_gold_versions.
* Expectations, visible in the pipeline UI and event log for each update: warn-level rule expectations on `silver_company_validated`, Bronze `bronze_record_well_formed` / `bronze_datephoto_resolved`, and the fail-level guards on Silver and Gold.
* Expectation history per run:

```sql
SELECT timestamp, origin.update_id, origin.flow_name, e.name, e.passed_records, e.failed_records
FROM event_log('7d5a7e79-a623-4529-813a-893437536bf0'),
     LATERAL explode(from_json(details:flow_progress.data_quality.expectations,
       'array<struct<name string, dataset string, passed_records bigint, failed_records bigint>>')) AS t(e)
WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC;
```

## 6. Results on production data (first run, 14 snapshots 2026-11-03..16)

Bronze 7,000 rows; Silver valid 6,660; rejected 340 (136 COMPANY_NAME_NULL, 130 COMPANY_ID_DUPLICATE, 70 INTERNAL_COMPANY_INVALID_VALUE with `X`, 4 with both COMPANY_NAME_NULL and COMPANY_ID_DUPLICATE). 69 valid rows have Location `LOC9999`, which is not in the Location reference (warning only). Gold has 714 versions, 500 of them current, and no Company_ID with more than one current row. An independent SQL recomputation of the SCD2 from the raw CSVs matched Gold exactly (0 missing, 0 extra rows).

## 7. Test cases (explorations/test_company_pipeline, run against the TEST pipeline)

| # | Input | Expected (all PASS) |
| --- | --- | --- |
| B01-B03 | 2 files on 01-01, then 01-02/03, then 01-05, then late 01-04; Location.csv in the same folder | only Company*.csv ingested; new files picked up incrementally |
| B04 | `  Société  Générale  ` | raw value unchanged in Bronze |
| B05 | `OUT/2026/01/03/`, `adhoc/Company_20260106.csv`, `adhoc/Company_nodate.csv` | 2026-01-03 (FOLDER), 2026-01-06 (FILE_NAME), delivery date (FILE_MODIFICATION_TIME) |
| B06/B08 | 28 data lines in the 01-01 file | 28 Bronze rows, metadata populated |
| B07 | 01-02 file re-delivered | 2 file versions kept in Bronze |
| B09 | `T025,Name25,LOC025,Y,EXTRA` and `T026,Name26` | kept and flagged; Silver MALFORMED_RECORD |
| B10 | 01-05 file adds column Company_Type | column added automatically; NULL for older files |
| S01 | T001 / COMPANY01 / 123ABC | valid |
| S02/S03 | Company_ID NULL / `   ` | COMPANY_ID_NULL / COMPANY_ID_BLANK |
| S04 | COMP-001, COMP 001 | COMPANY_ID_INVALID_FORMAT |
| S05/S06 | DUP1 twice in one file; XD1 in Company.csv and Company_part2.csv | every occurrence COMPANY_ID_DUPLICATE |
| S07/S08 | Company_Name NULL / `   ` | COMPANY_NAME_NULL / COMPANY_NAME_BLANK |
| S09-S11 | Location NULL / `   ` / LOC-014 / LOC 015 | LOCATION_ID_NULL / _BLANK / _INVALID_FORMAT |
| S12-S15 | flag NULL / `   ` / YES / TRUE / 1 / y | INTERNAL_COMPANY_NULL / _BLANK / _INVALID_VALUE (y invalid with the case-sensitive default) |
| S16/S19 | `T022,,,X` | `COMPANY_NAME_NULL, LOCATION_ID_NULL, INTERNAL_COMPANY_INVALID_VALUE` in one row |
| S17 | `  T023  ,  Société  Générale  ` | valid; stored as `T023`, `Société Générale` |
| S18 | LOC9999 | valid, location_ref_status = NOT_FOUND (warn) |
| S21 | re-delivery + no-op rerun | one error row per source record; error table identical after rerun |
| G01 | T040 first seen 01-02 | 1 version 2026-01-02 → 9999-12-31 |
| G02 | T023 unchanged (after standardisation) | 1 version |
| G03/G10 | COMPANY01 name A→B→A→B over 01-01..01-05 | 4 versions: 01-01..01-01, 01-02..01-03, 01-04..01-04, 01-05..open |
| G04 | 123ABC LOC003 → LOC033 on 01-03 | 01-01..01-02, 01-03..open |
| G05 | T030 Y → N on 01-02 | 01-01..01-01, 01-02..open |
| G07 | T024 only in 01-01 | stays current |
| G08 | 01-02 re-delivered | no extra versions |
| G09 | 01-04 (T001 v2) arrives after 01-05 | T001: 01-01..01-03, 01-04..open (change back-dated) |
| G11-G13 | all | one current row per Company_ID; only the 7 Gold columns; contiguous intervals |
| G14 | update with no new files | Gold identical |

## 8. Assumptions requiring business confirmation

1. Company_ID format: alphanumeric only. Should the `COMP###` convention be enforced (`company.company_id_convention_regex`)?
2. Duplicate scope = per snapshot. Should one duplicate occurrence survive instead (none is selected today)?
3. Is_InternalCompany case-sensitive (`y` is rejected).
4. Location reference = warn against `team_2.hr_location.gold_location` (any version).
5. Open-end date 9999-12-31; absent companies stay current (no deactivation rule).
6. Malformed rows are rejected even when their business fields are valid.

## 9. Performance and scalability notes

* The Silver and Gold MVs use window functions over the full history (Company_ID × snapshots). At about 500 companies per day this is trivial. At millions of rows, rely on serverless incremental refresh, keep the liquid clustering on `datephoto`/`Company_ID`, and consider archiving old Bronze snapshots.
* The Location reference is distinct and broadcast. Bronze uses Auto Loader directory listing; for very large folders enable file notification mode.
