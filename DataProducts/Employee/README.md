# Trust360 – Employee Data Product (Lakeflow Spark Declarative Pipeline)

Pipeline: **Trust360_Employee_Medallion** (id `a0c59900-8aef-4d38-95ee-e8f73c8c0dec`), serverless, Python, publishes to `team_2.hr_employee`.

## 1. Architecture

```
/Volumes/team_2/source/hr_data/OUT/YYYY/MM/DD/Employee*.csv
        |  Auto Loader (cloudFiles, incremental, exactly-once per file version)
        v
BRONZE  bronze_employee            Streaming table: all CSV columns as STRING (unchanged)
        (01_bronze_employee.py)    + datephoto, source_file, source_file_modification_time,
                                     ingestion_timestamp, _rescued_data
        |  batch read (MV, recomputed / incrementally refreshed)
        v
SILVER  silver_employee_validated  private MV: latest file version + whitespace-normalised keys
        (02_silver_employee.py)      + error_codes[] / rule_names[] / is_valid
          |-- silver_employee         valid rows only (trimmed values, one row per IGG per datephoto)
          '-- silver_employee_errors  rejected rows, raw values, ALL reasons, one row per source record
        |
        v
GOLD    gold_employee              MV: SCD Type 2 on IGG (effective_start_date, effective_end_date,
        (03_gold_employee.py)        is_current, version_number, record_hash, employee_sk) – no datephoto
          |-- gold_employee_scd2_check   private MV guard: 1 current row per IGG, no overlaps (expect_or_fail)
          '-- dq_employee_summary        per-datephoto DQ + SCD metrics, refreshed every run
```

## 2. Files (place all three in the pipeline libraries, in this order)

| File | Datasets | Role |
| --- | --- | --- |
| 01_bronze_employee.py | bronze_employee | Raw incremental ingestion |
| 02_silver_employee.py | silver_employee_validated (private), silver_employee, silver_employee_errors | Cleansing, validation, error routing |
| 03_gold_employee.py | gold_employee, gold_employee_scd2_check (private), dq_employee_summary | SCD2 + monitoring |

## 3. Layer design

### Bronze
* Auto Loader, `header=true`, `sep=,`, `encoding=UTF-8`, `mode=PERMISSIVE`, `cloudFiles.inferColumnTypes=false` (every value kept as the delivered string), `schemaEvolutionMode=addNewColumns`, `_rescued_data` for malformed / non-conforming values (rows are never dropped).
* `datephoto` priority: folder `YYYY/MM/DD` -> file name `<Product>_YYYY-MM-DD_<SSS>.csv` -> file modification date. Same value for every row of a file.
* `cloudFiles.allowOverwrites=true`: a re-delivered file with the same path is re-ingested as a new version; Silver keeps only the latest version, so nothing is double counted.
* Checkpoint and schema location are managed by the pipeline (not configured in code).

### Silver rules (error codes -> rule names)

| Code | Rule name | Condition (after trimming leading/trailing whitespace) |
| --- | --- | --- |
| EMAIL_NULL / EMAIL_BLANK | Null Email / Blank Email | NULL / whitespace-only |
| EMAIL_INVALID_FORMAT | Invalid Email | not RFC-style dot-atom local part @ labelled domain + alphabetic TLD (2-63), length <= 254 |
| COMPANY_ID_NULL / _BLANK | Null / Blank Company_ID | NULL / whitespace-only |
| COMPANY_ID_INVALID_FORMAT | Invalid Company_ID | not `^[A-Za-z0-9]+$` |
| IGG_NULL / IGG_BLANK | Null / Blank IGG | NULL / whitespace-only |
| IGG_DUPLICATE | Duplicate IGG | IGG occurs more than once across ALL files of the same datephoto; all occurrences rejected |
| MALFORMED_RECORD | Malformed CSV record | `_rescued_data` is not NULL |

Error table columns: all original columns (raw values), `_rescued_data`, `datephoto`, `source_file`, `ingestion_timestamp`, `error_reason` (all codes, comma separated), `rule_name` (all names), `error_codes` (array), `error_count`, `run_date`, `error_timestamp`.
Because both Silver tables are materialized views derived from Bronze, re-processing a file or rerunning the pipeline never creates duplicate error rows.

### Gold – SCD Type 2 (approach evaluation)

| Approach | Limitation for this use case |
| --- | --- |
| AUTO CDC (`create_auto_cdc_flow`, SCD2, `sequence_by=datephoto`) | Needs an append-only streaming source; Silver must be an MV (duplicates across files can invalidate earlier rows retroactively), and an MV cannot be streamed. Columns are `__START_AT/__END_AT` (end = next start, not day-1). Corrections need full refresh. |
| AUTO CDC FROM SNAPSHOT | Keys missing from a snapshot are treated as deletes (breaks Scenario 4). Snapshot versions must be processed in increasing order, so late / out-of-order snapshots are not supported. |
| Delta MERGE in foreachBatch sink | Imperative; out-of-order snapshots require re-splicing history; idempotency and "one current row" must be hand-coded. |
| **Chosen: declarative gaps-and-islands MV** | Deterministic recomputation from Silver history: idempotent, handles late / out-of-order snapshots, A->B->A, NULL-safe hashing, no auto-expiry of absent employees. Cost grows with history (employees x snapshots); serverless incremental refresh reduces it. |

Rules: hash = SHA-256 of all business columns except IGG (NULL sentinel), new version when hash differs from the previous valid snapshot, `effective_end_date = next start - 1 day`, open end `9999-12-31`, `is_current = no later version`.

## 4. Setup and deployment

1. Schema: `CREATE SCHEMA IF NOT EXISTS team_2.hr_employee;` (done).
2. Permissions for the pipeline owner / run-as principal:
   * `USE CATALOG` on `team_2`; `USE SCHEMA`, `CREATE TABLE`, `CREATE MATERIALIZED VIEW` on `team_2.hr_employee`.
   * `USE SCHEMA` on `team_2.source` and `READ VOLUME` on `team_2.source.hr_data`.
   * Consumers: `SELECT` on `gold_employee`, `silver_employee`, `silver_employee_errors`, `dq_employee_summary`.
3. Pipeline settings: serverless, channel CURRENT, catalog `team_2`, schema `hr_employee`, libraries = the three .py files. Configuration keys: `employee.source_root`, `employee.file_glob`, `employee.csv_delimiter`, `employee.csv_encoding`, `employee.igg_duplicate_scope` (`snapshot`|`global`), `employee.scd_open_end_date`, `employee.scd_tracked_columns` (optional).
4. Run: start an update from the pipeline editor. For production, set `development=false` and schedule it with a Lakeflow Job using a file-arrival trigger on the OUT folder (or a daily schedule after the IN->OUT copy job).

## 5. Monitoring

* `dq_employee_summary`: per datephoto – bronze_records_ingested, silver_valid_records, rejected_records, invalid_email_records, invalid_company_id_records, null_blank_igg_records, duplicate_igg_records, malformed_records, new_employees, changed_employees, unchanged_employees, plus total_current_employees, total_historical_versions.
* Expectations (pipeline UI / event log): `email_valid`, `company_id_valid`, `igg_not_null_or_blank`, `igg_unique`, `record_well_formed` (warn); Silver/Gold `expect_all_or_fail` guards; `one_current_version_per_igg`, `no_overlapping_versions`.
* Per-run history of expectation metrics:

```sql
SELECT timestamp, origin.update_id, origin.flow_name,
       e.name, e.passed_records, e.failed_records
FROM event_log('a0c59900-8aef-4d38-95ee-e8f73c8c0dec'),
     LATERAL explode(from_json(details:flow_progress.data_quality.expectations,
       'array<struct<name string, dataset string, passed_records bigint, failed_records bigint>>')) AS t(e)
WHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC;
```

## 6. Test cases (executed against the pipeline code with synthetic data on 2026-11-18)

| # | Input (IGG, Email, Company_ID) | Expected / actual |
| --- | --- | --- |
| S1 | T01, john@gmail.com, ABC123 | silver_employee |
| S2 | T02, jane@yahoo.com, 123ABC | silver_employee |
| S3 | T03, NULL, COMPANY01 | EMAIL_NULL |
| S4 | T04, '   ', ABC123 | EMAIL_BLANK |
| S5 | john.gmail.com / john@ / @gmail.com / john@gmail | EMAIL_INVALID_FORMAT |
| S6 | ABC-123 / ABC 123 / ABC@123 | COMPANY_ID_INVALID_FORMAT |
| S7 | Company_ID NULL | COMPANY_ID_NULL |
| S8 | IGG NULL / '  ' | IGG_NULL / IGG_BLANK |
| S9 | T15 twice in the same file | both rows IGG_DUPLICATE |
| S10 | T16 in Employee.csv and Employee_part2.csv of the same date | both rows IGG_DUPLICATE |
| S11 | T17 ('bad', ' ABC-1') + T17 (NULL email) | 'EMAIL_INVALID_FORMAT, COMPANY_ID_INVALID_FORMAT, IGG_DUPLICATE' and 'EMAIL_NULL, IGG_DUPLICATE' (one row each) |
| S12 | '  T18 ', ' t18@corp.co.uk ', ' C18 ' | valid, stored trimmed |
| S13 | row with _rescued_data | MALFORMED_RECORD |
| G1 | G1 identical on 11-03/04/05 | 1 version, 2026-11-03 -> 9999-12-31, current |
| G2 | G2 Location L1 -> L2 -> L1 | 3 versions: 11-03..11-03, 11-04..11-04, 11-05..9999-12-31 |
| G3 | G3 only in 11-03 | stays current (not expired) |
| G4 | G4 first seen 11-05 | 1 version from 2026-11-05 |
| G5 | Location NULL -> L9 | NULL-safe change detected, 2 versions |
| G6 | 11-05 file delivered twice (older + newer version) | only latest version used; 4 rows for 11-05 in Silver, no duplicates |
| G7 | gold_employee columns | no datephoto / source_file; scd2 check returned 0 violations |

Not executed as a separate test: a late-arriving older snapshot. The design handles it because Gold is recomputed from the full Silver history ordered by datephoto. To test it, drop an extra file such as `OUT/2026/11/02/Employee.csv` and rerun.

## 7. Assumptions requiring business confirmation

1. IGG uniqueness is applied **per snapshot (datephoto)**, across all files of that date. Each daily file is a full snapshot, so the same IGG legitimately appears every day. Set `employee.igg_duplicate_scope=global` for whole-history uniqueness (which would reject almost every record with the current data).
2. Empty CSV fields are read as NULL (Spark CSV default), so an empty Email gives EMAIL_NULL; whitespace-only gives EMAIL_BLANK.
3. Email TLD must be alphabetic with at least 2 characters; IGG and Company_ID comparisons are case-sensitive.
4. Tracked SCD attributes = all business columns except IGG (override with `employee.scd_tracked_columns`). There is no termination rule, so absent employees remain current.
5. A record that is rejected in a snapshot is treated as absent from that snapshot for SCD purposes.
6. `error_timestamp` / `run_date` reflect the latest pipeline run that evaluated the rule (the error MV is recomputed on each run).

## 8. Performance notes

* Silver duplicate detection and Gold SCD use window functions over the full history. These are fine at the current volume (500 rows/day). For millions of rows/day, consider moving them to an incremental per-datephoto design.
* `silver_employee_errors` uses `current_timestamp()`, so it is fully recomputed each run (small table).
* Liquid clustering: Bronze `datephoto`; Silver `datephoto, IGG`; Gold `IGG, effective_end_date`.
* Auto Loader on Volumes uses optimized listing. If the OUT folder grows large, enable `cloudFiles.cleanSource` or file events.
