# Presentation & Verification Guide — Kumparan ETL with Data Cleansing

## 1. Explain the problem
Kumparan needs article data from its operational database prepared for Business Intelligence (BI). The assessment covers hourly ELT, historical articles since 2016, incremental updates, soft/hard deletes, data cleansing, and quality monitoring. All article records used in the assessment are synthetic.

## 2. Architecture

```text
Kumparan_Source.dbo.articles
       ↓
Apache Airflow (hourly; 0 * * * *)
       ↓
Kumparan_DWH.stg.articles_raw        (raw extraction; unchanged)
       ↓
clean_and_validate_articles          (SQL cleansing and validation)
       ↓
Kumparan_DWH.stg.articles_clean      (cleaned data per run_id)
       ↓
Kumparan_DWH.core.articles           (current state)
       └── core.article_change_history (change tracking)
       ↓
Kumparan_DWH.mart.*                  (dimensions, fact, aggregates)
```

Operational monitoring uses `etl.pipeline_runs`, `etl.watermarks`, and `etl.data_quality_results`. PostgreSQL is used only for Airflow metadata, not article storage.

## 3. Demonstrate the nine Airflow tasks

Open `http://localhost:8080`, find DAG `kumparan_articles_hourly`, and show the hourly schedule `0 * * * *`.

```text
start_pipeline
      ↓
extract_source
      ↓
load_staging
      ↓
clean_and_validate_articles
      ↓
sync_core_and_history
      ↓
detect_hard_delete
      ↓
refresh_data_mart
      ↓
run_data_quality
      ↓
finalize_pipeline
```

Explain that a green task indicates successful execution, not proof that all edge cases have been tested. Point out the cleansing task between staging and core.

## 4. Explain article cleansing

The implementation uses SQL Server queries executed from the Airflow Python DAG. It preserves source data and `stg.articles_raw`, and writes cleaned rows into `stg.articles_clean` for the current `run_id`.

| Rule | Behavior | Severity |
|---|---|---|
| Title whitespace | Trim leading/trailing whitespace; normalize CR/LF/TAB/NBSP to spaces | Transformation |
| Content whitespace | Trim leading/trailing whitespace; preserve paragraph line breaks | Transformation |
| Row count | Cleaned row count must match staged row count | Critical |
| Empty title | Reject individual article to articles_rejected (EMPTY_TITLE) | Record-level rejection |
| Invalid timestamp | Reject if `updated_at < created_at` | Critical |
| Empty content | Record quality warning | Warning |
| Publication date before creation | Record quality warning | Warning |
| Deletion date before creation | Record quality warning | Warning |

Critical failures stop processing before core synchronization; warning checks are recorded without necessarily failing the DAG. The pipeline does **not** lowercase articles, remove stopwords, strip HTML, or fabricate missing content.

## 5. Demonstrate in SQL Server (DBeaver)

Run the read-only sections of `script_sql/04_etl_validation.sql` for source/core/mart and ETL monitoring. Then run `script_sql/04_etl_validation.sql` to inspect staging-versus-clean counts, cleaned records, and validation results. Avoid treating rows from different `run_id` values as one batch.

Useful additional checks:

```sql
SELECT TOP (20) run_id, id, title, content, updated_at
FROM Kumparan_DWH.stg.articles_clean
ORDER BY run_id DESC, id;

SELECT TOP (30) run_id, check_name, passed,
       observed_value, expected_value, checked_at
FROM Kumparan_DWH.etl.data_quality_results
WHERE check_name LIKE 'cleansing_%'
ORDER BY checked_at DESC;

SELECT TOP (20) *
FROM Kumparan_DWH.etl.pipeline_runs
ORDER BY run_id DESC;
```

A warning row may have `passed=0` while the overall DAG succeeds. Explain the difference between critical and informational checks.

## 6. Demonstrate cleansing with controlled test data

In an isolated/disposable test database, insert an article with a title containing leading/trailing spaces, a tab or newline, and valid timestamps. Trigger the DAG, then compare `stg.articles_raw` against `stg.articles_clean` and `core.articles`. Next test an empty title and confirm that the record is stored in articles_rejected with reason EMPTY_TITLE; the run can still succeed if critical DQ passes. Restore the test data before continuing.

For an incremental UPDATE, update the article's `updated_at`, trigger the DAG again, and verify the cleaned core value and change history. Trigger once more without changes to check idempotency. Do not modify the existing assessment dataset just to force a demonstration.

## 7. Explain the 2016 historical-data bonus

`script_script_sql/02_insert_data_articles.sql` generates synthetic historical source data for this assessment only. In a real system, the initial backfill reads existing historical source records. Manual bootstrap and Airflow initial-load execution are different: do not present manual bootstrap as an Airflow run.

## 8. Explain soft and hard deletes

`deleted_at` represents a soft delete. An ID missing from the source represents a hard delete. Core retains a tombstone (`is_hard_deleted=1`) and change history; the active publication mart excludes deleted articles. This is logical synchronization, not a physical purge of all warehouse records.

## 9. Explain limitations candidly

- The mart is rebuilt each run, suitable for a small assessment but costly at scale.
- Source extraction and hard-delete detection are not guaranteed to share one consistent snapshot under concurrent writes.
- The five-minute overlap may re-extract rows; incremental synchronization must remain idempotent.
- Text cleansing is intentionally conservative and does not perform full HTML sanitization or NLP.
- Validation warnings are retained in `etl.data_quality_results`; they do not necessarily fail a DAG.
- The 12,000-row synthetic example is not a production benchmark.
- A successful Airflow run is not equivalent to comprehensive test coverage.

## 10. AI disclosure

Disclose AI assistance in brainstorming, code drafting, synthetic-data preparation, and documentation. The submitter remains responsible for running tests, interpreting results, and explaining design choices. Refer to the project's AI disclosure document if included in the submitted package.

## 11. Safe submission checklist

- [ ] `.env` and real secrets are excluded; only `.env.example` is shared when available.
- [ ] `script_sql/01_database_setup.sql` has been applied to the intended test database.
- [ ] The nine-task Airflow DAG imports without errors and a new run succeeds.
- [ ] `etl/sql/cleansing_articles.sql` is present and accessible to the Airflow task.
- [ ] `stg.articles_raw` remains unchanged and `stg.articles_clean` is populated per run.
- [ ] Critical cleansing checks and non-critical warnings are visible in `etl.data_quality_results`.
- [ ] `script_sql/04_etl_validation.sql` and the presentation validation queries have been executed.
- [ ] INSERT, UPDATE, soft delete, restore, hard delete, and unchanged rerun are tested on disposable data.
- [ ] Source/core/mart counts and publication filters reconcile under the documented rules.
- [ ] README distinguishes synthetic seed, manual bootstrap, and actual Airflow runs.
- [ ] AI disclosure and known limitations are included.

**Verification status:** This guide reflects the project code inspected; it does not assert that the SQL Server/Airflow end-to-end tests have already passed.

## Visualisasi flow

Lihat `images/etl_architecture.svg`, `images/airflow_task_flow.svg`, dan `docs/ETL_FLOW.md`.


## Koreksi implementasi terbaru (8 November 2026)

Gunakan [`ETL_FLOW.md`](ETL_FLOW.md) sebagai acuan urutan sembilan task dan status implementasi. SQL setup sudah menyertakan `stg.articles_clean`, `stg.articles_rejected`, dan `etl.recovery_state`. Artikel invalid dipisahkan ke REJECTED dan tidak otomatis menggagalkan DAG; kegagalan kritis terjadi pada ketidaksesuaian RAW vs CLEAN+REJECTED atau DQ lain. Query validasi ada di `script_sql/04_etl_validation.sql`. `script_sql/05_seed_dirty_articles.sql` hanya untuk menambahkan data sintetis testing pada rentang ID 12001–12200; jangan digunakan pada data produksi. Hasil uji runtime belum diverifikasi hanya dari berkas kode.
