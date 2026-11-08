"""Kumparan hourly ELT: nine observable Airflow tasks, SQL Server DDL v1.

Existing Kumparan_Source / Kumparan_DWH data is preserved. No CREATE/DROP.
Requires Airflow 2.10.x, pymssql, and MSSQL_HOST, MSSQL_PORT,
MSSQL_USER, MSSQL_PASSWORD, MSSQL_SOURCE_DB, MSSQL_DWH_DB.
Both databases must be on the same SQL Server instance.
"""

import os
import re
import uuid
from datetime import datetime, timedelta

import pymssql
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

PIPELINE = "kumparan_articles_hourly"
BATCH_SIZE = 1000
OVERLAP_MINUTES = 5
RECOVERY_SCAN_SIZE = int(os.getenv("RECOVERY_SCAN_SIZE", "1000"))  # Jumlah ID Source yang diperiksa per jam


def source_table():
    """Validate SQL identifier before using it in cross-database queries."""
    db = os.environ.get("MSSQL_SOURCE_DB", "Kumparan_Source")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", db):
        raise ValueError("MSSQL_SOURCE_DB must be a simple SQL identifier")
    return f"[{db}].dbo.articles"


def dwh_table():
    """Nama Core lintas database untuk pemeriksaan recovery otomatis."""
    db = os.environ.get("MSSQL_DWH_DB", "Kumparan_DWH")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", db):
        raise ValueError("MSSQL_DWH_DB harus berupa identifier SQL sederhana")
    return f"[{db}].core.articles"


def connection(db_key="MSSQL_DWH_DB"):
    return pymssql.connect(
        server=os.environ["MSSQL_HOST"],
        port=int(os.getenv("MSSQL_PORT", "1433")),
        user=os.environ["MSSQL_USER"],
        password=os.environ["MSSQL_PASSWORD"],
        database=os.environ[db_key],
        charset="UTF-8",
        login_timeout=15,
        timeout=600,
        autocommit=False,
    )


def on_task_failure(context):
    """Best-effort failure audit after Airflow has exhausted task retries."""
    ti = context.get("ti")
    if ti is None:
        return
    run_info = ti.xcom_pull(task_ids="start_pipeline")
    if not isinstance(run_info, dict) or not run_info.get("run_id"):
        return
    conn = None
    try:
        conn = connection()
        cur = conn.cursor()
        message = str(context.get("exception") or "Airflow task failed")[:2000]
        cur.execute("""UPDATE etl.pipeline_runs
            SET status='FAILED', finished_at=SYSUTCDATETIME(), error_message=%s
            WHERE run_id=%s AND status<>'SUCCESS'""", (message, run_info["run_id"]))
        conn.commit()
    except Exception:
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()


@dag(
    dag_id=PIPELINE,
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_task_failure,
    },
    tags=["kumparan", "sqlserver", "staging", "cleansing", "core", "mart", "monitoring"],
)
def kumparan_pipeline():
    @task(task_id="start_pipeline")
    def start_pipeline():
        context = get_current_context()
        config = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        mode = config.get("mode", "incremental")
        if mode not in ("incremental", "backfill"):
            raise ValueError("mode must be incremental or backfill")
        if mode == "backfill":
            if context.get("dag_run").run_type != "manual":
                raise ValueError("Backfill is only allowed for manually triggered runs")
            id_from = int(config.get("article_id_from", 0))
            id_to = int(config.get("article_id_to", 0))
            if not (1 <= id_from <= id_to and id_to - id_from < 100000):
                raise ValueError("Backfill requires valid article_id_from/article_id_to (max 100000 IDs)")
        else:
            id_from = id_to = None
        conn = connection()
        try:
            cur = conn.cursor(as_dict=True)
            # Validasi dependensi sebelum membuat catatan run agar kegagalan mudah didiagnosis.
            required_tables = (
                "etl.pipeline_runs", "etl.watermarks", "etl.recovery_state",
                "stg.articles_raw", "stg.articles_clean", "stg.articles_rejected",
                "core.articles", "core.article_change_history",
                "etl.data_quality_results", "mart.dim_author", "mart.dim_date",
                "mart.fact_article_publication", "mart.agg_article_daily",
                "mart.agg_author_daily",
            )
            missing = []
            for table_name in required_tables:
                cur.execute("SELECT OBJECT_ID(%s, 'U') AS object_id", (table_name,))
                if cur.fetchone()["object_id"] is None:
                    missing.append(table_name)
            if missing:
                raise RuntimeError(
                    "Tabel Kumparan_DWH belum tersedia: " + ", ".join(missing)
                    + ". Jalankan skrip migrasi yang disertakan pada SQL Server "
                    "(khusus tabel baru), lalu clear task start_pipeline. "
                    "Database tidak perlu di-reset."
                )
            cur.execute("""INSERT INTO etl.pipeline_runs (dag_id,status,started_at)
                OUTPUT INSERTED.run_id VALUES (%s,'RUNNING',SYSUTCDATETIME())""", (PIPELINE,))
            run_id = int(cur.fetchone()["run_id"])
            cur.execute("""SELECT last_updated_at FROM etl.watermarks
                WHERE pipeline_name=%s""", (PIPELINE,))
            state = cur.fetchone()
            lower = (state["last_updated_at"] - timedelta(minutes=OVERLAP_MINUTES)
                     if state else datetime(1900, 1, 1))
            # Cursor recovery disimpan terpisah; tidak mengganggu watermark incremental.
            cur.execute("SELECT last_scanned_id FROM etl.recovery_state WHERE pipeline_name=%s", (PIPELINE,))
            recovery_row = cur.fetchone()
            recovery_cursor = int(recovery_row["last_scanned_id"]) if recovery_row else 0
            # Cutoff dicatat satu kali; retry memakai nilai yang sama.
            cur.execute("SELECT SYSUTCDATETIME() AS cutoff")
            cutoff = cur.fetchone()["cutoff"]
            conn.commit()
            return {"run_id": run_id, "batch_id": str(uuid.uuid4()),
                    "lower": lower.isoformat(), "cutoff": cutoff.isoformat(),
                    "mode": mode, "article_id_from": id_from, "article_id_to": id_to,
                    "recovery_cursor": recovery_cursor}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="extract_source")
    def extract_source(info):
        """Ekstraksi incremental + recovery bergilir; tidak menghapus snapshot run lain."""
        src = connection("MSSQL_SOURCE_DB")
        dst = connection()
        extracted = 0
        recovery_next = info["recovery_cursor"]
        try:
            s = src.cursor(as_dict=True)
            d = dst.cursor()
            d.execute("DELETE FROM stg.articles_raw WHERE run_id=%s", (info["run_id"],))
            dst.commit()
            seen_ids = set()

            def store(rows):
                nonlocal extracted
                unique = [r for r in rows if int(r["id"]) not in seen_ids]
                if not unique:
                    return
                payload = [(info["batch_id"], info["run_id"], r["id"], r["title"],
                            r["content"], r["published_at"], r["author_id"],
                            r["created_at"], r["updated_at"], r["deleted_at"])
                           for r in unique]
                d.executemany("""INSERT INTO stg.articles_raw
                    (batch_id,run_id,id,title,content,published_at,author_id,
                     created_at,updated_at,deleted_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", payload)
                dst.commit()
                seen_ids.update(int(r["id"]) for r in unique)
                extracted += len(unique)

            if info["mode"] == "backfill":
                # Manual: untuk uji ulang atau koreksi rentang ID tertentu.
                last_id = info["article_id_from"] - 1
                while True:
                    s.execute("""SELECT TOP (%s) id,title,content,published_at,author_id,
                        created_at,updated_at,deleted_at FROM dbo.articles
                        WHERE id BETWEEN %s AND %s AND id > %s ORDER BY id""",
                        (BATCH_SIZE, info["article_id_from"], info["article_id_to"], last_id))
                    rows = s.fetchall()
                    if not rows:
                        break
                    store(rows)
                    last_id = int(rows[-1]["id"])
            else:
                # Tahap 1: incremental berdasarkan watermark dengan overlap 5 menit.
                lower = datetime.fromisoformat(info["lower"])
                cutoff = datetime.fromisoformat(info["cutoff"])
                last_ts, last_id = lower, -1
                while True:
                    s.execute("""SELECT TOP (%s) id,title,content,published_at,author_id,
                        created_at,updated_at,deleted_at FROM dbo.articles
                        WHERE updated_at >= %s AND updated_at < %s
                          AND (updated_at > %s OR (updated_at = %s AND id > %s))
                        ORDER BY updated_at,id""",
                        (BATCH_SIZE, lower, cutoff, last_ts, last_ts, last_id))
                    rows = s.fetchall()
                    if not rows:
                        break
                    store(rows)
                    last_ts, last_id = rows[-1]["updated_at"], int(rows[-1]["id"])

                # Tahap 2: recovery scan. Setiap jam periksa maksimal 1000 ID
                # historis. Cursor berputar ke awal setelah ID terakhir.
                # Bandingkan keberadaan dan updated_at, bukan hanya ID terbesar.
                # Perubahan dengan updated_at identik memerlukan CDC/hash terpisah.
                if RECOVERY_SCAN_SIZE < 1 or RECOVERY_SCAN_SIZE > 100000:
                    raise ValueError("RECOVERY_SCAN_SIZE harus 1..100000")
                s.execute("""SELECT TOP (%s) id,title,content,published_at,author_id,
                    created_at,updated_at,deleted_at FROM dbo.articles
                    WHERE id > %s ORDER BY id""",
                    (RECOVERY_SCAN_SIZE, info["recovery_cursor"]))
                scan_rows = s.fetchall()
                if not scan_rows and info["recovery_cursor"] != 0:
                    # Wrap-around: mulai dari ID paling kecil.
                    s.execute("""SELECT TOP (%s) id,title,content,published_at,author_id,
                        created_at,updated_at,deleted_at FROM dbo.articles ORDER BY id""",
                        (RECOVERY_SCAN_SIZE,))
                    scan_rows = s.fetchall()
                if scan_rows:
                    recovery_next = int(scan_rows[-1]["id"])
                    # Gunakan perbandingan terparameterisasi di database DWH.
                    ids = [int(r["id"]) for r in scan_rows]
                    changed = set()
                    # Query per 500 ID agar parameter SQL Server tidak melampaui batas.
                    for start in range(0, len(ids), 500):
                        chunk = ids[start:start + 500]
                        marks = ",".join(["%s"] * len(chunk))
                        d.execute(f"SELECT id,updated_at FROM core.articles WHERE id IN ({marks})", tuple(chunk))
                        existing = {int(row[0]): row[1] for row in d.fetchall()}
                        for row in scan_rows[start:start + 500]:
                            article_id = int(row["id"])
                            if article_id not in existing or existing[article_id] != row["updated_at"]:
                                changed.add(article_id)
                    store([r for r in scan_rows if int(r["id"]) in changed])

            d.execute("UPDATE etl.pipeline_runs SET extracted_count=%s WHERE run_id=%s",
                      (extracted, info["run_id"]))
            dst.commit()
            return {"count": extracted, "recovery_next": recovery_next}
        except Exception:
            dst.rollback()
            raise
        finally:
            src.close()
            dst.close()

    @task(task_id="load_staging")
    def load_staging(info, extracted):
        """Validate the durable staging snapshot (no duplicate article IDs)."""
        conn = connection()
        try:
            cur = conn.cursor(as_dict=True)
            cur.execute("""SELECT COUNT_BIG(*) AS total, COUNT_BIG(DISTINCT id) AS unique_ids
                FROM stg.articles_raw WHERE run_id=%s""", (info["run_id"],))
            result = cur.fetchone()
            if int(result["total"]) != extracted["count"] or result["total"] != result["unique_ids"]:
                raise RuntimeError("Staging row count or distinct article IDs do not match extraction")
            return int(result["total"])
        finally:
            conn.close()

    @task(task_id="clean_and_validate_articles")
    def clean_and_validate_articles(info, staged):
        """Persist valid rows in clean and invalid rows in rejected, with audit."""
        from pathlib import Path
        conn = connection()
        try:
            d = conn.cursor(as_dict=True)
            script = (Path(os.environ.get("KUMPARAN_SQL_DIR", "/opt/airflow/etl/sql")) / "cleansing_articles.sql").read_text(encoding="utf-8")
            d.execute(script, (info["run_id"], info["run_id"], info["run_id"]))
            d.execute("""SELECT
                (SELECT COUNT_BIG(*) FROM stg.articles_clean WHERE run_id=%s) AS clean_count,
                (SELECT COUNT_BIG(*) FROM stg.articles_rejected WHERE run_id=%s) AS rejected_count,
                (SELECT COUNT_BIG(*) FROM stg.articles_clean WHERE run_id=%s AND content IS NULL) AS empty_content""",
                (info["run_id"], info["run_id"], info["run_id"]))
            r = d.fetchone()
            clean_count = int(r["clean_count"])
            rejected_count = int(r["rejected_count"])
            checks = [
                ("cleansing_partition_matches_raw", clean_count + rejected_count, int(staged), True),
                ("cleansing_rejected_count", rejected_count, 0, False),
                ("cleansing_empty_content_warning", int(r["empty_content"]), 0, False),
            ]
            d.execute("DELETE FROM etl.data_quality_results WHERE run_id=%s AND check_name LIKE 'cleansing_%%'", (info["run_id"],))
            for name, observed, expected, critical in checks:
                d.execute("""INSERT INTO etl.data_quality_results
                    (run_id,check_name,passed,observed_value,expected_value,details)
                    VALUES (%s,%s,%s,%s,%s,%s)""",
                    (info["run_id"], name, int(observed == expected), str(observed), str(expected),
                     "CRITICAL" if critical else "WARNING - retained for audit"))
            conn.commit()
            failures = [name for name, actual, expected, critical in checks if critical and actual != expected]
            if failures:
                raise RuntimeError("Cleansing validation failed: " + ", ".join(failures))
            return clean_count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="sync_core_and_history")
    def sync_core_and_history(info, staged):
        """History and core writes are atomic; re-running won't double-count changes."""
        conn = connection()
        try:
            d = conn.cursor()
            # Each run's staged batch has at most one row per article.
            # Insert CORE first, then history. This also works when history.article_id
            # has a foreign key referencing core. Capture exactly newly inserted IDs.
            d.execute("CREATE TABLE #new_core_ids (id BIGINT NOT NULL PRIMARY KEY)")
            d.execute("""INSERT INTO core.articles
                (id,title,content,published_at,author_id,created_at,updated_at,deleted_at)
                OUTPUT INSERTED.id INTO #new_core_ids(id)
                SELECT s.id,s.title,s.content,s.published_at,s.author_id,s.created_at,
                    s.updated_at,s.deleted_at FROM stg.articles_clean s
                WHERE s.run_id=%s AND NOT EXISTS
                (SELECT 1 FROM core.articles c WITH (UPDLOCK, HOLDLOCK) WHERE c.id=s.id)""",
                (info["run_id"],))
            inserted = d.rowcount
            d.execute("""INSERT INTO core.article_change_history
                (article_id,run_id,change_type,old_title,new_title,old_updated_at,new_updated_at)
                SELECT s.id,%s,'INSERT',NULL,s.title,NULL,s.updated_at
                FROM stg.articles_clean s
                JOIN #new_core_ids n ON n.id=s.id
                WHERE s.run_id=%s""", (info["run_id"], info["run_id"]))
            # Only log actual differences, including restore / soft delete.
            d.execute("""SELECT
                COALESCE(SUM(CASE WHEN c.deleted_at IS NULL AND s.deleted_at IS NOT NULL
                    THEN 1 ELSE 0 END),0), COUNT_BIG(*)
                FROM core.articles c JOIN stg.articles_clean s ON s.id=c.id
                WHERE s.run_id=%s AND (c.is_hard_deleted=1 OR EXISTS (
                  SELECT c.title,c.content,c.published_at,c.author_id,c.created_at,c.updated_at,c.deleted_at
                  EXCEPT
                  SELECT s.title,s.content,s.published_at,s.author_id,s.created_at,s.updated_at,s.deleted_at))""",
                (info["run_id"],))
            soft_deleted, updated = (int(v) for v in d.fetchone())
            change_predicate = """c.is_hard_deleted=1 OR EXISTS (
                SELECT c.title,c.content,c.published_at,c.author_id,c.created_at,c.updated_at,c.deleted_at
                EXCEPT SELECT s.title,s.content,s.published_at,s.author_id,s.created_at,s.updated_at,s.deleted_at)"""
            d.execute(f"""INSERT INTO core.article_change_history
                (article_id,run_id,change_type,old_title,new_title,old_updated_at,new_updated_at)
                SELECT c.id,%s,
                  CASE WHEN c.is_hard_deleted=1 THEN 'RESTORE'
                       WHEN c.deleted_at IS NULL AND s.deleted_at IS NOT NULL THEN 'SOFT_DELETE'
                       WHEN c.deleted_at IS NOT NULL AND s.deleted_at IS NULL THEN 'UNDELETE'
                       ELSE 'UPDATE' END,
                  c.title,s.title,c.updated_at,s.updated_at
                FROM core.articles c JOIN stg.articles_clean s ON s.id=c.id
                WHERE s.run_id=%s AND ({change_predicate})""",
                (info["run_id"], info["run_id"]))
            d.execute(f"""UPDATE c SET c.title=s.title,c.content=s.content,
                c.published_at=s.published_at,c.author_id=s.author_id,
                c.created_at=s.created_at,c.updated_at=s.updated_at,
                c.deleted_at=s.deleted_at,c.is_hard_deleted=0,
                c.hard_deleted_detected_at=NULL,c.last_synced_at=SYSUTCDATETIME()
                FROM core.articles c JOIN stg.articles_clean s ON s.id=c.id
                WHERE s.run_id=%s AND ({change_predicate})""", (info["run_id"],))
            d.execute("""UPDATE etl.pipeline_runs SET inserted_count=%s,updated_count=%s,
                soft_deleted_count=%s WHERE run_id=%s""",
                (inserted, updated, soft_deleted, info["run_id"]))
            conn.commit()
            return {"inserted": inserted, "updated": updated, "soft_deleted": soft_deleted}
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="detect_hard_delete")
    def detect_hard_delete(info, _core_counts):
        """Mark missing source IDs; never physically delete core articles."""
        if info["mode"] == "backfill":
            return 0  # A partial ID range cannot prove global hard deletes.
        conn = connection()
        try:
            d = conn.cursor()
            d.execute(f"""INSERT INTO core.article_change_history
                (article_id,run_id,change_type,old_title,old_updated_at)
                SELECT c.id,%s,'HARD_DELETE',c.title,c.updated_at
                FROM core.articles c WHERE c.is_hard_deleted=0 AND NOT EXISTS
                (SELECT 1 FROM {source_table()} s WHERE s.id=c.id)""",
                (info["run_id"],))
            d.execute(f"""UPDATE c SET is_hard_deleted=1,
                hard_deleted_detected_at=SYSUTCDATETIME(),last_synced_at=SYSUTCDATETIME()
                FROM core.articles c WHERE c.is_hard_deleted=0 AND NOT EXISTS
                (SELECT 1 FROM {source_table()} s WHERE s.id=c.id)""")
            count = d.rowcount
            d.execute("UPDATE etl.pipeline_runs SET hard_deleted_count=%s WHERE run_id=%s",
                      (count, info["run_id"]))
            conn.commit()
            return count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="refresh_data_mart")
    def refresh_data_mart(info, _hard_deleted):
        """Atomic full mart refresh; appropriate for current ~12k-row assessment."""
        conn = connection()
        try:
            d = conn.cursor()
            d.execute("""INSERT INTO mart.dim_author(author_id,first_seen_at,last_seen_at)
                SELECT DISTINCT c.author_id,SYSUTCDATETIME(),SYSUTCDATETIME()
                FROM core.articles c WHERE NOT EXISTS
                (SELECT 1 FROM mart.dim_author a WHERE a.author_id=c.author_id)""")
            d.execute("""INSERT INTO mart.dim_date
                (date_key,full_date,calendar_year,calendar_month,calendar_day,calendar_quarter)
                SELECT DISTINCT YEAR(c.published_at)*10000+MONTH(c.published_at)*100+DAY(c.published_at),
                    CAST(c.published_at AS DATE),YEAR(c.published_at),MONTH(c.published_at),
                    DAY(c.published_at),DATEPART(QUARTER,c.published_at)
                FROM core.articles c WHERE c.published_at IS NOT NULL AND NOT EXISTS
                (SELECT 1 FROM mart.dim_date x WHERE x.full_date=CAST(c.published_at AS DATE))""")
            # Aggregate/fact rows are derived data. Refresh within one transaction.
            d.execute("DELETE FROM mart.agg_author_daily")
            d.execute("DELETE FROM mart.agg_article_daily")
            d.execute("DELETE FROM mart.fact_article_publication")
            d.execute("""INSERT INTO mart.fact_article_publication
                (article_id,author_id,date_key,published_at,article_count,refreshed_at)
                SELECT id,author_id,YEAR(published_at)*10000+MONTH(published_at)*100+DAY(published_at),
                    published_at,1,SYSUTCDATETIME() FROM core.articles
                WHERE published_at IS NOT NULL AND deleted_at IS NULL AND is_hard_deleted=0""")
            fact_count = d.rowcount
            d.execute("""INSERT INTO mart.agg_article_daily(date_key,published_articles,refreshed_at)
                SELECT date_key,COUNT(*),SYSUTCDATETIME()
                FROM mart.fact_article_publication GROUP BY date_key""")
            d.execute("""INSERT INTO mart.agg_author_daily(date_key,author_id,published_articles,refreshed_at)
                SELECT date_key,author_id,COUNT(*),SYSUTCDATETIME()
                FROM mart.fact_article_publication GROUP BY date_key,author_id""")
            conn.commit()
            return fact_count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="run_data_quality")
    def run_data_quality(info, _fact_count):
        """Persist all check results, then fail the task if any check fails."""
        conn = connection()
        try:
            d = conn.cursor(as_dict=True)
            checks = [
                ("fact_matches_active_published", """SELECT
                    (SELECT COUNT_BIG(*) FROM mart.fact_article_publication) AS actual,
                    (SELECT COUNT_BIG(*) FROM core.articles WHERE published_at IS NOT NULL
                     AND deleted_at IS NULL AND is_hard_deleted=0) AS expected"""),
                ("article_daily_matches_fact", """SELECT
                    (SELECT COALESCE(SUM(CONVERT(BIGINT,published_articles)),0) FROM mart.agg_article_daily) AS actual,
                    (SELECT COUNT_BIG(*) FROM mart.fact_article_publication) AS expected"""),
                ("author_daily_matches_fact", """SELECT
                    (SELECT COALESCE(SUM(CONVERT(BIGINT,published_articles)),0) FROM mart.agg_author_daily) AS actual,
                    (SELECT COUNT_BIG(*) FROM mart.fact_article_publication) AS expected"""),
                ("fact_dimensions_valid", """SELECT
                    (SELECT COUNT_BIG(*) FROM mart.fact_article_publication f
                     LEFT JOIN mart.dim_date dt ON dt.date_key=f.date_key
                     LEFT JOIN mart.dim_author a ON a.author_id=f.author_id
                     WHERE dt.date_key IS NULL OR a.author_id IS NULL) AS actual, CAST(0 AS BIGINT) AS expected"""),
                ("cleansing_rows_match_staging", """SELECT
                    ((SELECT COUNT_BIG(*) FROM stg.articles_clean WHERE run_id=%s) +
                     (SELECT COUNT_BIG(*) FROM stg.articles_rejected WHERE run_id=%s)) AS actual,
                    (SELECT COUNT_BIG(*) FROM stg.articles_raw WHERE run_id=%s) AS expected"""),
                ("cleansing_core_matches", """SELECT
                    (SELECT COUNT_BIG(*) FROM stg.articles_clean s
                     LEFT JOIN core.articles c ON c.id=s.id
                     WHERE s.run_id=%s AND (c.id IS NULL OR c.is_hard_deleted=1 OR EXISTS (
                       SELECT c.title,c.content,c.published_at,c.author_id,c.created_at,c.updated_at,c.deleted_at
                       EXCEPT SELECT s.title,s.content,s.published_at,s.author_id,s.created_at,s.updated_at,s.deleted_at))) AS actual,
                    CAST(0 AS BIGINT) AS expected"""),
                ("core_no_orphan_active_articles", f"""SELECT
                    (SELECT COUNT_BIG(*) FROM core.articles c WHERE c.is_hard_deleted=0
                     AND NOT EXISTS (SELECT 1 FROM {source_table()} src WHERE src.id=c.id)) AS actual,
                    CAST(0 AS BIGINT) AS expected"""),
            ]
            failed = []
            # Retry-safe: replace only this run's DQ results.
            d.execute("DELETE FROM etl.data_quality_results WHERE run_id=%s AND check_name NOT LIKE 'cleansing_%%'", (info["run_id"],))
            for name, sql in checks:
                if name == "cleansing_rows_match_staging":
                    d.execute(sql, (info["run_id"], info["run_id"], info["run_id"]))
                elif name == "cleansing_core_matches":
                    d.execute(sql, (info["run_id"],))
                else:
                    d.execute(sql)
                values = d.fetchone()
                actual, expected = int(values["actual"]), int(values["expected"])
                passed = actual == expected
                d.execute("""INSERT INTO etl.data_quality_results
                    (run_id,check_name,passed,observed_value,expected_value)
                    VALUES (%s,%s,%s,%s,%s)""",
                    (info["run_id"], name, int(passed), str(actual), str(expected)))
                if not passed:
                    failed.append(f"{name}: {actual} != {expected}")
            conn.commit()  # Keep failed DQ evidence even if task raises.
            if failed:
                raise RuntimeError("Data quality failed: " + "; ".join(failed))
            return len(checks)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task(task_id="finalize_pipeline")
    def finalize_pipeline(info, _quality_count, extracted):
        """Advance watermark only after every upstream task and DQ passed."""
        conn = connection()
        try:
            d = conn.cursor()
            if info["mode"] == "backfill":
                d.execute("""UPDATE etl.pipeline_runs SET status='SUCCESS',
                    finished_at=SYSUTCDATETIME(),error_message=NULL WHERE run_id=%s""",
                    (info["run_id"],))
                conn.commit()
                return
            # Cursor recovery baru dipindahkan sesudah seluruh task sukses.
            d.execute("""UPDATE etl.recovery_state SET last_scanned_id=%s,
                recorded_at=SYSUTCDATETIME() WHERE pipeline_name=%s""",
                (extracted["recovery_next"], PIPELINE))
            if d.rowcount == 0:
                d.execute("""INSERT INTO etl.recovery_state (pipeline_name,last_scanned_id)
                    VALUES (%s,%s)""", (PIPELINE, extracted["recovery_next"]))
            cutoff = datetime.fromisoformat(info["cutoff"])
            d.execute("""UPDATE etl.watermarks
                SET last_updated_at=%s,last_article_id=0,last_success_run_id=%s,
                    recorded_at=SYSUTCDATETIME() WHERE pipeline_name=%s""",
                (cutoff, info["run_id"], PIPELINE))
            if d.rowcount == 0:
                d.execute("""INSERT INTO etl.watermarks
                    (pipeline_name,last_updated_at,last_article_id,last_success_run_id)
                    VALUES (%s,%s,0,%s)""", (PIPELINE, cutoff, info["run_id"]))
            d.execute("""UPDATE etl.pipeline_runs SET status='SUCCESS',
                finished_at=SYSUTCDATETIME(),error_message=NULL WHERE run_id=%s""",
                (info["run_id"],))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    info = start_pipeline()
    extracted = extract_source(info)
    staged = load_staging(info, extracted)
    cleaned = clean_and_validate_articles(info, staged)
    core_counts = sync_core_and_history(info, cleaned)
    deleted = detect_hard_delete(info, core_counts)
    facts = refresh_data_mart(info, deleted)
    quality = run_data_quality(info, facts)
    finalize_pipeline(info, quality, extracted)


kumparan_pipeline()
