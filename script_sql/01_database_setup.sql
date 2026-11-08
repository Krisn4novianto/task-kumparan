```
/* ============================================================================
   KUMPARAN | DATA ENGINEER ASSESSMENT
   DATABASE SETUP TERPADU | SOURCE, ETL, STAGING, CORE, MART
   SQL Server / DBeaver

   Tujuan:
   - Membuat database Source dan DWH jika belum ada
   - Membuat schema, tabel, dan index jika belum ada
   - Menyiapkan staging RAW, CLEAN, REJECTED dan checkpoint recovery
   - Tidak membuat ulang tabel/index yang sudah ada
   - Tidak menghapus baris data; penyesuaian nullability CLEAN dapat mengubah skema

   Catatan:
   - Script ini bukan pipeline ETL. ETL dijalankan oleh Apache Airflow.
   - IF NOT EXISTS tidak memigrasi struktur tabel lama secara umum.
   - Pastikan skema cocok dengan DAG sebelum digunakan di lingkungan produksi.
   - Index tambahan meningkatkan performa query tertentu, tetapi juga
     membutuhkan penyimpanan dan biaya pemeliharaan saat DML.
   ============================================================================ */

SET NOCOUNT ON;

/* 01. DATABASE */
IF DB_ID(N'Kumparan_Source') IS NULL
    EXEC(N'CREATE DATABASE Kumparan_Source');

IF DB_ID(N'Kumparan_DWH') IS NULL
    EXEC(N'CREATE DATABASE Kumparan_DWH');

/* 02. SOURCE */
IF OBJECT_ID(N'Kumparan_Source.dbo.articles', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_Source.dbo.articles (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        title NVARCHAR(500) NOT NULL,
        content NVARCHAR(MAX) NOT NULL,
        published_at DATETIME2(3) NULL,
        author_id BIGINT NOT NULL,
        created_at DATETIME2(3) NOT NULL,
        updated_at DATETIME2(3) NOT NULL,
        deleted_at DATETIME2(3) NULL
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_Source.sys.indexes WHERE object_id = OBJECT_ID(N'Kumparan_Source.dbo.articles') AND name = N'IX_articles_updated_id')
    CREATE NONCLUSTERED INDEX IX_articles_updated_id
    ON Kumparan_Source.dbo.articles (updated_at, id)
    INCLUDE (published_at, author_id, deleted_at);

/* 03. SCHEMA DWH */
IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.schemas WHERE name = N'stg')
    EXEC(N'Kumparan_DWH.sys.sp_executesql N''CREATE SCHEMA stg''');
IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.schemas WHERE name = N'core')
    EXEC(N'Kumparan_DWH.sys.sp_executesql N''CREATE SCHEMA core''');
IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.schemas WHERE name = N'mart')
    EXEC(N'Kumparan_DWH.sys.sp_executesql N''CREATE SCHEMA mart''');
IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.schemas WHERE name = N'etl')
    EXEC(N'Kumparan_DWH.sys.sp_executesql N''CREATE SCHEMA etl''');

/* 04. ETL MONITORING */
IF OBJECT_ID(N'Kumparan_DWH.etl.pipeline_runs', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.etl.pipeline_runs (
        run_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        dag_id NVARCHAR(250) NOT NULL DEFAULT N'kumparan_articles_hourly',
        airflow_run_id NVARCHAR(250) NULL,
        status NVARCHAR(30) NOT NULL,
        started_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        finished_at DATETIME2(3) NULL,
        extracted_count INT NOT NULL DEFAULT 0,
        inserted_count INT NOT NULL DEFAULT 0,
        updated_count INT NOT NULL DEFAULT 0,
        soft_deleted_count INT NOT NULL DEFAULT 0,
        hard_deleted_count INT NOT NULL DEFAULT 0,
        error_message NVARCHAR(MAX) NULL
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE object_id = OBJECT_ID(N'Kumparan_DWH.etl.pipeline_runs') AND name = N'IX_runs_dag_started')
    CREATE NONCLUSTERED INDEX IX_runs_dag_started
    ON Kumparan_DWH.etl.pipeline_runs (dag_id, started_at)
    INCLUDE (status, finished_at);

IF OBJECT_ID(N'Kumparan_DWH.etl.watermarks', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.etl.watermarks (
        pipeline_name NVARCHAR(250) NOT NULL PRIMARY KEY,
        last_updated_at DATETIME2(3) NOT NULL,
        last_article_id BIGINT NOT NULL DEFAULT 0,
        last_success_run_id BIGINT NULL,
        recorded_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;


/* 05. ETL RECOVERY STATE
   Menyimpan checkpoint ID terakhir yang diperiksa saat pemulihan data historis.
   Terpisah dari watermark incremental agar proses recovery tidak mengganggu
   posisi ekstraksi normal. */
IF OBJECT_ID(N'Kumparan_DWH.etl.recovery_state', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.etl.recovery_state (
        pipeline_name NVARCHAR(200) NOT NULL PRIMARY KEY,
        last_scanned_id BIGINT NOT NULL CONSTRAINT DF_recovery_last_id DEFAULT (0),
        recorded_at DATETIME2(3) NOT NULL CONSTRAINT DF_recovery_recorded DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'Kumparan_DWH.etl.data_quality_results', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.etl.data_quality_results (
        quality_result_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        run_id BIGINT NOT NULL,
        check_name NVARCHAR(200) NOT NULL,
        passed BIT NOT NULL,
        observed_value NVARCHAR(500) NULL,
        expected_value NVARCHAR(500) NULL,
        checked_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        details NVARCHAR(MAX) NULL
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE object_id = OBJECT_ID(N'Kumparan_DWH.etl.data_quality_results') AND name = N'IX_quality_run_passed')
    CREATE NONCLUSTERED INDEX IX_quality_run_passed
    ON Kumparan_DWH.etl.data_quality_results (run_id, passed)
    INCLUDE (check_name, checked_at);

/* 06. STAGING */
IF OBJECT_ID(N'Kumparan_DWH.stg.articles_raw', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.stg.articles_raw (
        staging_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        batch_id UNIQUEIDENTIFIER NOT NULL,
        run_id BIGINT NULL,
        id BIGINT NOT NULL,
        title NVARCHAR(500) NOT NULL,
        content NVARCHAR(MAX) NOT NULL,
        published_at DATETIME2(3) NULL,
        author_id BIGINT NOT NULL,
        created_at DATETIME2(3) NOT NULL,
        updated_at DATETIME2(3) NOT NULL,
        deleted_at DATETIME2(3) NULL,
        extracted_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE object_id = OBJECT_ID(N'Kumparan_DWH.stg.articles_raw') AND name = N'IX_stg_article_updated')
    CREATE NONCLUSTERED INDEX IX_stg_article_updated
    ON Kumparan_DWH.stg.articles_raw (id, updated_at)
    INCLUDE (run_id, batch_id);

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE name = N'IX_stg_batch_article' AND object_id = OBJECT_ID(N'Kumparan_DWH.stg.articles_raw'))
    CREATE NONCLUSTERED INDEX IX_stg_batch_article ON Kumparan_DWH.stg.articles_raw (batch_id, id);

/* 07. STAGING CLEAN — HASIL CLEANSING PER RUN
   Struktur mengikuti script setup cleansing yang diberikan.
   NULL pada title/content dipertahankan untuk kompatibilitas proses validasi.
   Perubahan nullability pada tabel lama dilakukan hanya jika diperlukan. */
IF OBJECT_ID(N'Kumparan_DWH.stg.articles_clean', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.stg.articles_clean (
        run_id BIGINT NOT NULL,
        id BIGINT NOT NULL,
        title NVARCHAR(500) NULL,
        content NVARCHAR(MAX) NULL,
        published_at DATETIME2(3) NULL,
        author_id BIGINT NOT NULL,
        created_at DATETIME2(3) NOT NULL,
        updated_at DATETIME2(3) NOT NULL,
        deleted_at DATETIME2(3) NULL,
        cleaned_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_articles_clean_run_id PRIMARY KEY (run_id, id)
    );
END;

/* Kompatibilitas dengan versi lama yang menggunakan NOT NULL. */
IF EXISTS (
    SELECT 1
    FROM Kumparan_DWH.sys.columns AS c
    JOIN Kumparan_DWH.sys.tables AS t ON c.object_id = t.object_id
    JOIN Kumparan_DWH.sys.schemas AS sc ON t.schema_id = sc.schema_id
    WHERE sc.name = N'stg' AND t.name = N'articles_clean'
      AND c.name = N'title' AND c.is_nullable = 0
)
    ALTER TABLE Kumparan_DWH.stg.articles_clean
    ALTER COLUMN title NVARCHAR(500) NULL;

IF EXISTS (
    SELECT 1
    FROM Kumparan_DWH.sys.columns AS c
    JOIN Kumparan_DWH.sys.tables AS t ON c.object_id = t.object_id
    JOIN Kumparan_DWH.sys.schemas AS sc ON t.schema_id = sc.schema_id
    WHERE sc.name = N'stg' AND t.name = N'articles_clean'
      AND c.name = N'content' AND c.is_nullable = 0
)
    ALTER TABLE Kumparan_DWH.stg.articles_clean
    ALTER COLUMN content NVARCHAR(MAX) NULL;


/* 08. STAGING REJECTED — ARTIKEL TIDAK LOLOS VALIDASI
   Menyimpan artikel yang ditolak pada setiap run berikut alasan penolakannya.
   Primary Key (run_id, id) mencegah duplikasi artikel dalam run yang sama. */
IF OBJECT_ID(N'Kumparan_DWH.stg.articles_rejected', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.stg.articles_rejected (
        run_id BIGINT NOT NULL,
        id BIGINT NOT NULL,
        reject_reason NVARCHAR(100) NOT NULL,
        title NVARCHAR(500) NULL,
        content NVARCHAR(MAX) NULL,
        published_at DATETIME2(3) NULL,
        author_id BIGINT NOT NULL,
        created_at DATETIME2(3) NOT NULL,
        updated_at DATETIME2(3) NOT NULL,
        deleted_at DATETIME2(3) NULL,
        rejected_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_articles_rejected_run_id PRIMARY KEY (run_id, id)
    );
END;

/* 09. CORE */
IF OBJECT_ID(N'Kumparan_DWH.core.articles', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.core.articles (
        id BIGINT NOT NULL PRIMARY KEY,
        title NVARCHAR(500) NOT NULL,
        content NVARCHAR(MAX) NOT NULL,
        published_at DATETIME2(3) NULL,
        author_id BIGINT NOT NULL,
        created_at DATETIME2(3) NOT NULL,
        updated_at DATETIME2(3) NOT NULL,
        deleted_at DATETIME2(3) NULL,
        is_hard_deleted BIT NOT NULL DEFAULT 0,
        hard_deleted_detected_at DATETIME2(3) NULL,
        first_loaded_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        last_synced_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE object_id = OBJECT_ID(N'Kumparan_DWH.core.articles') AND name = N'IX_core_active_published')
    CREATE NONCLUSTERED INDEX IX_core_active_published
    ON Kumparan_DWH.core.articles (published_at, id)
    INCLUDE (author_id)
    WHERE published_at IS NOT NULL AND deleted_at IS NULL AND is_hard_deleted = 0;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE name = N'IX_core_updated' AND object_id = OBJECT_ID(N'Kumparan_DWH.core.articles'))
    CREATE NONCLUSTERED INDEX IX_core_updated ON Kumparan_DWH.core.articles (updated_at, id);

IF OBJECT_ID(N'Kumparan_DWH.core.article_change_history', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.core.article_change_history (
        change_id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        article_id BIGINT NOT NULL,
        run_id BIGINT NULL,
        change_type NVARCHAR(30) NOT NULL,
        old_title NVARCHAR(500) NULL,
        new_title NVARCHAR(500) NULL,
        old_updated_at DATETIME2(3) NULL,
        new_updated_at DATETIME2(3) NULL,
        changed_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE object_id = OBJECT_ID(N'Kumparan_DWH.core.article_change_history') AND name = N'IX_history_article_changed')
    CREATE NONCLUSTERED INDEX IX_history_article_changed
    ON Kumparan_DWH.core.article_change_history (article_id, changed_at)
    INCLUDE (run_id, change_type);

/* 10. DATA MART */
IF OBJECT_ID(N'Kumparan_DWH.mart.dim_date', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.mart.dim_date (
        date_key INT NOT NULL PRIMARY KEY,
        full_date DATE NOT NULL UNIQUE,
        calendar_year SMALLINT NOT NULL,
        calendar_month TINYINT NOT NULL,
        calendar_day TINYINT NOT NULL,
        calendar_quarter TINYINT NOT NULL
    );
END;

IF OBJECT_ID(N'Kumparan_DWH.mart.dim_author', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.mart.dim_author (
        author_id BIGINT NOT NULL PRIMARY KEY,
        first_seen_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        last_seen_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'Kumparan_DWH.mart.fact_article_publication', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.mart.fact_article_publication (
        article_id BIGINT NOT NULL PRIMARY KEY,
        author_id BIGINT NOT NULL,
        date_key INT NOT NULL,
        published_at DATETIME2(3) NOT NULL,
        article_count INT NOT NULL DEFAULT 1,
        refreshed_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE name = N'IX_fact_date_author' AND object_id = OBJECT_ID(N'Kumparan_DWH.mart.fact_article_publication'))
    CREATE NONCLUSTERED INDEX IX_fact_date_author ON Kumparan_DWH.mart.fact_article_publication (date_key, author_id);

IF OBJECT_ID(N'Kumparan_DWH.mart.agg_article_daily', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.mart.agg_article_daily (
        date_key INT NOT NULL PRIMARY KEY,
        published_articles INT NOT NULL,
        refreshed_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME()
    );
END;

IF OBJECT_ID(N'Kumparan_DWH.mart.agg_author_daily', N'U') IS NULL
BEGIN
    CREATE TABLE Kumparan_DWH.mart.agg_author_daily (
        date_key INT NOT NULL,
        author_id BIGINT NOT NULL,
        published_articles INT NOT NULL,
        refreshed_at DATETIME2(3) NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_agg_author_daily PRIMARY KEY (date_key, author_id)
    );
END;

IF NOT EXISTS (SELECT 1 FROM Kumparan_DWH.sys.indexes WHERE object_id = OBJECT_ID(N'Kumparan_DWH.mart.agg_author_daily') AND name = N'IX_agg_author_author_date')
    CREATE NONCLUSTERED INDEX IX_agg_author_author_date
    ON Kumparan_DWH.mart.agg_author_daily (author_id, date_key)
    INCLUDE (published_articles);

/* 11. VERIFIKASI TABEL */
SELECT s.name AS schema_name, t.name AS table_name
FROM Kumparan_DWH.sys.tables AS t
INNER JOIN Kumparan_DWH.sys.schemas AS s ON t.schema_id = s.schema_id
WHERE s.name IN (N'stg', N'core', N'mart', N'etl')
ORDER BY s.name, t.name;

/* 12. VERIFIKASI INDEX DWH */
SELECT s.name AS schema_name, t.name AS table_name, i.name AS index_name,
       i.type_desc AS index_type, i.is_unique, i.is_primary_key
FROM Kumparan_DWH.sys.indexes AS i
INNER JOIN Kumparan_DWH.sys.tables AS t ON i.object_id = t.object_id
INNER JOIN Kumparan_DWH.sys.schemas AS s ON t.schema_id = s.schema_id
WHERE i.name IS NOT NULL AND s.name IN (N'stg', N'core', N'mart', N'etl')
ORDER BY s.name, t.name, i.name;

/* 13. VERIFIKASI INDEX SOURCE */
SELECT s.name AS schema_name, t.name AS table_name, i.name AS index_name,
       i.type_desc AS index_type, i.is_unique, i.is_primary_key
FROM Kumparan_Source.sys.indexes AS i
INNER JOIN Kumparan_Source.sys.tables AS t ON i.object_id = t.object_id
INNER JOIN Kumparan_Source.sys.schemas AS s ON t.schema_id = s.schema_id
WHERE i.name IS NOT NULL AND s.name = N'dbo' AND t.name = N'articles'
ORDER BY i.name;


```