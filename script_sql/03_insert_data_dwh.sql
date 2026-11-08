SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @now DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @batch_id UNIQUEIDENTIFIER = NEWID();

    /* 01. STAGING */
    WITH NewStage AS (
        SELECT s.*
        FROM Kumparan_Source.dbo.articles AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM Kumparan_DWH.stg.articles_raw AS r
            WHERE r.id = s.id
              AND r.run_id IS NULL
              AND r.updated_at = s.updated_at
        )
    )
    INSERT INTO Kumparan_DWH.stg.articles_raw (
        batch_id, run_id, id, title, content, published_at, author_id, created_at, updated_at, deleted_at, extracted_at
    )
    SELECT
        @batch_id, NULL, id, title, content, published_at, author_id, created_at, updated_at, deleted_at, @now
    FROM NewStage;

    /* 02. HISTORY - ARTIKEL BARU */
    WITH NewArticles AS (
        SELECT s.id, s.title, s.updated_at
        FROM Kumparan_Source.dbo.articles AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM Kumparan_DWH.core.articles AS c
            WHERE c.id = s.id
        )
    )
    INSERT INTO Kumparan_DWH.core.article_change_history (
        article_id, run_id, change_type, old_title, new_title, old_updated_at, new_updated_at, changed_at
    )
    SELECT
        id, NULL, N'INSERT', NULL, title, NULL, updated_at, @now
    FROM NewArticles;

    /* 03. CORE - ARTIKEL BARU */
    ;WITH NewArticles AS (
        SELECT s.*
        FROM Kumparan_Source.dbo.articles AS s
        WHERE NOT EXISTS (
            SELECT 1
            FROM Kumparan_DWH.core.articles AS c
            WHERE c.id = s.id
        )
    )
    INSERT INTO Kumparan_DWH.core.articles (
        id, title, content, published_at,
        author_id, created_at, updated_at,
        deleted_at, is_hard_deleted,
        first_loaded_at, last_synced_at
    )
    SELECT
        id, title, content, published_at,
        author_id, created_at, updated_at,
        deleted_at, 0, @now, @now
    FROM NewArticles;

    /* 04. DIM AUTHOR */
    ;WITH NewAuthors AS (
        SELECT DISTINCT c.author_id
        FROM Kumparan_DWH.core.articles AS c
        WHERE NOT EXISTS (
            SELECT 1
            FROM Kumparan_DWH.mart.dim_author AS a
            WHERE a.author_id = c.author_id
        )
    )
    INSERT INTO Kumparan_DWH.mart.dim_author (
        author_id, first_seen_at, last_seen_at
    )
    SELECT author_id, @now, @now
    FROM NewAuthors;

    /* 05. DIM DATE */
    ;WITH NewDates AS (
        SELECT DISTINCT
            CAST(c.published_at AS DATE) AS full_date
        FROM Kumparan_DWH.core.articles AS c
        WHERE c.published_at IS NOT NULL
          AND c.deleted_at IS NULL
          AND c.is_hard_deleted = 0
          AND NOT EXISTS (
              SELECT 1
              FROM Kumparan_DWH.mart.dim_date AS d
              WHERE d.full_date = CAST(c.published_at AS DATE)
          )
    )
    INSERT INTO Kumparan_DWH.mart.dim_date (
        date_key, full_date, calendar_year,
        calendar_month, calendar_day, calendar_quarter
    )
    SELECT
        YEAR(full_date) * 10000
            + MONTH(full_date) * 100
            + DAY(full_date),
        full_date,
        YEAR(full_date),
        MONTH(full_date),
        DAY(full_date),
        DATEPART(QUARTER, full_date)
    FROM NewDates;

    /* 06. FACT PUBLICATION */
    ;WITH NewFacts AS (
        SELECT
            c.id AS article_id,
            c.author_id,
            YEAR(c.published_at) * 10000
                + MONTH(c.published_at) * 100
                + DAY(c.published_at) AS date_key,
            c.published_at
        FROM Kumparan_DWH.core.articles AS c
        WHERE c.published_at IS NOT NULL
          AND c.deleted_at IS NULL
          AND c.is_hard_deleted = 0
          AND NOT EXISTS (
              SELECT 1
              FROM Kumparan_DWH.mart.fact_article_publication AS f
              WHERE f.article_id = c.id
          )
    )
    INSERT INTO Kumparan_DWH.mart.fact_article_publication (
        article_id, author_id, date_key,
        published_at, article_count, refreshed_at
    )
    SELECT
        article_id, author_id, date_key,
        published_at, 1, @now
    FROM NewFacts;

    /* 07. DAILY AGGREGATE */
    ;WITH DailyArticles AS (
        SELECT
            date_key,
            COUNT(*) AS published_articles
        FROM Kumparan_DWH.mart.fact_article_publication
        GROUP BY date_key
    )
    INSERT INTO Kumparan_DWH.mart.agg_article_daily (
        date_key, published_articles, refreshed_at
    )
    SELECT
        d.date_key, d.published_articles, @now
    FROM DailyArticles AS d
    WHERE NOT EXISTS (
        SELECT 1
        FROM Kumparan_DWH.mart.agg_article_daily AS a
        WHERE a.date_key = d.date_key
    );

    /* 08. AUTHOR DAILY AGGREGATE */
    ;WITH DailyAuthors AS (
        SELECT
            date_key,
            author_id,
            COUNT(*) AS published_articles
        FROM Kumparan_DWH.mart.fact_article_publication
        GROUP BY date_key, author_id
    )
    INSERT INTO Kumparan_DWH.mart.agg_author_daily (
        date_key, author_id,
        published_articles, refreshed_at
    )
    SELECT
        d.date_key, d.author_id,
        d.published_articles, @now
    FROM DailyAuthors AS d
    WHERE NOT EXISTS (
        SELECT 1
        FROM Kumparan_DWH.mart.agg_author_daily AS a
        WHERE a.date_key = d.date_key
          AND a.author_id = d.author_id
    );

    COMMIT TRANSACTION;

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
