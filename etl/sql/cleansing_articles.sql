/* One atomic SQL batch, three run_id placeholders bound by pymssql.
   Raw/source are never modified. Invalid rows go to rejected, not core. */
SET XACT_ABORT ON;
DELETE FROM stg.articles_clean WHERE run_id=%s;
DELETE FROM stg.articles_rejected WHERE run_id=%s;
IF OBJECT_ID('tempdb..#classified_articles') IS NOT NULL DROP TABLE #classified_articles;
SELECT r.run_id,r.id,
    NULLIF(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(r.title,
        NCHAR(160),N' '),CHAR(13),N' '),CHAR(10),N' '),CHAR(9),N' '))),N'') AS title,
    NULLIF(LTRIM(RTRIM(r.content)),N'') AS content,
    r.published_at,r.author_id,r.created_at,r.updated_at,r.deleted_at,
    CASE WHEN NULLIF(LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(r.title,
        NCHAR(160),N' '),CHAR(13),N' '),CHAR(10),N' '),CHAR(9),N' '))),N'') IS NULL
         THEN N'EMPTY_TITLE'
         WHEN NULLIF(LTRIM(RTRIM(r.content)),N'') IS NULL THEN N'EMPTY_CONTENT'
         WHEN r.updated_at < r.created_at THEN N'INVALID_UPDATED_AT'
         ELSE NULL END AS reject_reason
INTO #classified_articles
FROM stg.articles_raw r WHERE r.run_id=%s;
INSERT INTO stg.articles_clean
    (run_id,id,title,content,published_at,author_id,created_at,updated_at,deleted_at)
SELECT run_id,id,title,content,published_at,author_id,created_at,updated_at,deleted_at
FROM #classified_articles WHERE reject_reason IS NULL;
INSERT INTO stg.articles_rejected
    (run_id,id,reject_reason,title,content,published_at,author_id,created_at,updated_at,deleted_at)
SELECT run_id,id,reject_reason,title,content,published_at,author_id,created_at,updated_at,deleted_at
FROM #classified_articles WHERE reject_reason IS NOT NULL;
DROP TABLE #classified_articles;
