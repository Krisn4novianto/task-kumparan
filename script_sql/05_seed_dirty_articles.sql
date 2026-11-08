/* ============================================================================
   KUMPARAN | DATA ENGINEER ASSESSMENT
   DIRTY DATA SEEDING & DATA QUALITY TESTING
   ============================================================================
   Database : Kumparan_Source
   Tabel    : dbo.articles
   Target   : 200 artikel sintetis (ID 12001 sampai 12200)

   TUJUAN
   Menyiapkan variasi data untuk pengujian extraction, cleansing, validasi
   kualitas data, dan incremental load melalui Apache Airflow.

   DISTRIBUSI SKENARIO
   01. TITLE_EXTRA_SPACES    : 40 artikel; spasi berlebih pada judul
   02. TITLE_TAB_NEWLINE     : 30 artikel; tab dan baris baru pada judul
   03. CONTENT_EXTRA_SPACES  : 30 artikel; spasi berlebih pada konten
   04. EMPTY_TITLE           : 20 artikel; judul hanya berisi spasi
   05. EMPTY_CONTENT         : 20 artikel; konten hanya berisi spasi
   06. INVALID_UPDATED_AT    : 20 artikel; updated_at < created_at
   07. DRAFT                 : 20 artikel; published_at NULL
   08. SOFT_DELETE           : 20 artikel; deleted_at terisi

   KETENTUAN KEAMANAN
   - Khusus database pengujian; tidak menghapus atau mengubah artikel lama.
   - Bila salah satu ID target sudah terisi, script berhenti tanpa INSERT.
   - Jalankan kembali setelah memastikan rentang ID target memang kosong.
   - Hasil CLEAN/REJECTED bergantung pada aturan validasi DAG yang aktif.
   ============================================================================ */

USE Kumparan_Source;
SET NOCOUNT ON;
SET XACT_ABORT ON;

/* 01. VALIDASI TABEL SUMBER DAN RENTANG ID
   Pastikan tabel tersedia dan seluruh ID target masih kosong.
   Validasi dilakukan lagi di dalam transaksi untuk mencegah konflik. */
IF OBJECT_ID(N'dbo.articles', N'U') IS NULL
    THROW 51000, N'Tabel dbo.articles tidak ditemukan.', 1;

DECLARE @identity_on BIT = CASE
    WHEN COLUMNPROPERTY(OBJECT_ID(N'dbo.articles'), N'id', 'IsIdentity') = 1
    THEN 1 ELSE 0 END;
DECLARE @identity_enabled BIT = 0;
DECLARE @seed_time DATETIME2(3) = SYSUTCDATETIME();
DECLARE @inserted INT = 0;

/* 02. TRANSAKSI INSERT
   Seluruh 200 baris disimpan bersama-sama atau dibatalkan seluruhnya. */
BEGIN TRY
    BEGIN TRANSACTION;

    IF EXISTS (
        SELECT 1
        FROM dbo.articles WITH (UPDLOCK, HOLDLOCK)
        WHERE id BETWEEN 12001 AND 12200
    )
        THROW 51002, N'ID 12001-12200 sudah terisi. INSERT dibatalkan agar data lama tetap aman.', 1;

    /* Kolom IDENTITY memerlukan izin eksplisit untuk memasukkan ID tertentu. */
    IF @identity_on = 1
    BEGIN
        SET IDENTITY_INSERT dbo.articles ON;
        SET @identity_enabled = 1;
    END;

    /* 03. PEMBENTUKAN 200 ARTIKEL DAN DELAPAN SKENARIO
       Nomor 1-200 dipetakan ke ID 12001-12200 dan kategori uji. */
    ;WITH numbers AS (
        SELECT TOP (200)
            ROW_NUMBER() OVER (ORDER BY a.object_id, b.object_id) AS rn
        FROM sys.all_objects AS a
        CROSS JOIN sys.all_objects AS b
    ),
    samples AS (
        SELECT
            rn,
            CAST(12000 + rn AS BIGINT) AS article_id,
            DATEADD(DAY, CAST(rn % 700 AS INT),
                    CAST('2024-01-01' AS DATETIME2(3))) AS created_time,
            CASE
                WHEN rn BETWEEN 1 AND 40   THEN N'TITLE_EXTRA_SPACES'
                WHEN rn BETWEEN 41 AND 70  THEN N'TITLE_TAB_NEWLINE'
                WHEN rn BETWEEN 71 AND 100 THEN N'CONTENT_EXTRA_SPACES'
                WHEN rn BETWEEN 101 AND 120 THEN N'EMPTY_TITLE'
                WHEN rn BETWEEN 121 AND 140 THEN N'EMPTY_CONTENT'
                WHEN rn BETWEEN 141 AND 160 THEN N'INVALID_UPDATED_AT'
                WHEN rn BETWEEN 161 AND 180 THEN N'DRAFT'
                ELSE N'SOFT_DELETE'
            END AS scenario
        FROM numbers
    )
    /* 04. INSERT DATA PENGUJIAN
       title/content sengaja diberi whitespace pada beberapa skenario.
       updated_at menggunakan waktu eksekusi untuk uji incremental load. */
    INSERT INTO dbo.articles (
        id, title, content, published_at, author_id,
        created_at, updated_at, deleted_at
    )
    SELECT
        s.article_id,
        /* TITLE: spasi, tab/newline, atau judul normal. */
        CASE
            WHEN s.scenario = N'TITLE_EXTRA_SPACES'
                THEN CONCAT(N'   Ekonomi: Artikel uji ', s.article_id, N'   ')
            WHEN s.scenario = N'TITLE_TAB_NEWLINE'
                THEN CONCAT(NCHAR(9), N'Teknologi:', NCHAR(13), NCHAR(10),
                            N' Artikel uji ', s.article_id, NCHAR(9))
            WHEN s.scenario = N'EMPTY_TITLE' THEN N'   '
            ELSE CONCAT(N'Kumparan: Artikel sintetis ', s.article_id)
        END,
        /* CONTENT: spasi berlebih, konten kosong secara logis, atau normal. */
        CASE
            WHEN s.scenario = N'CONTENT_EXTRA_SPACES'
                THEN CONCAT(N'    Konten artikel uji ', s.article_id,
                            N' dengan spasi berlebih.    ')
            WHEN s.scenario = N'EMPTY_CONTENT' THEN N'   '
            ELSE CONCAT(N'Konten sintetis untuk pengujian ELT ',
                        N'dan data quality. ID ', s.article_id)
        END,
        /* PUBLISHED_AT: NULL khusus draft. */
        CASE WHEN s.scenario = N'DRAFT' THEN NULL
             ELSE DATEADD(HOUR, 2, s.created_time) END,
        /* AUTHOR_ID: 150 kemungkinan penulis sintetis. */
        CAST(1 + (s.rn % 150) AS BIGINT),
        /* CREATED_AT: dibuat lebih baru daripada updated_at untuk uji invalid. */
        CASE WHEN s.scenario = N'INVALID_UPDATED_AT'
             THEN DATEADD(DAY, 1, @seed_time)
             ELSE s.created_time END,
        /* UPDATED_AT: waktu saat seed dijalankan. */
        @seed_time,
        /* DELETED_AT: diisi khusus skenario soft delete. */
        CASE WHEN s.scenario = N'SOFT_DELETE'
             THEN DATEADD(DAY, 2, s.created_time)
             ELSE NULL END
    FROM samples AS s;

    SET @inserted = @@ROWCOUNT;
    IF @inserted <> 200
        THROW 51001, N'Jumlah INSERT tidak mencapai 200 artikel.', 1;

    /* 05. PENYELESAIAN TRANSAKSI DAN IDENTITY_INSERT */
    IF @identity_enabled = 1
    BEGIN
        SET IDENTITY_INSERT dbo.articles OFF;
        SET @identity_enabled = 0;
    END;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;

    IF @identity_enabled = 1
    BEGIN TRY
        SET IDENTITY_INSERT dbo.articles OFF;
    END TRY
    BEGIN CATCH
        PRINT N'Peringatan: gagal menonaktifkan IDENTITY_INSERT.';
    END CATCH;

    THROW;
END CATCH;

/* 06. VERIFIKASI HASIL INSERT
   Pastikan jumlah artikel pada rentang target mencapai 200 baris. */
SELECT
    @inserted AS InsertedArticles,
    COUNT_BIG(*) AS TotalArticlesInTargetRange,
    MIN(updated_at) AS MinUpdatedAt,
    MAX(updated_at) AS MaxUpdatedAt
FROM dbo.articles
WHERE id BETWEEN 12001 AND 12200;

/* 07. VERIFIKASI DISTRIBUSI DELAPAN SKENARIO
   Hasil yang diharapkan: 40, 30, 30, 20, 20, 20, 20, 20. */
;WITH classified AS (
    SELECT
        CASE
            WHEN id BETWEEN 12001 AND 12040 THEN 'TITLE_EXTRA_SPACES'
            WHEN id BETWEEN 12041 AND 12070 THEN 'TITLE_TAB_NEWLINE'
            WHEN id BETWEEN 12071 AND 12100 THEN 'CONTENT_EXTRA_SPACES'
            WHEN id BETWEEN 12101 AND 12120 THEN 'EMPTY_TITLE'
            WHEN id BETWEEN 12121 AND 12140 THEN 'EMPTY_CONTENT'
            WHEN id BETWEEN 12141 AND 12160 THEN 'INVALID_UPDATED_AT'
            WHEN id BETWEEN 12161 AND 12180 THEN 'DRAFT'
            ELSE 'SOFT_DELETE'
        END AS TestScenario
    FROM dbo.articles
    WHERE id BETWEEN 12001 AND 12200
)
SELECT TestScenario, COUNT_BIG(*) AS TotalArticles
FROM classified
GROUP BY TestScenario
ORDER BY TestScenario;
