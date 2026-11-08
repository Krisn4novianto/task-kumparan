/* ============================================================================
   KUMPARAN | DATA ENGINEER ASSESSMENT
   03 - ETL VALIDATION & PIPELINE MONITORING
   Database : Kumparan_Source (OLTP) dan Kumparan_DWH (SQL Server)
   Pipeline : kumparan_articles_hourly (Apache Airflow)
   Aplikasi : DBeaver / SQL Server

   TUJUAN
   1. Memastikan data tersedia pada setiap layer pipeline.
   2. Memantau status eksekusi Airflow dan incremental watermark.
   3. Memeriksa kualitas data serta hasil cleansing RAW -> CLEAN.
   4. Memverifikasi artikel Core dan riwayat perubahan.
   5. Memeriksa konsistensi Fact dan agregasi Data Mart.

   CARA PAKAI
   - Jalankan setelah DAG Airflow selesai atau ketika melakukan investigasi.
   - Jalankan setiap blok SELECT secara terpisah agar hasil mudah dibaca.
   - Semua query bersifat READ ONLY; tidak ada perubahan data.
   - Perbandingan jumlah antar-layer tidak harus sama karena aturan
     cleansing, artikel belum terbit, soft delete, dan hard delete.
   - Tabel articles_clean dan article_change_history harus sudah tersedia.
   ============================================================================ */

SET NOCOUNT ON;

/* ============================================================================
   01. JUMLAH DATA DI SELURUH LAYER
   Memastikan tabel Source, Staging, Core, Data Mart, dan ETL terisi.
   Perhatikan: jumlah RAW dapat bertambah per run karena menyimpan hasil
   ekstraksi; jumlah CLEAN dan CORE bergantung pada aturan pipeline.
   ============================================================================ */
SELECT 'SOURCE' AS layer_name, 'dbo.articles' AS table_name, COUNT_BIG(*) AS row_count
FROM Kumparan_Source.dbo.articles
UNION ALL SELECT 'STAGING', 'stg.articles_raw', COUNT_BIG(*) FROM Kumparan_DWH.stg.articles_raw
UNION ALL SELECT 'STAGING', 'stg.articles_clean', COUNT_BIG(*) FROM Kumparan_DWH.stg.articles_clean
UNION ALL SELECT 'CORE', 'core.articles', COUNT_BIG(*) FROM Kumparan_DWH.core.articles
UNION ALL SELECT 'CORE', 'core.article_change_history', COUNT_BIG(*) FROM Kumparan_DWH.core.article_change_history
UNION ALL SELECT 'MART', 'mart.dim_date', COUNT_BIG(*) FROM Kumparan_DWH.mart.dim_date
UNION ALL SELECT 'MART', 'mart.dim_author', COUNT_BIG(*) FROM Kumparan_DWH.mart.dim_author
UNION ALL SELECT 'MART', 'mart.fact_article_publication', COUNT_BIG(*) FROM Kumparan_DWH.mart.fact_article_publication
UNION ALL SELECT 'MART', 'mart.agg_article_daily', COUNT_BIG(*) FROM Kumparan_DWH.mart.agg_article_daily
UNION ALL SELECT 'MART', 'mart.agg_author_daily', COUNT_BIG(*) FROM Kumparan_DWH.mart.agg_author_daily
UNION ALL SELECT 'ETL', 'etl.watermarks', COUNT_BIG(*) FROM Kumparan_DWH.etl.watermarks
UNION ALL SELECT 'ETL', 'etl.pipeline_runs', COUNT_BIG(*) FROM Kumparan_DWH.etl.pipeline_runs
UNION ALL SELECT 'ETL', 'etl.data_quality_results', COUNT_BIG(*) FROM Kumparan_DWH.etl.data_quality_results;

/* ============================================================================
   02. MONITORING EKSEKUSI PIPELINE AIRFLOW
   Periksa status setiap run, jumlah data yang diproses, waktu eksekusi,
   serta error_message ketika proses gagal.
   ============================================================================ */
SELECT TOP (20)
    run_id, dag_id, airflow_run_id, status,
    extracted_count, inserted_count, updated_count,
    soft_deleted_count, hard_deleted_count,
    started_at, finished_at, error_message
FROM Kumparan_DWH.etl.pipeline_runs
ORDER BY run_id DESC;

/* ============================================================================
   03. MONITORING INCREMENTAL WATERMARK
   Watermark menandai posisi data terakhir yang berhasil diproses.
   Nilai tidak harus berubah jika memang tidak ada artikel baru/berubah.
   ============================================================================ */
SELECT pipeline_name, last_updated_at, last_article_id,
       last_success_run_id, recorded_at
FROM Kumparan_DWH.etl.watermarks
ORDER BY pipeline_name;

/* ============================================================================
   04. MONITORING SELURUH HASIL DATA QUALITY
   passed = 1 berarti pemeriksaan dinyatakan lolos oleh pipeline;
   passed = 0 berarti perlu ditinjau. Lihat details untuk penyebabnya.
   ============================================================================ */
SELECT TOP (50)
    run_id, check_name, passed, observed_value,
    expected_value, checked_at, details
FROM Kumparan_DWH.etl.data_quality_results
ORDER BY checked_at DESC, run_id DESC;

/* ============================================================================
   05. DATA QUALITY KHUSUS CLEANSING
   Memfilter check_name dengan awalan cleansing_ agar hasil pemeriksaan
   pembersihan data mudah ditelusuri tanpa bercampur dengan check lain.
   ============================================================================ */
SELECT TOP (50)
    run_id, check_name, passed, observed_value,
    expected_value, details, checked_at
FROM Kumparan_DWH.etl.data_quality_results
WHERE check_name LIKE N'cleansing_%'
ORDER BY checked_at DESC, run_id DESC;

/* ============================================================================
   06. PERBANDINGAN JUMLAH RAW DAN CLEAN PER RUN
   Menampilkan jumlah baris RAW dan CLEAN untuk tiap run_id.
   Selisih tidak otomatis berarti error: sebagian data mungkin ditolak
   oleh aturan validasi atau mengalami deduplikasi.

   Catatan: RAW dengan run_id NULL (misalnya hasil ETL manual lama)
   ditampilkan sebagai kelompok terpisah; bukan satu run Airflow.
   ============================================================================ */
WITH raw_counts AS (
    SELECT run_id, COUNT_BIG(*) AS raw_rows
    FROM Kumparan_DWH.stg.articles_raw
    GROUP BY run_id
), clean_counts AS (
    SELECT run_id, COUNT_BIG(*) AS clean_rows
    FROM Kumparan_DWH.stg.articles_clean
    GROUP BY run_id
)
SELECT r.run_id, r.raw_rows,
       COALESCE(c.clean_rows, CONVERT(BIGINT, 0)) AS clean_rows,
       r.raw_rows - COALESCE(c.clean_rows, CONVERT(BIGINT, 0)) AS row_difference
FROM raw_counts AS r
LEFT JOIN clean_counts AS c ON c.run_id = r.run_id
ORDER BY CASE WHEN r.run_id IS NULL THEN 1 ELSE 0 END, r.run_id DESC;

/* ============================================================================
   07. CONTOH PERUBAHAN KONTEN RAW -> CLEAN
   Menampilkan artikel yang judul atau kontennya berubah setelah cleansing.
   EXCEPT membandingkan NULL secara setara sehingga aman untuk kolom nullable.
   Apabila RAW menyimpan beberapa versi artikel dalam satu run, hasil JOIN
   bisa berulang; cek id dan run_id saat menganalisis.
   ============================================================================ */
SELECT TOP (30)
    r.run_id, r.id,
    r.title AS raw_title, c.title AS clean_title,
    LEFT(r.content, 100) AS raw_content_preview,
    LEFT(c.content, 100) AS clean_content_preview
FROM Kumparan_DWH.stg.articles_raw AS r
INNER JOIN Kumparan_DWH.stg.articles_clean AS c
    ON c.run_id = r.run_id AND c.id = r.id
WHERE EXISTS (
    SELECT r.title, r.content
    EXCEPT
    SELECT c.title, c.content
)
ORDER BY r.run_id DESC, r.id;

/* ============================================================================
   08. PEMERIKSAAN DATA BERPOTENSI TIDAK VALID DI CLEAN
   Mendeteksi judul NULL/kosong, konten NULL/kosong, atau updated_at
   lebih awal daripada created_at. Hasil idealnya kosong jika semua
   kondisi tersebut memang termasuk aturan validasi pipeline.
   ============================================================================ */
SELECT TOP (30)
    c.run_id, c.id, c.title,
    LEFT(c.content, 100) AS content_preview,
    c.created_at, c.updated_at
FROM Kumparan_DWH.stg.articles_clean AS c
WHERE c.title IS NULL
   OR NULLIF(LTRIM(RTRIM(c.title)), N'') IS NULL
   OR c.content IS NULL
   OR NULLIF(LTRIM(RTRIM(c.content)), N'') IS NULL
   OR c.updated_at < c.created_at
ORDER BY c.run_id DESC, c.id;

/* ============================================================================
   09. VERIFIKASI CONTOH ARTIKEL CORE
   Memeriksa hasil akhir artikel yang sudah tersinkronisasi dari staging.
   deleted_at menandai soft delete; is_hard_deleted menandai hard delete
   yang terdeteksi oleh pipeline.
   ============================================================================ */
SELECT TOP (20)
    id, title, author_id, published_at,
    created_at, updated_at, deleted_at,
    is_hard_deleted, hard_deleted_detected_at,
    first_loaded_at, last_synced_at
FROM Kumparan_DWH.core.articles
ORDER BY id DESC;

/* ============================================================================
   10. VERIFIKASI RIWAYAT PERUBAHAN ARTIKEL
   Memastikan audit perubahan pada Core tercatat, misalnya artikel baru,
   pembaruan, dan penghapusan (sesuai change_type yang dihasilkan DAG).
   ============================================================================ */
SELECT TOP (30)
    change_id, article_id, run_id, change_type,
    old_title, new_title,
    old_updated_at, new_updated_at, changed_at
FROM Kumparan_DWH.core.article_change_history
ORDER BY change_id DESC;

/* ============================================================================
   11. VERIFIKASI CONTOH FACT PUBLIKASI
   Fact berisi artikel yang tercatat sebagai publikasi. Artikel yang
   belum diterbitkan atau tidak memenuhi filter Mart bisa tidak muncul.
   ============================================================================ */
SELECT TOP (30)
    article_id, author_id, date_key,
    published_at, article_count, refreshed_at
FROM Kumparan_DWH.mart.fact_article_publication
ORDER BY published_at DESC, article_id DESC;

/* ============================================================================
   12. REKONSILIASI FACT VS AGREGASI HARIAN
   Membandingkan jumlah publikasi dari Fact dengan agg_article_daily.
   FULL OUTER JOIN memperlihatkan tanggal yang hanya ada di salah satu sisi.
   difference = 0 menunjukkan nilai cocok untuk tanggal tersebut.
   ============================================================================ */
WITH fact_daily AS (
    SELECT date_key, SUM(CONVERT(BIGINT, article_count)) AS fact_articles
    FROM Kumparan_DWH.mart.fact_article_publication
    GROUP BY date_key
)
SELECT COALESCE(f.date_key, a.date_key) AS date_key,
       COALESCE(f.fact_articles, CONVERT(BIGINT, 0)) AS fact_articles,
       COALESCE(CONVERT(BIGINT, a.published_articles), CONVERT(BIGINT, 0)) AS aggregate_articles,
       COALESCE(f.fact_articles, CONVERT(BIGINT, 0))
           - COALESCE(CONVERT(BIGINT, a.published_articles), CONVERT(BIGINT, 0)) AS difference
FROM fact_daily AS f
FULL OUTER JOIN Kumparan_DWH.mart.agg_article_daily AS a
    ON a.date_key = f.date_key
ORDER BY date_key DESC;

/* ============================================================================
   13. REKONSILIASI FACT VS AGREGASI PENULIS PER HARI
   Memastikan jumlah artikel per penulis dan tanggal pada agregasi sesuai
   dengan rincian yang tersimpan di Fact.
   ============================================================================ */
WITH fact_author_daily AS (
    SELECT date_key, author_id,
           SUM(CONVERT(BIGINT, article_count)) AS fact_articles
    FROM Kumparan_DWH.mart.fact_article_publication
    GROUP BY date_key, author_id
)
SELECT COALESCE(f.date_key, a.date_key) AS date_key,
       COALESCE(f.author_id, a.author_id) AS author_id,
       COALESCE(f.fact_articles, CONVERT(BIGINT, 0)) AS fact_articles,
       COALESCE(CONVERT(BIGINT, a.published_articles), CONVERT(BIGINT, 0)) AS aggregate_articles,
       COALESCE(f.fact_articles, CONVERT(BIGINT, 0))
           - COALESCE(CONVERT(BIGINT, a.published_articles), CONVERT(BIGINT, 0)) AS difference
FROM fact_author_daily AS f
FULL OUTER JOIN Kumparan_DWH.mart.agg_author_daily AS a
    ON a.date_key = f.date_key AND a.author_id = f.author_id
ORDER BY date_key DESC, author_id;

/* ============================================================================
   SELESAI
   - Semua pemeriksaan di atas hanya membaca data.
   - Jika ada selisih, telusuri run_id di pipeline_runs, data_quality_results,
     dan watermark sebelum menyimpulkan ada masalah pada ETL.
   ============================================================================ */
