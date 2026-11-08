/*
============================================================
  KUMPARAN ETL - GENERATE DATA ARTIKEL UNTUK PENGUJIAN
============================================================

TUJUAN:
Mengisi tabel Kumparan_Source.dbo.articles dengan maksimal
12.000 artikel sintetis sebagai data sumber pipeline ETL.

DATA YANG DIBUAT:
- ID artikel
- Judul dan isi artikel
- Tanggal publikasi
- ID penulis
- Tanggal pembuatan
- Tanggal terakhir diperbarui
- Tanggal penghapusan (jika ada)

KEGUNAAN:
1. Menguji proses Extract, Transform, dan Load (ETL).
2. Menguji incremental loading menggunakan watermark.
3. Menguji deteksi perubahan data (CDC).
4. Menguji penanganan artikel yang ditandai dihapus.
5. Menyediakan data historis untuk pengujian DWH.

CATATAN:
- Data ini bukan artikel asli Kumparan.
- Data dibuat secara sintetis untuk assessment/demo.
- Artikel dengan ID yang sudah ada tidak dimasukkan ulang.
- Jalankan pada database pengujian, bukan produksi.
============================================================
*/


-- =========================================================
-- STEP 01: PERIKSA APAKAH KOLOM ID MENGGUNAKAN IDENTITY
-- =========================================================

-- IDENTITY adalah fitur SQL Server untuk membuat nomor ID
-- secara otomatis ketika data baru dimasukkan.

-- Karena query ini mengisi ID artikel secara manual,
-- kita perlu mengetahui apakah IDENTITY_INSERT diperlukan.

DECLARE @source_identity BIT =
    CASE
        WHEN COLUMNPROPERTY(
            OBJECT_ID(N'Kumparan_Source.dbo.articles'),
            N'id',
            'IsIdentity'
        ) = 1
        THEN 1
        ELSE 0
    END;


-- Jika kolom id menggunakan IDENTITY,
-- izinkan pengisian ID secara manual.

IF @source_identity = 1
    SET IDENTITY_INSERT Kumparan_Source.dbo.articles ON;


-- =========================================================
-- STEP 02: MEMBUAT 12.000 NOMOR URUT ARTIKEL
-- =========================================================

-- CTE (Common Table Expression) bernama 'n'
-- digunakan untuk menghasilkan nomor urut 1 sampai 12.000.

-- master.sys.all_objects adalah katalog objek SQL Server.
-- CROSS JOIN memperbanyak kombinasi baris agar tersedia
-- cukup data untuk menghasilkan 12.000 nomor.

WITH n AS (
    SELECT TOP (12000) ROW_NUMBER() OVER ( ORDER BY a.object_id, b.object_id ) AS rn
    FROM master.sys.all_objects a
    CROSS JOIN master.sys.all_objects b
),


-- =========================================================
-- STEP 03: MEMBUAT DATA ARTIKEL SINTETIS
-- =========================================================

-- CTE 'sample' menyiapkan informasi dasar artikel:
-- nomor artikel, tanggal dibuat, kategori, dan kota.

sample AS (
    SELECT
        rn,

        -- Membuat variasi tanggal artikel mulai 1 Januari 2016.
        -- Rumus modulo (%) menghasilkan sebaran tanggal
        -- dengan pola yang dapat diulang.

        DATEADD(
            DAY,
            CAST((rn * 37) % 3950 AS INT),
            CAST('2016-01-01' AS DATETIME2(3))
        ) AS created_time,


        -- Menentukan kategori berdasarkan sisa pembagian
        -- nomor artikel dengan angka 8.
        -- Hasilnya terdapat 8 kategori artikel.

        CASE rn % 8
            WHEN 0 THEN N'Ekonomi'
            WHEN 1 THEN N'Teknologi'
            WHEN 2 THEN N'Olahraga'
            WHEN 3 THEN N'Politik'
            WHEN 4 THEN N'Bisnis'
            WHEN 5 THEN N'Pendidikan'
            WHEN 6 THEN N'Gaya Hidup'
            ELSE N'Wisata'
        END AS category,


        -- Menentukan kota berdasarkan sisa pembagian
        -- nomor artikel dengan angka 6.
        -- Hasilnya terdapat 6 variasi kota.

        CASE rn % 6
            WHEN 0 THEN N'Jakarta'
            WHEN 1 THEN N'Bandung'
            WHEN 2 THEN N'Surabaya'
            WHEN 3 THEN N'Medan'
            WHEN 4 THEN N'Yogyakarta'
            ELSE N'Makassar'
        END AS city

    FROM n
)


-- =========================================================
-- STEP 04: INSERT DATA KE TABEL SUMBER
-- =========================================================

-- Memasukkan data sintetis ke tabel articles.
-- Data dari CTE 'sample' diubah menjadi format artikel.

INSERT INTO Kumparan_Source.dbo.articles (
    id,
    title,
    content,
    published_at,
    author_id,
    created_at,
    updated_at,
    deleted_at
)

SELECT
    -- Nomor unik artikel dari 1 sampai 12.000.
    s.rn,


    -- Membuat judul artikel otomatis.
    -- Contoh:
    -- Ekonomi: Perkembangan terbaru di Jakarta — laporan 8

    CONCAT(
        s.category,
        N': Perkembangan terbaru di ',
        s.city,
        N' — laporan ',
        s.rn
    ),


    -- Membuat isi artikel berdasarkan kategori dan kota.
    -- Semua artikel merupakan data sintetis.

    CONCAT(
        N'Perkembangan ',
        LOWER(s.category),
        N' di ',
        s.city,
        N' menunjukkan berbagai dinamika terbaru. ',
        N'Sejumlah perkembangan dan aktivitas di sektor ini ',
        N'menjadi perhatian dalam beberapa waktu terakhir. ',
        N'Artikel sintetis untuk assessment.'
    ),


    -- published_at:
    -- Sebagian artikel belum dipublikasikan (NULL).
    -- Setiap artikel dengan rn kelipatan 11 akan NULL.
    -- Artikel lainnya dipublikasikan 2 jam setelah dibuat.

    CASE
        WHEN s.rn % 11 = 0 THEN NULL
        ELSE DATEADD(HOUR, 2, s.created_time)
    END,


    -- author_id:
    -- Membuat variasi 150 ID penulis.
    -- Nilai yang dihasilkan berada pada rentang 1-150.

    1 + (s.rn % 150),


    -- created_at:
    -- Waktu artikel pertama kali dibuat.

    s.created_time,


    -- updated_at:
    -- Setiap artikel dengan rn kelipatan 7
    -- dianggap pernah diperbarui 2 hari setelah dibuat.
    -- Artikel lainnya menggunakan waktu pembuatan.

    CASE
        WHEN s.rn % 7 = 0
            THEN DATEADD(DAY, 2, s.created_time)
        ELSE s.created_time
    END,


    -- deleted_at:
    -- Setiap artikel dengan rn kelipatan 97
    -- diberi tanggal penghapusan 3 hari setelah dibuat.
    --
    -- Jika NULL, artikel tidak ditandai dihapus.
    -- Jika berisi tanggal, artikel dianggap soft-deleted.

    CASE
        WHEN s.rn % 97 = 0
            THEN DATEADD(DAY, 3, s.created_time)
        ELSE NULL
    END

FROM sample s


-- =========================================================
-- STEP 05: MENCEGAH INSERT DATA DENGAN ID YANG SAMA
-- =========================================================

-- NOT EXISTS memastikan artikel tidak dimasukkan ulang
-- jika ID tersebut sudah tersedia di tabel sumber.
--
-- Dengan demikian, menjalankan query kembali tidak akan
-- menambahkan ulang ID artikel yang sudah ada.

WHERE NOT EXISTS (
    SELECT 1
    FROM Kumparan_Source.dbo.articles x
    WHERE x.id = s.rn
);


-- =========================================================
-- STEP 06: KEMBALIKAN PENGATURAN IDENTITY_INSERT
-- =========================================================

-- Setelah INSERT selesai, nonaktifkan kembali izin
-- pengisian ID manual jika sebelumnya diaktifkan.

IF @source_identity = 1
    SET IDENTITY_INSERT Kumparan_Source.dbo.articles OFF;


-- =========================================================
-- STEP 07: VERIFIKASI JUMLAH ARTIKEL
-- =========================================================

-- Menghitung seluruh artikel yang ada pada tabel sumber.
-- Hasilnya digunakan untuk memastikan data sudah masuk.
--
-- Perhatian:
-- COUNT(*) menghitung seluruh isi tabel, termasuk data
-- yang mungkin sudah ada sebelum query dijalankan.
-- Jadi hasilnya tidak harus tepat 12.000.

SELECT COUNT(*) AS total_articles
FROM Kumparan_Source.dbo.articles;


/*
============================================================
KESIMPULAN

Query ini menyiapkan data artikel sintetis untuk menguji
pipeline ETL Kumparan.

Variasi data mencakup:
- Artikel baru
- Artikel belum dipublikasikan
- Artikel dengan timestamp perubahan
- Artikel dengan penanda soft delete

Data tersebut dapat digunakan untuk pengujian incremental
loading, CDC, staging, core, dan data warehouse.

PENTING:
Query ini hanya menyiapkan data pengujian.
Query ini tidak menjalankan pipeline Airflow dan tidak
secara langsung membuktikan bahwa CDC sudah bekerja.
============================================================
*/
