# 🛠 Panduan Instalasi & Menjalankan Kumparan ELT Pipeline

> Panduan operasional terpisah dari [README utama](../README.md). Contoh konfigurasi harus disesuaikan dengan environment lokal dan `docker-compose.yml` aktual.

## 1. Prasyarat

- Docker Desktop dan Docker Compose aktif.
- Microsoft SQL Server dapat diakses pada port `1433`.
- DBeaver untuk mengelola dan memverifikasi database.
- File project: `Dockerfile`, `docker-compose.yml`, `.env.example`, `dags/`, dan `sql/`.

```bash
docker --version
docker compose version
docker info
```

## 2. Siapkan environment

Masuk ke root project (sesuaikan path):

```bash
cd "/Users/a2345/Downloads/kumparan-etl"
```

Jika belum ada `.env`:

```bash
cp .env.example .env
```

Contoh nilai yang perlu dikonfigurasi (jangan commit `.env`):

```dotenv
MSSQL_HOST=host.docker.internal
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PASSWORD=GANTI_PASSWORD_SQL_SERVER
MSSQL_SOURCE_DB=Kumparan_Source
MSSQL_DWH_DB=Kumparan_DWH
AIRFLOW_DB_PASSWORD=GANTI_PASSWORD_POSTGRES
AIRFLOW_ADMIN_PASSWORD=GANTI_PASSWORD_ADMIN
```

Untuk SQL Server yang berjalan di host Mac, container Docker menggunakan `host.docker.internal`; koneksi DBeaver dari Mac biasanya `localhost`.

**Penting:** Periksa apakah Compose juga memerlukan `AIRFLOW_DB_PASSWORD_URLENCODED`; nilainya harus berupa password PostgreSQL yang di-URL-encode jika dipakai di connection URI.

## 3. Siapkan database

Hubungkan DBeaver ke SQL Server, lalu jalankan `script_sql/01_database_setup.sql` sesuai urutan database → schema → tabel. Pastikan `Kumparan_Source` dan `Kumparan_DWH` tersedia.

Script `script_sql/02_insert_data_articles.sql` **hanya untuk source kosong**. Jangan menjalankannya ulang pada data assessment yang sudah berisi 12.000 artikel.

```sql
SELECT COUNT(*) AS total_articles FROM Kumparan_Source.dbo.articles;
```

## 4. Jalankan Docker dan Airflow

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps -a
```

Ekspektasi: `airflow-db` healthy, `airflow-init` exited (0), `airflow-webserver` dan `airflow-scheduler` running. Sesuaikan dengan nama service Compose aktual.

## 5. Login Airflow

Buka `http://localhost:8080`, atau port host lain yang tertera pada `docker-compose.yml` (misalnya mapping `8081:8080` berarti `http://localhost:8081`). Username biasanya `admin`; password mengikuti `AIRFLOW_ADMIN_PASSWORD` saat user dibuat.

```bash
docker compose logs -f airflow-webserver
```

Jika perlu reset password (dan variabel tersebut tersedia di container):

```bash
docker compose exec airflow-webserver bash -c 'airflow users reset-password --username admin --password "$AIRFLOW_ADMIN_PASSWORD"'
```

## 6. Uji koneksi SQL Server dari container

```bash
docker compose exec airflow-scheduler python -c "import os,pymssql; c=pymssql.connect(server=os.environ['MSSQL_HOST'],port=int(os.environ['MSSQL_PORT']),user=os.environ['MSSQL_USER'],password=os.environ['MSSQL_PASSWORD'],database=os.environ['MSSQL_SOURCE_DB'],login_timeout=10); print('SQL Server connected successfully'); c.close()"
```

## 7. Jalankan pipeline

```bash
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Di dashboard Airflow, cari `kumparan_articles_hourly`, aktifkan jika paused, lalu klik **Trigger DAG**. Pantau seluruh task hingga selesai. Jadwal otomatis DAG adalah `0 * * * *` (setiap jam).

## 8. Verifikasi hasil di DBeaver

```sql
SELECT COUNT(*) AS total_source FROM Kumparan_Source.dbo.articles;
SELECT COUNT(*) AS total_core FROM Kumparan_DWH.core.articles;
SELECT TOP (20) * FROM Kumparan_DWH.etl.pipeline_runs ORDER BY run_id DESC;
SELECT * FROM Kumparan_DWH.etl.watermarks;
SELECT TOP (20) * FROM Kumparan_DWH.etl.data_quality_results ORDER BY checked_at DESC;
SELECT TOP (20) * FROM Kumparan_DWH.core.article_change_history;
```

Status DAG `SUCCESS` tidak otomatis membuktikan seluruh layer sudah benar. Periksa juga staging, mart, perubahan historis, dan hasil data quality.

## 9. Log dan troubleshooting

```bash
docker compose logs --tail=150 airflow-init
docker compose logs --tail=100 airflow-webserver
docker compose logs --tail=100 airflow-scheduler
docker compose logs -f airflow-scheduler
```

**DAG tidak muncul:** periksa `airflow dags list-import-errors` dan mount folder `dags/`.

**Port bentrok:**

```bash
lsof -nP -iTCP:8080 -sTCP:LISTEN
```

**SQL Server connection refused:** periksa service SQL Server, port `1433`, autentikasi, dan akses dari container.

**PostgreSQL password authentication failed:** pastikan `AIRFLOW_DB_PASSWORD`, nilai URL-encoded jika digunakan, dan password role yang tersimpan pada volume PostgreSQL konsisten. Mengubah `.env` saja tidak otomatis mengganti password role database yang sudah ada.

## 10. Restart tanpa menghapus data

```bash
docker compose stop
docker compose start
# atau
docker compose up -d
```

`docker compose down` mempertahankan named volumes secara default, sedangkan **`docker compose down -v` menghapus named volumes**. Hindari `-v` agar metadata Airflow tidak hilang. Mengganti file DAG tidak menghapus data SQL Server.

## 11. Pengujian mutasi

Gunakan `script_sql/05_seed_dirty_articles.sql` hanya pada data dummy terkontrol. Uji INSERT, UPDATE, soft delete, restore, dan hard delete; trigger DAG dan bandingkan perubahan pada core, history, dan mart. Jangan menganggap semua skenario sudah lulus tanpa bukti query dan hasil eksekusi.


## Mengaktifkan Data Cleansing

1. Backup database dan DAG sebelumnya.
2. Jalankan `script_sql/01_database_setup.sql` sekali melalui DBeaver di instance SQL Server yang sama.
3. Pastikan file `etl/sql/cleansing_articles.sql` tersedia di folder `etl/sql/` dan mount `/opt/airflow/etl/sql`.
4. Jalankan `docker compose exec airflow-scheduler airflow dags list-import-errors`, lalu trigger DAG dari UI.
5. Verifikasi: `SELECT TOP (20) * FROM Kumparan_DWH.etl.data_quality_results WHERE check_name LIKE 'cleansing_%' ORDER BY checked_at DESC;`
6. Periksa snapshot: `SELECT TOP (20) * FROM Kumparan_DWH.stg.articles_clean ORDER BY run_id DESC,id;`

Data source, raw staging, dan core lama tidak dihapus oleh migrasi. Artikel invalid diarahkan ke REJECTED; task gagal hanya bila pemeriksaan kritis gagal.


## Setup Cleansing

Jalankan `script_sql/01_database_setup.sql` di DBeaver sebelum trigger DAG. Untuk verifikasi jalankan `script_sql/04_etl_validation.sql`. Detail: `CLEANSING.md`.


## Setup cleansing terbaru

1. Backup SQL Server DWH dan DAG sebelumnya.
2. Jalankan `script_sql/01_database_setup.sql` di DBeaver.
3. Pastikan `.env` sesuai `.env.example` dan jangan commit password.
4. `docker compose config --quiet` lalu `docker compose up -d --build`.
5. `docker compose exec airflow-scheduler airflow dags list-import-errors`.
6. Trigger `kumparan_articles_hourly` dan verifikasi 9 task di Airflow.
7. Jalankan `script_sql/04_etl_validation.sql` dan `script_sql/04_etl_validation.sql`.
8. Uji INSERT, UPDATE, soft delete, hard delete hanya di database dummy cadangan.

**Jangan jalankan `docker compose down -v`** karena menghapus volume metadata Airflow.


## Koreksi implementasi terbaru (8 November 2026)

Gunakan [`ETL_FLOW.md`](ETL_FLOW.md) sebagai acuan urutan sembilan task dan status implementasi. SQL setup sudah menyertakan `stg.articles_clean`, `stg.articles_rejected`, dan `etl.recovery_state`. Artikel invalid dipisahkan ke REJECTED dan tidak otomatis menggagalkan DAG; kegagalan kritis terjadi pada ketidaksesuaian RAW vs CLEAN+REJECTED atau DQ lain. Query validasi ada di `script_sql/04_etl_validation.sql`. `script_sql/05_seed_dirty_articles.sql` hanya untuk menambahkan data sintetis testing pada rentang ID 12001–12200; jangan digunakan pada data produksi. Hasil uji runtime belum diverifikasi hanya dari berkas kode.
