# Aturan Cleansing dan Validasi Data

Implementasi: [`etl/sql/cleansing_articles.sql`](../etl/sql/cleansing_articles.sql), dipanggil task `clean_and_validate_articles` pada [`dags/kumparan_articles_hourly.py`](../dags/kumparan_articles_hourly.py).

| Kondisi | Transformasi | Hasil |
|---|---|---|
| Judul memiliki NBSP/tab/newline/spasi tepi | Karakter whitespace tertentu diganti spasi lalu trim | CLEAN dengan judul ternormalisasi |
| Judul kosong setelah trim | Menjadi NULL | REJECTED `EMPTY_TITLE` |
| Konten memiliki spasi tepi | Trim tepi saja | CLEAN dengan konten ternormalisasi |
| Konten kosong setelah trim | Menjadi NULL | REJECTED `EMPTY_CONTENT` |
| `updated_at < created_at` | Tidak diperbaiki otomatis | REJECTED `INVALID_UPDATED_AT` |
| `published_at IS NULL` | Tidak diubah | CLEAN jika valid; tidak masuk fact |
| `deleted_at IS NOT NULL` | Tidak diubah | CLEAN jika valid; tidak masuk fact |

Prioritas alasan penolakan: EMPTY_TITLE, lalu EMPTY_CONTENT, lalu INVALID_UPDATED_AT. RAW dan Source tidak dimodifikasi. SQL runtime menerima tiga parameter `run_id` dari Python dan tidak dimaksudkan dijalankan manual.

**Validasi:** `RAW = CLEAN + REJECTED` per run bersifat kritis. Jumlah reject dicatat sebagai warning, bukan kegagalan otomatis. Karena konten kosong masuk REJECTED, pemeriksaan `cleansing_empty_content_warning` pada CLEAN umumnya bernilai nol. Hasil DQ dicatat di `etl.data_quality_results`.

**Ekspektasi dirty seed 200:** 140 valid dan 60 rejected bila seluruh 200 ID benar-benar diproses pada run yang sama dan tidak ada faktor lain. Ini belum membuktikan hasil runtime. Lihat [ETL_FLOW.md](ETL_FLOW.md) untuk alur lengkap dan batasannya.
