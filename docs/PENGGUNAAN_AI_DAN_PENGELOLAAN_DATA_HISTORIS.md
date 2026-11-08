# Penggunaan AI dalam Pengembangan Project

Dalam pengerjaan Data Engineer Assessment ini, saya memanfaatkan ChatGPT sebagai salah satu referensi untuk mendukung proses perencanaan, pengembangan, dan evaluasi implementasi ETL. Penggunaan AI berfokus pada eksplorasi pendekatan teknis, referensi penulisan kode, serta penyelesaian kendala yang ditemukan selama pengembangan.

Adapun penggunaan AI dalam project ini meliputi:

1. **Perencanaan Arsitektur dan Alur ETL**

   Saya menggunakan AI sebagai bahan referensi dan pertimbangan dalam menentukan pendekatan pengolahan data, mulai dari proses ekstraksi hingga penyediaan data untuk kebutuhan analisis. Pembahasan mencakup penggunaan Apache Airflow sebagai scheduler, penerapan lapisan Staging, Core, dan Data Mart, serta mekanisme incremental loading, watermark, dan penanganan data yang mengalami perubahan maupun penghapusan.

2. **Referensi Implementasi dan Pengembangan Kode**

   AI digunakan untuk membantu mencari referensi penulisan query SQL dan kode Python, memahami alternatif implementasi, serta menelusuri penyebab kesalahan yang muncul selama proses pengembangan. Referensi tersebut kemudian digunakan sebagai bahan penyesuaian terhadap kebutuhan dan struktur project yang dibangun.

3. **Penyediaan Data untuk Pengujian**

   Karena assessment tidak menyediakan akses langsung ke database operasional Kumparan, saya menyiapkan sekitar 12.000 data artikel dummy dengan rentang tanggal historis mulai tahun 2016. AI digunakan sebagai referensi dalam penyusunan script pembangkitan data tersebut.

   Data dummy digunakan untuk menguji alur pemrosesan ETL, termasuk pemuatan data awal, penambahan artikel baru, perubahan data, dan penghapusan artikel. Seluruh data tersebut bersifat simulasi dan bukan merupakan data produksi Kumparan.

4. **Dokumentasi dan Persiapan Presentasi**

   AI juga dimanfaatkan untuk membantu menyusun dokumentasi teknis, merapikan penjelasan alur sistem, serta mempersiapkan materi pendukung yang digunakan untuk menjelaskan implementasi project.

Penggunaan AI dalam project ini merupakan bagian dari proses pencarian referensi dan pendukung pengembangan. Saya tetap bertanggung jawab terhadap keputusan teknis, implementasi, pengujian, serta pemahaman atas keseluruhan sistem yang diserahkan.

---

# Pendekatan Pengolahan Data Historis

Salah satu kondisi yang dijelaskan dalam assessment adalah database operasional Kumparan yang telah menyimpan data artikel sejak tahun 2016. Kondisi tersebut menjadi pertimbangan dalam menentukan pendekatan pemuatan data awal dan proses sinkronisasi berikutnya.

Dalam project ini, saya menggunakan data dummy untuk merepresentasikan kondisi database yang sudah memiliki data historis. Pendekatan tersebut dipilih karena tidak tersedia akses ke database produksi maupun dataset asli Kumparan.

Pada implementasi di lingkungan produksi, data historis tidak perlu dibuat kembali. Proses ETL akan mengambil data yang sudah tersimpan di database sumber melalui mekanisme initial load, kemudian memprosesnya ke dalam Data Warehouse.

Setelah pemuatan data awal selesai, pipeline dijadwalkan berjalan setiap satu jam untuk mengambil data baru maupun data yang mengalami perubahan. Proses incremental loading memanfaatkan kolom `updated_at` dan watermark untuk menentukan data yang perlu diproses pada eksekusi berikutnya.

Selain perubahan data, pipeline juga dilengkapi mekanisme pendeteksian hard delete untuk mengidentifikasi artikel yang telah dihapus secara permanen dari database sumber. Informasi tersebut digunakan untuk menjaga kesesuaian status data antara database operasional dan Data Warehouse.

## Pertimbangan untuk Implementasi Produksi

Penggunaan sekitar 12.200 data dummy dalam assessment ini ditujukan untuk menguji fungsionalitas dan alur pemrosesan data, bukan sebagai pengukuran performa sistem pada skala produksi.

Apabila diterapkan pada lingkungan dengan volume data yang lebih besar, terdapat beberapa aspek yang perlu dipertimbangkan, antara lain:

1. **Pemuatan data historis secara bertahap**, agar proses initial load tidak memberikan beban berlebihan pada database sumber.
2. **Pengujian performa**, untuk mengetahui kemampuan pipeline dalam menangani peningkatan volume data dan waktu pemrosesan.
3. **Konsistensi data**, terutama ketika terjadi penambahan, perubahan, atau penghapusan artikel selama proses ETL berlangsung.
4. **Optimalisasi pembaruan Data Mart**, agar pemrosesan data dapat dilakukan secara lebih efisien tanpa harus membangun ulang seluruh hasil analisis pada setiap eksekusi.
5. **Keamanan dan monitoring**, mencakup pengelolaan akses database, pemantauan eksekusi pipeline, pencatatan kegagalan, dan pemeriksaan kualitas data.

Dengan pendekatan tersebut, project ini tidak hanya menunjukkan proses pemindahan data dari database sumber ke Data Warehouse, tetapi juga mempertimbangkan keberlanjutan proses, konsistensi data, dan kesiapan pengembangan sistem untuk kebutuhan operasional yang lebih besar.