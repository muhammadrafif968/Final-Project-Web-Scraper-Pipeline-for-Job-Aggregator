# Job Search Medallion Pipeline v1 (Glints and Jobstreet)

Pipeline ETL otomatis untuk mengumpulkan, mengolah, dan menyajikan data lowongan kerja dari berbagai platform (Glints & Jobstreet) menggunakan arsitektur **Medallion** di dalam lingkungan **Docker**.

## Overview
Project ini mengotomatisasi seluruh alur data dari tahap *scraping* (extraction) hingga tahap agregasi siap saji (Gold layer). Pipeline ini dikelola menggunakan **Apache Airflow** sebagai orchestrator dan **Selenium Grid** untuk menangani browser secara terdistribusi.

---

## Tech Stack
* **Orchestrator:** Apache Airflow
* **Environment:** Docker & Docker Compose
* **Database:** PostgreSQL (Bronze, Silver, Gold layers)
* **Scraping:** Python (Selenium WebDriver)
* **Browser Provider:** Selenium Grid (Chrome Nodes)
* **Analysis:** SQL / DBeaver

---

## Data Architecture (Medallion)
Data mengalir melalui tiga tahap utama untuk menjamin kualitas dan integritas:

1.  **Bronze Layer (Raw):** (Append)
    * Menyimpan data mentah hasil *scraping* tanpa modifikasi. 
    * Format: Tabel `bronze.jobs`.
    * Tujuan: Histori data dan audit.
2.  **Silver Layer (Cleaned):** (Upsert)
    * Data dibersihkan dari duplikat berdasarkan `job_id`.
    * Transformasi lokasi: Memisahkan string mentah menjadi kolom `City` dan `Province` yang granular yang terstandarisasi. 
    * Standarisasi format tanggal dan teks.
3.  **Gold Layer (Aggregated):** (Overwrite)
    * Data siap saji untuk analisis.
    * Berisi tabel agregasi (misal: jumlah loker per kota, tren gaji).
    * Otomatis terupdate setiap kali pipeline berjalan.

---

## How to Run

1.  **Clone Repository**
2.  **Setup Environment:** Isi file `.env` dengan kredensial database serta email (disarankan dummy).
3.  **Start Containers:**
    ```bash
    docker-compose up -d
    ```
4.  **Access Airflow UI:** Buka `localhost:8080` dan aktifkan DAG.
5.  **Check Data:** Pantau tabel di PostgreSQL menggunakan DBeaver.

---

## Challenges & Known Issues
* **Anti-Bot Protection:** Platform memiliki proteksi CAPTCHA dinamis. Project dilengkapi sistem monitoring screenshot.

---

## Author
**Muhammad Rafif** - Data Engineering Enthusiast