from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

def run_gold():
    print("\n--- [GOLD LAYER] Building Normalized Schema + Aggregate Stats ---")
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    queries = [
        # 0. Inisialisasi Skema
        "CREATE SCHEMA IF NOT EXISTS gold;",

        # 1. TABEL DIM_COMPANIES (Master Perusahaan)
        "DROP TABLE IF EXISTS gold.dim_companies CASCADE;",
        """
        CREATE TABLE gold.dim_companies AS
        SELECT 
            MD5(company)::varchar(32) as company_id, 
            company as company_name
        FROM silver.jobs
        GROUP BY 1, 2;
        """,
        "ALTER TABLE gold.dim_companies ADD PRIMARY KEY (company_id);",

        # 2. TABEL FACT_JOBS (Data Utama - Partitioned)
        "DROP TABLE IF EXISTS gold.fact_jobs CASCADE;",
        """
        CREATE TABLE gold.fact_jobs (
            job_id VARCHAR(100),
            title TEXT,
            company_id VARCHAR(32),
            location TEXT,
            scrape_date TIMESTAMP,
            link TEXT,
            source VARCHAR(50),
            PRIMARY KEY (job_id, scrape_date)
        ) PARTITION BY RANGE (scrape_date);
        """,

        # Partisi April 2026
        """
        CREATE TABLE IF NOT EXISTS gold.fact_jobs_2026_04 
        PARTITION OF gold.fact_jobs
        FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
        """,

        # Ingest Fact Jobs dengan Logika Cleaning Lokasi
        """
        INSERT INTO gold.fact_jobs
        SELECT 
            job_id, title, MD5(company)::varchar(32),
            CASE 
                WHEN city = province THEN city 
                WHEN city ILIKE '%Region%' AND province ILIKE '%Singapore%' THEN 'Singapore'
                ELSE city || ', ' || province
            END as location,
            scrape_date, link, source
        FROM silver.jobs;
        """,

        # 3. TABEL DIM_SALARY (Enrichment Gaji)
        "DROP TABLE IF EXISTS gold.dim_salary CASCADE;",
        """
        CREATE TABLE gold.dim_salary AS
        SELECT 
            job_id,
            salary_min as min_salary,
            salary_max as max_salary,
            currency,
            CASE 
                WHEN salary_min IS NULL THEN 'N/A'
                WHEN currency = 'SGD' AND salary_min < 3000 THEN '< 3K SGD'
                WHEN currency = 'SGD' AND salary_min BETWEEN 3000 AND 5000 THEN '3K - 5K SGD'
                WHEN currency = 'SGD' AND salary_min > 5000 THEN '> 5K SGD'
                WHEN currency = 'IDR' AND salary_min < 10000000 THEN '< 10jt IDR'
                WHEN currency = 'IDR' AND salary_min BETWEEN 10000000 AND 20000000 THEN '10jt - 20jt IDR'
                ELSE '> 20jt IDR'
            END as salary_bracket
        FROM silver.jobs;
        """,

        # === TABEL AGREGAT (PENGGANTI DASHBOARD SEMENTARA) ===

        # 4. Statistik: Top Hiring Companies
        "DROP TABLE IF EXISTS gold.stats_companies;",
        """
        CREATE TABLE gold.stats_companies AS
        SELECT c.company_name, count(f.job_id) as total_vacancies, f.source
        FROM gold.fact_jobs f
        JOIN gold.dim_companies c ON f.company_id = c.company_id
        GROUP BY 1, 3 ORDER BY 2 DESC;
        """,

        # 5. Statistik: Market Salary per Title
        "DROP TABLE IF EXISTS gold.stats_salary;",
        """
        CREATE TABLE gold.stats_salary AS
        SELECT 
            f.title, 
            round(avg(s.min_salary)) as avg_min_salary,
            count(f.job_id) as sample_size,
            s.currency
        FROM gold.fact_jobs f
        JOIN gold.dim_salary s ON f.job_id = s.job_id
        WHERE s.min_salary IS NOT NULL
        GROUP BY 1, 4 HAVING count(*) > 1 ORDER BY 2 DESC;
        """,

        # 6. Statistik: Job Distribution by Location
        "DROP TABLE IF EXISTS gold.stats_location;",
        """
        CREATE TABLE gold.stats_location AS
        SELECT location, count(*) as job_count
        FROM gold.fact_jobs
        GROUP BY 1 ORDER BY 2 DESC;
        """
    ]

    with engine.begin() as conn:
        for q in queries:
            conn.execute(text(q))
            
    print("✅ Sukses: Star Schema & Tabel Agregat Dashboard siap!")

if __name__ == "__main__":
    run_gold()