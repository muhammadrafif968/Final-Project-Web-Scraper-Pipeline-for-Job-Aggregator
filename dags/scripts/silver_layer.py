from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PORT, DB_USER, DB_PASS, DB_NAME

def run_silver():
    print("\n--- [SILVER LAYER] Cleaning & Parsing to 'silver' schema ---")
    
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Query ini akan membuat skema, tabel, dan melakukan UPSERT dari bronze ke silver
    query = r"""
    CREATE SCHEMA IF NOT EXISTS silver;

    CREATE TABLE IF NOT EXISTS silver.jobs (
        job_id VARCHAR(100) PRIMARY KEY,
        title TEXT, company TEXT, salary TEXT,
        salary_min NUMERIC, 
        salary_max NUMERIC,
        currency VARCHAR(10),
        city TEXT, province TEXT, posted_time TEXT,
        link TEXT, scrape_date TIMESTAMP, source VARCHAR(50)
    );

    INSERT INTO silver.jobs
    SELECT DISTINCT ON (job_id)
        job_id, title, 
        COALESCE(company, 'Tidak Diketahui'), 
        COALESCE(salary, 'Tidak Dicantumkan'),
        
        -- LOGIKA PARSING SALARY MIN
        CASE 
            WHEN salary ~ '[0-9]' THEN 
                (
                    -- 1. Hapus titik (.) pemisah ribuan
                    -- 2. Ganti koma (,) jadi titik desimal (.)
                    -- 3. Ekstrak angkanya, mau ada desimal atau utuh
                    SUBSTRING(regexp_replace(regexp_replace(salary, '\.', '', 'g'), ',', '.') FROM '([0-9]+\.?[0-9]*)')::numeric 
                    * CASE 
                        WHEN salary ILIKE '%jt%' OR salary ILIKE '%juta%' THEN 1000000 
                        WHEN salary ILIKE '%k%' THEN 1000 
                        ELSE 1 
                      END
                )
            ELSE NULL 
        END as salary_min,

        -- LOGIKA PARSING SALARY MAX
        CASE 
            WHEN salary ~ '[-–]' THEN 
                (
                    SUBSTRING(
                        split_part(
                            regexp_replace(regexp_replace(regexp_replace(salary, '\.', '', 'g'), ',', '.'), '–', '-'), 
                            '-', 2
                        ) 
                    FROM '([0-9]+\.?[0-9]*)')::numeric 
                    * CASE 
                        WHEN salary ILIKE '%jt%' OR salary ILIKE '%juta%' THEN 1000000 
                        WHEN salary ILIKE '%k%' THEN 1000 
                        ELSE 1 
                      END
                )
            ELSE NULL 
        END as salary_max,

        -- LOGIKA CURRENCY (Biar nggak SGD semua)
        CASE 
            WHEN salary ILIKE '%$%' OR salary ILIKE '%SGD%' THEN 'SGD'
            WHEN salary ILIKE '%Rp%' OR salary ILIKE '%IDR%' THEN 'IDR'
            ELSE 'Unknown'
        END as currency,

        COALESCE(city, 'Tidak Spesifik'), 
        COALESCE(province, 'Tidak Spesifik'), 
        
        -- LOGIKA STANDARISASI WAKTU POSTING (posted_time)
        CASE 
            WHEN LOWER(posted_time) ILIKE '%more than thirty%' OR posted_time ~ '30\+' THEN 'more than 30 days ago'
            WHEN posted_time ~ '^([0-9]+)d ago' THEN regexp_replace(posted_time, '^([0-9]+)d ago', '\1 days ago')
            WHEN posted_time ~ '^([0-9]+)h ago' THEN regexp_replace(posted_time, '^([0-9]+)h ago', '\1 hours ago')
            WHEN posted_time ~ '^([0-9]+)m ago' THEN regexp_replace(posted_time, '^([0-9]+)m ago', '\1 months ago')
            ELSE 
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                    LOWER(TRIM(REPLACE(posted_time, 'Listed', '')))
                , 'thirty', '30')
                , 'twenty nine', '29'), 'twenty eight', '28'), 'twenty seven', '27'), 'twenty six', '26'), 'twenty five', '25')
                , 'twenty four', '24'), 'twenty three', '23'), 'twenty two', '22'), 'twenty one', '21'), 'twenty', '20')
                , 'nineteen', '19'), 'eighteen', '18'), 'seventeen', '17'), 'sixteen', '16'), 'fifteen', '15')
                , 'fourteen', '14'), 'thirteen', '13'), 'twelve', '12'), 'eleven', '11'), 'ten', '10')
                , 'nine', '9'), 'eight', '8'), 'seven', '7'), 'six', '6'), 'five', '5')
                , 'four', '4'), 'three', '3'), 'two', '2'), 'one', '1')
        END as posted_time,
        
        link, 
        scrape_date::TIMESTAMP, 
        source
    FROM bronze.jobs
    ORDER BY job_id, scrape_date DESC
    ON CONFLICT (job_id) DO UPDATE SET 
        scrape_date = EXCLUDED.scrape_date,
        salary_min = EXCLUDED.salary_min,
        salary_max = EXCLUDED.salary_max,
        currency = EXCLUDED.currency;
        
    -- Hapus data yang lebih dari 30 hari
    DELETE FROM silver.jobs WHERE posted_time = 'more than 30 days ago';
    """

    with engine.begin() as conn:
        raw_conn = conn.connection
        cursor = raw_conn.cursor()
        for statement in query.strip().split(';'):
            stmt = statement.strip()
            if stmt:
                cursor.execute(stmt)
        cursor.close()
        
    print("✅ Sukses: Data bersih dan ter-parsing mendarat di silver.jobs")

if __name__ == "__main__":
    run_silver()