from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Konfigurasi Dasar
default_args = {
    'owner': 'rafif', #Ganti dengan nama yang menjalankan DAG ini (optional, tapi bagus untuk dokumentasi)
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Definisi Pipeline
with DAG(
    'job_search_medallion_pipeline_v1',
    default_args=default_args,
    description='Pipeline Scraper Glints & Jobstreet ke Postgres',
    schedule_interval='@daily', # Jalan otomatis tiap hari
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['final_project', 'scraper'],
) as dag:

    # Path folder script di dalam container Airflow
    SCRIPT_PATH = "/opt/airflow/dags/scripts"

    # --- TASK 1: BRONZE (Scraping & Ingest) ---
    # Kita jalankan Glints & Jobstreet barengan (paralel) biar cepet
    task_scrape_glints = BashOperator(
        task_id='scrape_glints_to_bronze',
        bash_command=f'python3 {SCRIPT_PATH}/scraper_glints.py'
    )

    task_scrape_jobstreet = BashOperator(
        task_id='scrape_jobstreet_to_bronze',
        bash_command=f'python3 {SCRIPT_PATH}/scraper_jobstreet.py'
    )

    # --- TASK 2: SILVER (Cleaning & Upsert) ---
    task_silver = BashOperator(
        task_id='clean_to_silver',
        bash_command=f'python3 {SCRIPT_PATH}/silver_layer.py'
    )

    # --- TASK 3: GOLD (Normalizing & Aggregating) ---
    task_gold = BashOperator(
        task_id='finalize_to_gold',
        bash_command=f'python3 {SCRIPT_PATH}/gold_layer.py'
    )

    # 3. MENGATUR ALUR KERJA (Flow)
    [task_scrape_glints, task_scrape_jobstreet] >> task_silver >> task_gold