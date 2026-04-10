import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine, text

# IMPORT DARI CONFIG.PY
from config import (
    MAX_PAGES, 
    SEARCH_KEYWORD, 
    DB_USER,    
    DB_PASS,    
    DB_HOST,    
    DB_PORT,    
    DB_NAME,
    SELENIUM_URL
)

def init_driver_jobstreet():
    print("Menghubungkan ke Selenium Grid di Docker untuk Jobstreet...")
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless') 
    options.add_argument('--window-size=1920,1080')
    
    # User agent terbaru biar gak dicurigai
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Anti-bot features
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    driver = webdriver.Remote(
        command_executor=SELENIUM_URL, 
        options=options
    )
    
    # --- PENTING: Hapus jejak bot ---
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver
    
def split_jobstreet_location(location_text):
    if not location_text:
        return "Indonesia", "Indonesia"
    
    parts = [p.strip() for p in location_text.split(',')]
    if len(parts) >= 2:
        return parts[0], parts[1] # "Jakarta Selatan, DKI Jakarta"
    
    return parts[0], parts[0]

def run_scraper_jobstreet():
    driver = init_driver_jobstreet()
    scraped_data = {} # Pakai dict biar otomatis handle duplikat di memori
    scrape_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    formatted_keyword = SEARCH_KEYWORD.replace(' ', '-')
    print(f"Mulai scraping Jobstreet untuk keyword: '{SEARCH_KEYWORD}', maksimal {MAX_PAGES} halaman...")

    try:
        for page in range(1, MAX_PAGES + 1):
            print(f"\n--- Jobstreet: Membuka Halaman {page} ---")
            
            url = f"https://id.jobstreet.com/id/jobs?keywords={SEARCH_KEYWORD}&page={page}"
            driver.get(url)
            
            time.sleep(7) 

            try:
                # --- REVISI 1: Targetkan mata bot ke "Overlay" yang kamu temukan ---
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-automation='job-list-item-link-overlay']"))
                )
            except:
                print(f"Data tidak ditemukan di halaman {page}. Ambil screenshot...")
                driver.save_screenshot("/opt/airflow/data/debug_jobstreet.png")
                break
            
            # Scroll untuk memicu render elemen bawah
            driver.execute_script("window.scrollTo(0, 800);")
            time.sleep(2)

            # --- REVISI 2: Ambil semua link overlay sebagai basis kartu ---
            overlays = driver.find_elements(By.CSS_SELECTOR, "a[data-automation='job-list-item-link-overlay']")
            print(f"Ditemukan {len(overlays)} lowongan di halaman {page}")

            for overlay in overlays:
                try:
                    # 1. ID & Link (Kita bisa langsung dapat ini dari elemen overlay!)
                    link = overlay.get_attribute("href").split('?')[0]
                    raw_id = link.rstrip('/').split('/')[-1] 
                    job_id = f"js-{raw_id}"

                    if job_id in scraped_data: continue

                    # 2. Cari "Bungkus" Utama (Naik ke parent terdekat yang membungkus teks)
                    try:
                        # Jobstreet biasanya pakai <article> atau <div> 3 level di atas overlay
                        card = overlay.find_element(By.XPATH, "./ancestor::article | ./ancestor::div[3]")
                    except:
                        # Fallback aman
                        card = overlay.find_element(By.XPATH, "../..")

                    # 3. Title (Kombinasi data-automation terbaru atau tag Heading 3)
                    try:
                        title = card.find_element(By.CSS_SELECTOR, "[data-automation='jobTitle'], h3").text.strip()
                    except:
                        title = "Judul Tidak Diketahui"

                    # 4. Company
                    try:
                        company = card.find_element(By.CSS_SELECTOR, "[data-automation='jobCompany']").text.strip()
                    except:
                        company = "Perusahaan Dirahasiakan"

                    # 5. Location
                    try:
                        full_loc = card.find_element(By.CSS_SELECTOR, "[data-automation='jobLocation']").text.strip()
                        city, province = split_jobstreet_location(full_loc)
                    except:
                        city, province = "Indonesia", "Indonesia"

                    # 6. Salary
                    try:
                        salary = card.find_element(By.CSS_SELECTOR, "[data-automation='jobSalary']").text.strip()
                    except:
                        salary = "Tidak Dicantumkan"

                    # 7. Posted Time
                    try:
                        posted_time = card.find_element(By.CSS_SELECTOR, "[data-automation='jobListingDate']").text.strip()
                    except:
                        posted_time = "Baru saja"

                    scraped_data[job_id] = {
                        "job_id": job_id,
                        "title": title,
                        "company": company,
                        "salary": salary,
                        "city": city,        
                        "province": province, 
                        "posted_time": posted_time,
                        "link": link,
                        "scrape_date": scrape_date,
                        "source": "Jobstreet"
                    }

                except Exception as e:
                    continue

            print(f"Total Unik Jobstreet: {len(scraped_data)}")

    except Exception as e:
        print(f"Error utama: {e}")
    finally:
        driver.quit()

    return list(scraped_data.values())

if __name__ == "__main__":
    # Jalankan Scraper
    data = run_scraper_jobstreet()
    
    if data:
        df = pd.DataFrame(data)
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            
            df.to_sql('jobs', engine, schema='bronze', if_exists='append', index=False, chunksize=1000, method='multi')
            print(f"✅ [BRONZE] {len(df)} data Jobstreet berhasil masuk ke DB.")
            
        except Exception as e:
            print(f"❌ [DB ERROR] {e}")
        
        os.makedirs("data", exist_ok=True)
        df.to_csv("data/raw_jobstreet_data.csv", index=False)
    else:
        print("❌ Tidak ada data Jobstreet yang didapat.")