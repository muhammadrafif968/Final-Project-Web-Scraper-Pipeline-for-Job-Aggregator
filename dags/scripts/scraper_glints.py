import time
import os
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from config import (
    MAX_PAGES, 
    SEARCH_KEYWORD, 
    GLINTS_EMAIL, 
    GLINTS_PASSWORD,
    DB_USER,    
    DB_PASS,    
    DB_HOST,    
    DB_PORT,    
    DB_NAME,
    SELENIUM_URL     
)

def init_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    
    # User Agent agar terdeteksi sebagai browser asli (Anti-Bot)
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Remote(command_executor=SELENIUM_URL, options=options)
    
    # Hapus jejak webdriver di navigator
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def login_glints(driver):
    login_url = "https://glints.com/id/login"
    print(f"Mencoba login Glints ID: {GLINTS_EMAIL}...")
    driver.get(login_url)
    
    try:
        # 1. KLIK: "Masuk dengan Email"
        print("-> Mengetuk pintu 'Masuk dengan Email'...")
        gatekeeper_btn = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Masuk dengan Email')]"))
        )
        driver.execute_script("arguments[0].click();", gatekeeper_btn)
        
        time.sleep(2)

        # 2. ISI EMAIL
        email_input = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='email']"))
        )
        email_input.clear()
        email_input.send_keys(GLINTS_EMAIL)
        print("-> Email terisi...")
        
        # 3. ISI PASSWORD & TEKAN ENTER
        pass_input = driver.find_element(By.CSS_SELECTOR, "input[type='password']")
        pass_input.clear()
        pass_input.send_keys(GLINTS_PASSWORD)
        pass_input.send_keys(Keys.ENTER) # Trik 1: Paksa login pakai keyboard
        print("-> Password terisi & Enter ditekan...")
        
        time.sleep(1)

        # 4. KLIK TOMBOL MASUK FINAL (Pakai data-cy sesuai temuan kamu)
        try:
            login_btn = driver.find_element(By.CSS_SELECTOR, "button[data-cy='submit_btn_login']")
            driver.execute_script("arguments[0].click();", login_btn) 
            print("-> Klik tombol login (data-cy) berhasil!")
        except:
            print("-> Tombol mungkin sudah terproses lewat Enter.")
        
        # 5. VERIFIKASI: Tunggu sampai URL bukan lagi halaman login
        WebDriverWait(driver, 20).until(
            lambda d: "login" not in d.current_url.lower()
        )
        
        print("✅ Login Berhasil! Sesi aman.")
        time.sleep(5) 
        
    except Exception as e:
        print(f"❌ Login Gagal: {e}")
        save_path = "/opt/airflow/data/error_login_gatekeeper.png" 
        driver.save_screenshot(save_path)
        print(f"-> Screenshot error disimpan ke folder /data")

def run_scraper():
    driver = init_driver()
    login_glints(driver) 
    
    scraped_data = {}
    scrape_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    card_xpath = "//div[contains(@class, 'CompactOpportunityCard')]"

    try:
        for page in range(1, MAX_PAGES + 1):
            print(f"\n--- Mengambil Halaman {page} ---")
            # URL Indonesia agar sinkron dengan City/Province Indonesia
            url = f"https://glints.com/id/opportunities/jobs/explore?keyword={SEARCH_KEYWORD}&page={page}"
            driver.get(url)
            
            # Waktu tunggu render website (SPA)
            time.sleep(5) 

            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, card_xpath)))
            except:
                print(f"Data tidak ditemukan di halaman {page}. Berhenti.")
                break
            
            # Scroll untuk memicu lazy loading
            driver.execute_script("window.scrollTo(0, 1000);")
            time.sleep(3)
            
            job_cards = driver.find_elements(By.XPATH, card_xpath)
            print(f"Ditemukan {len(job_cards)} lowongan di halaman {page}.")

            for card in job_cards:
                try:
                    # Title & Link
                    title_elem = card.find_element(By.XPATH, ".//h2[contains(@class, 'JobTitle')]/a")
                    link = title_elem.get_attribute("href").split('?')[0]
                    
                    # Job ID
                    raw_id = link.rstrip('/').split('/')[-1]
                    job_id = f"gl-{raw_id}"

                    if job_id in scraped_data: continue

                    # Company & Salary
                    company_elem = card.find_elements(By.XPATH, ".//a[contains(@class, 'CompanyLink')]")
                    company = company_elem[0].get_attribute("textContent").strip() if company_elem else "Unknown"

                    salary_elem = card.find_elements(By.XPATH, ".//span[contains(@class, 'SalaryWrapper')]")
                    salary = salary_elem[0].get_attribute("textContent").strip() if salary_elem else "Tidak Dicantumkan"

                    # --- EKSTRAKSI LOKASI DINAMIS (City & Province) ---
                    city = "Unknown"
                    province = "Unknown"
                    
                    # Link lokasi berdasarkan class yang kamu temukan
                    loc_links = card.find_elements(By.CSS_SELECTOR, "a[class*='JobCardLocationNoStyleAnchor']")
                    
                    if len(loc_links) >= 2:
                        city = loc_links[0].get_attribute("textContent").strip()
                        province = loc_links[1].get_attribute("textContent").strip()
                    elif len(loc_links) == 1:
                        city = loc_links[0].get_attribute("textContent").strip()
                        province = city
                    # --------------------------------------------------

                    posted_elem = card.find_elements(By.XPATH, ".//p[contains(@class, 'UpdatedAtMessage')]")
                    posted_time = posted_elem[0].get_attribute("textContent").strip() if posted_elem else "Baru saja"

                    scraped_data[job_id] = {
                        "job_id": job_id,
                        "title": title_elem.get_attribute("textContent").strip(),
                        "company": company,
                        "salary": salary,
                        "city": city,
                        "province": province,
                        "posted_time": posted_time,
                        "link": link,
                        "scrape_date": scrape_date,
                        "source": "Glints"
                    }
                except:
                    continue
            
            print(f"Total Unik Sementara: {len(scraped_data)}")

    finally:
        driver.quit()

    return list(scraped_data.values())

if __name__ == "__main__":
    # 1. Jalankan Scraper
    data = run_scraper()
    
    if data:
        df = pd.DataFrame(data)
        
        # 2. Database Ingestion
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
            
            df.to_sql('jobs', engine, schema='bronze', if_exists='append', index=False, chunksize=1000, method='multi')
            print(f"✅ [BRONZE] {len(df)} data Glints berhasil masuk ke DB.")
            
        except Exception as e:
            print(f"❌ [DB ERROR] Gagal ingest ke DB: {e}")
        
        # 3. CSV Backup
        os.makedirs("data", exist_ok=True)
        df.to_csv("data/raw_glints_data.csv", index=False)
        print("✅ Backup CSV berhasil disimpan.")
    else:
        print("❌ Tidak ada data Glints yang didapat.")