import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def init_driver():
    print("Menghubungkan ke Selenium Grid di Docker...")
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    # options.add_argument('--headless') # Uncomment nanti saat masuk Airflow
    
    driver = webdriver.Remote(
        command_executor='http://localhost:4444/wd/hub',
        options=options
    )
    return driver

def check_older_than_30_days(time_text):
    if not time_text: return False
    time_text = time_text.lower()
    if "month" in time_text or "year" in time_text:
        if "months" in time_text or "year" in time_text:
            return True 
    return False

def extract_job_id_from_url(url):
    try:
        return url.split('/')[-1].split('?')[0]
    except:
        return None

import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def init_driver():
    print("Menghubungkan ke Selenium Grid di Docker...")
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Remote(
        command_executor='http://localhost:4444/wd/hub',
        options=options
    )
    return driver

def check_older_than_30_days(time_text):
    if not time_text: return False
    time_text = time_text.lower()
    if "month" in time_text or "year" in time_text:
        if "months" in time_text or "year" in time_text:
            return True 
    return False

def extract_job_id_from_url(url):
    try:
        return url.split('/')[-1].split('?')[0]
    except:
        return None

def run_scraper(max_pages=3):
    driver = init_driver()
    scraped_data = []
    scrape_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        # Buku tamu untuk mencatat ID
        seen_job_ids = set()

        for page in range(1, max_pages + 1):
            print(f"\n--- Membuka Halaman {page} ---")
            
            base_url = f"https://glints.com/sg/opportunities/jobs/explore?keyword=data&country=SG&locationName=All+Regions+in+Singapore&lowestLocationLevel=1&sortBy=LATEST&page={page}"
            driver.get(base_url)
            
            card_xpath = "//div[contains(@class, 'CompactOpportunityCard')]" 
            
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, card_xpath)))
            except:
                print(f"Data tidak ditemukan di halaman {page}. Berhenti scraping.")
                break
            
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3) 
            
            job_cards = driver.find_elements(By.XPATH, card_xpath)
            valid_cards = [card for card in job_cards if len(card.find_elements(By.XPATH, ".//h2[contains(@class, 'JobTitle')]/a")) > 0]
            print(f"Ditemukan {len(valid_cards)} lowongan di halaman {page}")

            for card in valid_cards:
                try:
                    title_elem = card.find_element(By.XPATH, ".//h2[contains(@class, 'JobTitle')]/a")
                    title = title_elem.text
                    raw_link = title_elem.get_attribute("href")
                    
                    # Bersihkan link & Anti Duplikat
                    link = raw_link.split('?')[0]
                    raw_id = extract_job_id_from_url(link)
                    job_id = f"gl-{raw_id}"

                    if job_id in seen_job_ids:
                        continue
                    seen_job_ids.add(job_id)
                    
                    try:
                        salary = card.find_element(By.XPATH, ".//span[contains(@class, 'SalaryWrapper')]").text
                    except:
                        salary = None
                        
                    try:
                        company = card.find_element(By.XPATH, ".//a[contains(@class, 'CompanyLink')]").text
                    except:
                        company = None

                    try:
                        locations = card.find_elements(By.XPATH, ".//span[contains(@class, 'LocationSpan')]/a")
                        city = locations[0].text if len(locations) > 0 else None
                        province = locations[1].text if len(locations) > 1 else None
                    except:
                        city, province = None, None

                    try:
                        posted_time = card.find_element(By.XPATH, ".//p[contains(@class, 'UpdatedAtMessage')]").text
                    except:
                        posted_time = None
                    
                    if check_older_than_30_days(posted_time):
                        continue

                    scraped_data.append({
                        "job_id": job_id,
                        "title": title,
                        "company": company,
                        "salary": salary,
                        "city": city,
                        "province": province,
                        "posted_time": posted_time,
                        "link": link,
                        "scrape_date": scrape_date,
                        "source": "Glints"
                    })

                except Exception as e:
                    print(f"Error pada 1 card: {e}")
                    continue

    except Exception as e:
        print(f"Error utama: {e}")
        
    finally:
        driver.quit()

    print(f"\nTOTAL DATA DISCRAPE: {len(scraped_data)}")
    return scraped_data

# === BLOK EKSEKUSI UTAMA ===
if __name__ == "__main__":
    import os
    import pandas as pd
    
    data = run_scraper(max_pages=2)
    
    if data:
        pd.set_option('display.max_colwidth', None) 
        pd.set_option('display.max_columns', None)
        
        df = pd.DataFrame(data)
        print("\n--- Sampel Data ---")
        print(df.head())
        
        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, "raw_glints_data.csv")
        df.to_csv(output_file, index=False)
        print(f"\n[SUKSES] Data berhasil diexport ke: {output_file}")
    else:
        print("\n[GAGAL] Tidak ada data yang berhasil discrape.")