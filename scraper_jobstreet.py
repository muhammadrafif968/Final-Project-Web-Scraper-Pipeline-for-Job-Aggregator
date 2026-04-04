import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def init_driver_jobstreet():
    print("Menghubungkan ke Selenium Grid di Docker untuk Jobstreet...")
    options = Options()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    # Menambahkan User-Agent agar tidak dicurigai sebagai bot
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Remote(
        command_executor='http://localhost:4444/wd/hub',
        options=options
    )
    return driver

def run_scraper_jobstreet(keyword="data analyst", max_pages=2):
    driver = init_driver_jobstreet()
    scraped_data = []
    scrape_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Format URL Jobstreet (biasanya spasi diganti strip)
    formatted_keyword = keyword.replace(' ', '-')

    try:
        for page in range(1, max_pages + 1):
            print(f"\n--- Jobstreet: Membuka Halaman {page} ---")
            
            # Catatan: ganti .com.sg atau .co.id sesuai target kamu
            url = f"https://sg.jobstreet.com/{formatted_keyword}-jobs?page={page}"
            driver.get(url)
            
            # Tunggu sampai elemen jobTitle pertama muncul
            try:
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//a[@data-automation='jobTitle']")))
            except:
                print(f"Data tidak ditemukan di halaman {page}. Berhenti scraping.")
                
                #Ambil Screenshot dan simpan ke folder data
                import os
                os.makedirs("data", exist_ok=True)
                screenshot_path = "data/debug_jobstreet_error.png"
                driver.save_screenshot(screenshot_path)
                print(f"[DEBUG] Screenshot disimpan di: {screenshot_path}")
                print(f"[DEBUG] URL terakhir yang dibuka: {driver.current_url}")
                
                break
            
            # Scroll pelan-pelan agar semua card termuat
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3) 

            # Mengambil kotak pembungkus (Biasanya Jobstreet menggunakan tag <article>)
            job_cards = driver.find_elements(By.XPATH, "//article")
            print(f"Ditemukan {len(job_cards)} lowongan di halaman {page}")

            for card in job_cards:
                try:
                    # 1. Title & Link
                    title_elem = card.find_element(By.XPATH, ".//a[@data-automation='jobTitle']")
                    title = title_elem.text
                    raw_link = title_elem.get_attribute("href")
                    link = raw_link.split('?')[0] # Bersihkan tracking parameter
                    
                    # 2. Extract ID dari Jobstreet link (biasanya ada angka di ujung URL)
                    raw_id = link.split('-')[-1] 
                    job_id = f"js-{raw_id}" # Tambahkan prefix js- agar beda dengan Glints

                    # 3. Company
                    try:
                        company = card.find_element(By.XPATH, ".//a[@data-automation='jobCompany']").text
                    except:
                        company = None

                    # 4. Location (Biasanya pakai jobLocation)
                    try:
                        location = card.find_element(By.XPATH, ".//a[@data-automation='jobLocation']").text
                    except:
                        location = None

                    # 5. Salary (Biasanya pakai jobSalary)
                    try:
                        salary = card.find_element(By.XPATH, ".//span[@data-automation='jobSalary']").text
                    except:
                        salary = None

                    # 6. Posted Time (Biasanya pakai jobListingDate)
                    try:
                        posted_time = card.find_element(By.XPATH, ".//span[@data-automation='jobListingDate']").text
                    except:
                        posted_time = None

                    scraped_data.append({
                        "job_id": job_id,
                        "title": title,
                        "company": company,
                        "salary": salary,
                        "city": location, # Disimpan di kolom city dulu untuk konsistensi
                        "province": None,
                        "posted_time": posted_time,
                        "link": link,
                        "scrape_date": scrape_date,
                        "source": "Jobstreet" # Penanda sumber data
                    })

                except Exception as e:
                    # Jika card tidak memiliki elemen title, skip
                    continue

    except Exception as e:
        print(f"Error utama saat scraping Jobstreet: {e}")
        
    finally:
        print("Menutup browser...")
        driver.quit()

    print(f"\nTOTAL DATA JOBSTREET DISCRAPE: {len(scraped_data)}")
    return scraped_data


# Blok untuk eksekusi langsung dan export ke CSV
if __name__ == "__main__":
    import pandas as pd
    
    # Tes scrape 2 halaman
    data = run_scraper_jobstreet(keyword="data", max_pages=2)
    
    if data:
        pd.set_option('display.max_colwidth', None) 
        pd.set_option('display.max_columns', None)
        
        df = pd.DataFrame(data)
        print("\n--- Sampel Data Jobstreet ---")
        print(df.head())
        
        # EXPORT KE CSV
        output_dir = "data"
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, "raw_jobstreet_data.csv")
        df.to_csv(output_file, index=False)
        print(f"\n[SUKSES] Data Jobstreet berhasil diexport ke: {output_file}")
    else:
        print("\n[GAGAL] Tidak ada data Jobstreet yang berhasil discrape.")