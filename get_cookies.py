from selenium import webdriver
import json
import time

# Pakai chrome biasa di Windows
driver = webdriver.Chrome()

# Buka Glints
driver.get("https://glints.com/id/login")

print("SILAHLAN LOGIN MANUAL...")
print("Setelah masuk ke Dashboard/Halaman utama, balik ke sini dan tekan ENTER.")
input("TEKAN ENTER KALAU SUDAH LOGIN...")

# Ambil cookies asli format Selenium
cookies = driver.get_cookies()

# Simpan ke JSON
with open("glints_cookies.json", "w") as f:
    json.dump(cookies, f, indent=2)

print("✅ BERHASIL! File glints_cookies.json sudah jadi dengan format yang benar.")
driver.quit()