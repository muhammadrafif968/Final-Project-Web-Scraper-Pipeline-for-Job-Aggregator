import os

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "airflow")
DB_PASS = os.getenv("DB_PASS", "airflow")
DB_NAME = os.getenv("DB_NAME", "airflow")

# Kredensial Scraper
GLINTS_EMAIL = os.getenv("GLINTS_EMAIL")
GLINTS_PASSWORD = os.getenv("GLINTS_PASSWORD")
MAX_PAGES = int(os.getenv("MAX_PAGES", 5))
SEARCH_KEYWORD = os.getenv("SEARCH_KEYWORD", "data")

# URL Selenium Grid 
SELENIUM_URL = "http://selenium:4444/wd/hub"