from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
import time
import re
import os
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from pymongo import MongoClient

# Konfigurasi logging untuk mencatat aktivitas scraping
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- (Tidak ada perubahan pada semua fungsi Anda dari get_mongo_client hingga extract_news_from_iqplus) ---
def get_mongo_client():
    try:
        logger.info("Menghubungkan ke MongoDB...")
        client = MongoClient("mongodb://mongodb-external:27017/")
        client.admin.command('ping')  # Test koneksi
        logger.info("Berhasil terhubung ke MongoDB!")
        return client
    except Exception as e:
        logger.error(f"Gagal terhubung ke MongoDB: {e}")
        raise

def setup_selenium_driver():
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Jalankan browser tanpa tampilan GUI
        chrome_options.add_argument("--disable-gpu")  # Nonaktifkan GPU untuk performa
        chrome_options.add_argument("--no-sandbox")  # Untuk kompatibilitas container
        chrome_options.add_argument("--disable-dev-shm-usage")  # Untuk mencegah crash di container
        chrome_options.add_argument("--window-size=1920,1080")  # Set ukuran window browser
        chrome_options.add_argument("--disable-extensions")  # Nonaktifkan ekstensi
        chrome_options.add_argument("--disable-notifications")  # Nonaktifkan notifikasi
        chrome_options.add_argument("--ignore-certificate-errors")  # Abaikan error sertifikat
        
        # Set lokasi binary Chrome dari environment variable
        chrome_bin = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
        chrome_options.binary_location = chrome_bin
        
        # Konfigurasi ChromeDriver
        chromedriver_path = os.environ.get("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver")
        chrome_service = Service(executable_path=chromedriver_path)
        
        # Inisialisasi WebDriver dengan logic retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
                driver.set_page_load_timeout(30)  # Timeout 30 detik untuk loading halaman
                return driver
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Inisialisasi WebDriver gagal (percobaan {attempt + 1}), mencoba lagi...")
                time.sleep(5)
    except Exception as e:
        logger.error(f"Gagal setup Selenium: {e}")
        raise

def extract_article_content(driver, article_url, base_url):
    full_url = base_url + article_url if not article_url.startswith("http") else article_url
    try:
        driver.get(full_url)
        # Tunggu sampai elemen dengan ID "zoomthis" muncul
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "zoomthis"))
        )
        
        # Parse HTML menggunakan BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        zoom_div = soup.find("div", id="zoomthis")
        if not zoom_div:
            logger.warning(f"Konten tidak ditemukan di {full_url}")
            return None, None
        
        # Ekstrak tanggal artikel
        date_element = zoom_div.find("small")
        date_text = date_element.text.strip() if date_element else "Tanggal tidak tersedia"
        
        # Hapus elemen yang tidak diinginkan
        for element in zoom_div.find_all(["small", "h3", "div"]):
            element.extract()
        
        # Bersihkan konten dari spasi berlebih
        content = zoom_div.get_text(separator=" ", strip=True)
        content = re.sub(r'\s+', ' ', content).strip()
        
        return date_text, content
        
    except Exception as e:
        logger.error(f"Error scraping artikel {full_url}: {e}")
        return None, None

def scrape_page(driver, url, base_url, collection):
    article_count = 0
    new_article_ids = []
    
    try:
        driver.get(url)
        # Tunggu sampai elemen dengan ID "load_news" muncul
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "load_news"))
        )
        time.sleep(2)  # Beri waktu untuk konten dinamis dimuat
        
        # Parse HTML untuk mencari daftar berita
        soup = BeautifulSoup(driver.page_source, "html.parser")
        news_list = soup.select_one("#load_news .box ul.news")
        if not news_list:
            logger.warning("Elemen daftar berita tidak ditemukan")
            return 0, []
        
        news_items = news_list.find_all("li")
        if not news_items:
            logger.warning("Tidak ada item berita ditemukan dalam daftar")
            return 0, []
        
        # Loop untuk setiap item berita
        for item in news_items:
            try:
                # Ekstrak waktu publikasi
                time_text = item.find("b").text.strip() if item.find("b") else "Waktu tidak tersedia"
                title_tag = item.find("a")
                
                if not title_tag or not title_tag.has_attr("href"):
                    continue
                
                # Ekstrak judul dan link
                title = title_tag.text.strip()
                link = title_tag["href"]
                
                # Cek apakah artikel sudah ada di database
                if collection.find_one({"judul": title}):
                    continue
                
                # Ekstrak konten artikel
                article_date, article_content = extract_article_content(driver, link, base_url)
                if not article_content:
                    continue
                
                # Siapkan data artikel untuk disimpan
                article_data = {
                    "judul": title,
                    "waktu": time_text,
                    "link": base_url + link if not link.startswith("http") else link,
                    "tanggal_artikel": article_date,
                    "konten": article_content,
                    "tanggal_scrape": datetime.now().isoformat(),
                    "status_transformasi": "belum"
                }
                
                # Simpan ke database
                result = collection.insert_one(article_data)
                new_article_ids.append(result.inserted_id)
                article_count += 1
                logger.info(f"Berhasil menyimpan artikel baru: {title[:50]}...")
                
            except Exception as e:
                logger.error(f"Error memproses item berita: {e}")
                continue
        
        return article_count, new_article_ids
        
    except Exception as e:
        logger.error(f"Error scraping halaman {url}: {e}")
        return 0, []

def extract_news_from_iqplus(news_category: str, news_url_path: str, **kwargs):
    driver = None # Inisialisasi driver ke None
    try:
        logger.info(f"Memulai ekstraksi berita IQPlus untuk kategori: {news_category}")
        
        # Setup koneksi MongoDB
        client = get_mongo_client()
        db = client["Iqplus"]
        collection = db["Iqplus_News_Extract"] 
        
        # Konfigurasi URL dan pengaturan
        base_url = "http://www.iqplus.info"
        start_url = f"{base_url}{news_url_path}/go-to-page,0.html"
        max_pages = int(os.getenv(f'MAX_PAGES_{news_category.upper().replace(" ", "_")}', '210')) # Environment variable untuk max halaman per kategori
        
        # Setup Selenium WebDriver
        driver = setup_selenium_driver()
        
        # Dapatkan total halaman yang akan di-scrape
        def get_last_page():
            try:
                driver.get(start_url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "nav"))
                )
                
                soup = BeautifulSoup(driver.page_source, "html.parser")
                nav_span = soup.find("span", class_="nav")
                if nav_span:
                    # Cari semua link halaman, yang kedua dari terakhir biasanya adalah nomor halaman terakhir
                    page_links = nav_span.find_all("a")
                    if len(page_links) >= 2:
                        last_page_link = page_links[-2] 
                        if last_page_link and last_page_link.text.isdigit():
                            return min(int(last_page_link.text), max_pages)
            except Exception as e:
                logger.error(f"Error mendapatkan halaman terakhir untuk {news_category}: {e}")
            return 1  # Fallback ke halaman pertama saja
        
        last_page = get_last_page()
        logger.info(f"Akan scraping {last_page} halaman untuk {news_category}")
        
        # Mulai scraping halaman
        total_articles = 0
        all_new_article_ids = []
        
        for page in range(0, last_page + 1):
            # Sesuaikan struktur URL berdasarkan cara IQPlus menangani paginasi untuk setiap kategori
            # Asumsi konsisten: /news/category/go-to-page,X.html
            page_url = f"{base_url}{news_url_path}/go-to-page,{page}.html"
            logger.info(f"Scraping halaman: {page_url} untuk {news_category}")
            
            articles_scraped, new_ids = scrape_page(driver, page_url, base_url, collection)
            total_articles += articles_scraped
            all_new_article_ids.extend(new_ids)
            
            logger.info(f"Artikel yang di-scrape dari halaman {page} ({news_category}): {articles_scraped}")
            time.sleep(2)  # Jeda untuk tidak membebani server
        
        logger.info(f"Selesai scraping {news_category}. Total artikel baru: {total_articles}")
        
        # Push hasil ke XCom untuk digunakan task lain
        kwargs['ti'].xcom_push(key=f'total_extracted_articles_{news_category.lower().replace(" ", "_")}', value=total_articles)
        kwargs['ti'].xcom_push(key=f'new_article_ids_{news_category.lower().replace(" ", "_")}', value=[str(id) for id in all_new_article_ids])
        
    except Exception as e:
        logger.error(f"Ekstraksi untuk {news_category} gagal: {e}")
        raise
    finally:
        # Pastikan WebDriver ditutup dengan baik
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Error menutup WebDriver: {e}")

# Definisi DAG
with DAG(
    dag_id='iqplus_scraper_parallel',
    description='Robust IQPlus news scraper untuk berita Market dan Stock secara paralel',
    schedule_interval='10 17 * * *',  # Dijadwalkan setiap hari jam 17:10 (5:10 PM)
    start_date=days_ago(1),
    catchup=False,  # Tidak menjalankan untuk tanggal yang terlewat
    default_args={
        'owner': 'airflow',
        'retries': 2,  # Maksimal 2 kali retry jika task gagal
        'retry_delay': timedelta(minutes=5),  # Jeda 5 menit antar retry
        'execution_timeout': timedelta(minutes=360),  # Maximum 6 jam untuk eksekusi
    },
    tags=['iqplus', 'news', 'scraper', 'parallel'],  # Tag untuk kategorisasi DAG
) as dag:

    # Definisi task untuk Market News
    extract_market_news_task = PythonOperator(
        task_id='extract_market_news',
        python_callable=extract_news_from_iqplus,
        op_kwargs={
            'news_category': 'Market News',
            'news_url_path': '/news/market_news', # Path spesifik untuk Market News
        },
        provide_context=True,
    )

    # Definisi task untuk Stock News
    extract_stock_news_task = PythonOperator(
        task_id='extract_stock_news',
        python_callable=extract_news_from_iqplus,
        op_kwargs={
            'news_category': 'Stock News',
            'news_url_path': '/news/stock_news', # Path spesifik untuk Stock News
        },
        provide_context=True,
    )
    
    # Ganti 'transform_load_iqplus' dengan ID DAG transformasi Anda jika berbeda
    trigger_transform_dag = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='transform_load_iqplus',
    )
    
    # Kedua task scraping berjalan paralel, lalu memicu DAG selanjutnya
    [extract_market_news_task, extract_stock_news_task] >> trigger_transform_dag