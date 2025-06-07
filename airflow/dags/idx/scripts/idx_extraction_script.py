import os
import time
import io
import pandas as pd
import requests
import zipfile
import random
import xml.etree.ElementTree as ET
import json
import logging
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Konfigurasi dasar
DOWNLOAD_DIR = "/app/downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
IDX_URL = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan/"
DB_NAME = "idx_financial_data_staging"
COLLECTION_NAME_PREFIX = "reports"

def init_driver(download_dir_container):
    logger.info("Menginisialisasi WebDriver...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", download_dir_container)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/zip,application/octet-stream,application/x-zip-compressed,multipart/x-zip")
    
    try:
        driver = webdriver.Firefox(options=options)
        logger.info(f"WebDriver berhasil diinisialisasi. Download akan disimpan di: {download_dir_container}")
        return driver
    except Exception as e:
        logger.error(f"Gagal menginisialisasi WebDriver: {e}")
        raise

def extract_xml_from_zip(zip_content_bytes):
    # ... (Fungsi ini tidak diubah, salin dari file asli Anda)
    try:
        logger.info("🔓 Mengekstrak XML/XBRL dari ZIP...")
        with zipfile.ZipFile(io.BytesIO(zip_content_bytes)) as zip_file:
            potential_files = [f for f in zip_file.namelist() if f.lower().endswith(('.xml', '.xbrl'))]
            if not potential_files:
                logger.warning("❌ Tidak ada file XML atau XBRL di dalam ZIP.")
                return None
            
            instance_files = [f for f in potential_files if 'instance' in f.lower()]
            if instance_files:
                target_file = instance_files[0]
            else:
                target_file = potential_files[0]

            logger.info(f"File yang dipilih untuk ekstraksi: {target_file}")
            xml_content = zip_file.read(target_file)
        logger.info("✅ XML/XBRL berhasil diekstrak.")
        return xml_content
    except Exception as e:
        logger.error(f"❌ Gagal mengekstrak XML/XBRL dari ZIP: {e}", exc_info=True)
        return None

def xml_to_json_flat(xml_content_bytes):
    # ... (Fungsi ini tidak diubah, salin dari file asli Anda)
    def remove_namespace(tag):
        return tag.split('}')[-1] if '}' in tag else tag
    try:
        logger.info("🔄 Mengonversi XML ke JSON flat...")
        if isinstance(xml_content_bytes, bytes):
            try:
                xml_content_str = xml_content_bytes.decode('utf-8')
            except UnicodeDecodeError:
                xml_content_str = xml_content_bytes.decode('latin-1', errors='replace')
        else:
            xml_content_str = xml_content_bytes
        
        root = ET.fromstring(xml_content_str)
        json_data = {}
        for elem in root.iter():
            tag_name = remove_namespace(elem.tag)
            if elem.text and elem.text.strip():
                json_data[tag_name] = elem.text.strip()
        
        if not json_data:
            logger.warning("Data JSON kosong setelah konversi.")
        logger.info("✅ XML berhasil dikonversi ke JSON.")
        return json_data
    except Exception as e:
        logger.error(f"❌ Gagal mengonversi XML ke JSON: {e}", exc_info=True)
        return None

def save_to_mongodb(data_to_save, company_code, year_str, period_str, mongo_uri):
    if data_to_save is None:
        logger.warning(f"Tidak ada data untuk disimpan: {company_code} - {year_str} {period_str}")
        return
    try:
        collection_name = f"{COLLECTION_NAME_PREFIX}_{year_str}_{period_str}"
        logger.info(f"💾 Menyimpan data untuk {company_code} ke koleksi '{collection_name}'...")
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[collection_name]
        document = {
            "company_code": company_code, "year": year_str, "period": period_str,
            "retrieved_at": pd.Timestamp.now(tz='UTC'),
            "financial_data_source": "IDX_Instance_XBRL", "data": data_to_save
        }
        collection.update_one(
            {"company_code": company_code, "year": year_str, "period": period_str},
            {"$set": document}, upsert=True
        )
        logger.info(f"✅ Data berhasil disimpan/diperbarui untuk {company_code}.")
    except Exception as e:
        logger.error(f"❌ Gagal menyimpan data ke MongoDB untuk {company_code}: {e}", exc_info=True)
    finally:
        if 'client' in locals() and client:
            client.close()

def scrape_financial_data(driver, mongo_uri, year_to_scrape, periods_to_scrape_str):
    periods_to_scrape = [p.strip().lower() for p in periods_to_scrape_str.split(',')]
    
    for period_val_str in periods_to_scrape:
        logger.info(f"\n🌐 Memulai scraping: Tahun {year_to_scrape}, Periode {period_val_str}")
        try:
            driver.get(IDX_URL)
            WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.ID, "search-form")))
            
            # Memilih tahun
            year_radio = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f"input[name='year'][value='{year_to_scrape}']")))
            driver.execute_script("arguments[0].click();", year_radio)
            logger.info(f"✅ Tahun dipilih: {year_to_scrape}")
            time.sleep(random.uniform(1, 2))
            
            # Memilih periode
            period_radio = WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.CSS_SELECTOR, f"input[name='period'][value='{period_val_str}']")))
            driver.execute_script("arguments[0].click();", period_radio)
            logger.info(f"✅ Periode dipilih: {period_val_str}")
            time.sleep(random.uniform(1, 2))

            # Klik tombol "Terapkan"
            apply_button = driver.find_element(By.CSS_SELECTOR, "button.btn--primary")
            driver.execute_script("arguments[0].click();", apply_button)
            logger.info("✅ Tombol 'Terapkan' diklik")
            
            # Tunggu hingga hasil muncul
            WebDriverWait(driver, 45).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div.bzg_c > div.box")))
            
            current_page = 1
            while True:
                logger.info(f"📄 Memproses halaman {current_page} untuk {year_to_scrape}-{period_val_str}")
                company_boxes = driver.find_elements(By.CSS_SELECTOR, "div.bzg_c > div.box")
                if not company_boxes:
                    logger.info("Tidak ada perusahaan di halaman ini. Selesai.")
                    break

                for box in company_boxes:
                    company_code = "UNKNOWN"
                    try:
                        company_code = box.find_element(By.CSS_SELECTOR, "div.box-title span").text.strip()
                        logger.info(f"🏢 Memproses: {company_code}")

                        download_links = box.find_elements(By.CSS_SELECTOR, "a.link-download")
                        instance_link_found = False
                        for link in download_links:
                            if 'instance.zip' in link.get_attribute('href'):
                                zip_url = link.get_attribute('href')
                                logger.info(f"🔗 Link download ditemukan: {zip_url}")
                                
                                response = requests.get(zip_url, timeout=60)
                                response.raise_for_status()
                                
                                xml_content = extract_xml_from_zip(response.content)
                                if xml_content:
                                    json_data = xml_to_json_flat(xml_content)
                                    if json_data:
                                        save_to_mongodb(json_data, company_code, year_to_scrape, period_val_str, mongo_uri)
                                
                                instance_link_found = True
                                break
                        if not instance_link_found:
                            logger.info(f"Tidak ada link 'instance.zip' untuk {company_code}")

                    except Exception as e_box:
                        logger.error(f"❌ Gagal memproses box untuk {company_code}: {e_box}", exc_info=True)

                try:
                    next_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[rel='next']")))
                    driver.execute_script("arguments[0].click();", next_button)
                    logger.info("➡️ Pindah ke halaman berikutnya...")
                    time.sleep(random.uniform(3, 5))
                    current_page += 1
                except:
                    logger.info("🏁 Tombol 'Next' tidak ditemukan. Selesai untuk periode ini.")
                    break
        except Exception as e_period:
            logger.error(f"❌ Gagal memproses {year_to_scrape}-{period_val_str}: {e_period}", exc_info=True)
            continue # Lanjutkan ke periode berikutnya jika gagal

def main_extraction_task(**kwargs):
    # Mengambil parameter dari Airflow
    year_to_process = kwargs['op_kwargs']['year']
    all_periods_string = kwargs['op_kwargs']['periods']
    mongo_uri = "mongodb://mongodb:27017/"  # Sesuai dengan nama service di docker-compose teman Anda
    
    logger.info(f"🚀 Memulai task ekstraksi untuk Tahun: {year_to_process}")
    
    driver = None
    try:
        # Bersihkan file download lama jika ada
        for item in os.listdir(DOWNLOAD_DIR):
            if item.lower().endswith((".zip", ".part")):
                os.remove(os.path.join(DOWNLOAD_DIR, item))

        driver = init_driver(DOWNLOAD_DIR)
        scrape_financial_data(driver, mongo_uri, year_to_process, all_periods_string)
        logger.info(f"✅ Task ekstraksi untuk Tahun {year_to_process} selesai.")
    except Exception as e:
        logger.error(f"🔥 Terjadi error fatal pada task untuk tahun {year_to_process}: {e}", exc_info=True)
        raise
    finally:
        if driver:
            driver.quit()
            logger.info("🚪 Browser ditutup.")