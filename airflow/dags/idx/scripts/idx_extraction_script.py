"""
IDX Financial Data Extraction Script - Updated Version
Script to extract financial data from the IDX website using Selenium.
It's designed to be robust against common web scraping challenges.
"""

import os
import time
import io
import pandas as pd
# import requests # Tidak lagi digunakan untuk unduhan langsung, bisa dihapus atau dikomentari
import zipfile
import random
import xml.etree.ElementTree as ET
import json
import logging
import shutil 

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, ElementClickInterceptedException, StaleElementReferenceException
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# Setup logging
logger = logging.getLogger(__name__)

# === KONFIGURASI ===
BASE_DOWNLOAD_DIR = "/opt/airflow/data"
IDX_URL = "https://idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan/"
DB_NAME = "idx_financial_data_staging"
COLLECTION_NAME_PREFIX = "reports"
DEFAULT_PERIODS = "tw1,tw2,tw3,audit" # **PENTING: Pastikan ini sudah terdefinisi di sini!**

# === UTILITY FUNCTIONS ===

def take_screenshot(driver, filename="screenshot_error.png", year_dir=None):
    """Mengambil screenshot untuk debugging."""
    try:
        if year_dir:
            screenshot_path = os.path.join(year_dir, filename)
        else:
            screenshot_path = os.path.join(BASE_DOWNLOAD_DIR, filename)
        driver.save_screenshot(screenshot_path)
        logger.error(f"Screenshot saved to: {screenshot_path}")
    except Exception as e:
        logger.error(f"Failed to take screenshot: {e}")

def ensure_year_download_dir(year):
    """Memastikan direktori download per tahun ada dan dapat diakses."""
    year_dir = os.path.join(BASE_DOWNLOAD_DIR, f"year_{year}")
    
    try:
        os.makedirs(year_dir, exist_ok=True)
        logger.info(f"Year download directory ready: {year_dir}")
        
        test_file = os.path.join(year_dir, "test_write.tmp")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        logger.info("Write permission verified for year directory")
        
        return year_dir
        
    except Exception as e:
        logger.error(f"Failed to prepare year download directory: {e}. Falling back to /tmp.")
        fallback_dir = f"/tmp/idx_downloads/year_{year}"
        os.makedirs(fallback_dir, exist_ok=True)
        logger.info(f"Using fallback directory: {fallback_dir}")
        return fallback_dir

def init_driver(year_download_dir):
    """Inisialisasi WebDriver dengan konfigurasi optimal."""
    logger.info("Initializing WebDriver...")

    options = Options()
    
    options.add_argument("--headless")
    options.add_argument("--no-sandbox") 
    options.add_argument("--disable-dev-shm-usage") 
    options.add_argument("--disable-gpu") 
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-web-security")
    options.add_argument("--allow-running-insecure-content")
    
    options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--memory-pressure-off")
    options.add_argument("--max_old_space_size=4096")

    options.log.level = "warn" 

    # Preferensi unduhan untuk mengarahkan ke direktori tahunan
    options.set_preference("browser.download.folderList", 2) 
    options.set_preference("browser.download.dir", year_download_dir)
    options.set_preference("browser.download.useDownloadDir", True)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk",
                           "application/zip,application/octet-stream,application/x-zip-compressed,multipart/x-zip")
    options.set_preference("browser.download.manager.showWhenStarting", False) 
    options.set_preference("pdfjs.disabled", True) 

    try:
        driver = webdriver.Firefox(options=options)
        driver.set_page_load_timeout(120) 
        logger.info(f"WebDriver initialized successfully. Downloads will be saved to: {year_download_dir}")
        return driver
        
    except WebDriverException as e:
        logger.error(f"Failed to initialize WebDriver: {e}. Make sure Firefox and Geckodriver are installed and in PATH.")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during WebDriver initialization: {e}")
        raise

def wait_for_page_load(driver, timeout=45):
    """Tunggu halaman selesai loading dengan multiple conditions."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
        try:
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return typeof jQuery !== 'undefined' && jQuery.active == 0")
            )
        except TimeoutException:
            logger.debug("jQuery not found or not active within 10 seconds. Proceeding...")
            pass
        try:
            WebDriverWait(driver, 5).until_not(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".loading-overlay, .spinner, #loading-indicator, [class*='loader']"))
            )
        except TimeoutException:
            logger.debug("Loading overlay did not disappear within 5 seconds. Proceeding...")
            pass
            
        logger.info("✅ Page fully loaded (or timed out gracefully).")
        return True
        
    except TimeoutException:
        logger.warning("⚠️ Page load timeout occurred, but proceeding with current state of the page.")
        return False
    except Exception as e:
        logger.error(f"An error occurred while waiting for page load: {e}")
        return False


def find_element_with_multiple_selectors(driver, selectors, timeout=20):
    """Coba beberapa selector sampai menemukan yang cocok."""
    for selector_type, selector_value in selectors:
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((selector_type, selector_value))
            )
            if element.is_displayed() and element.is_enabled():
                logger.info(f"✅ Found and verified element with selector: {selector_type} = {selector_value}")
                return element
            else:
                logger.debug(f"Element found with {selector_type} = {selector_value} but not visible/enabled. Trying next selector.")
                continue
        except TimeoutException:
            logger.debug(f"❌ Selector {selector_type} = {selector_value} timed out. Trying next selector.")
            continue
        except NoSuchElementException:
            logger.debug(f"❌ Selector {selector_type} = {selector_value} found no matching element. Trying next selector.")
            continue
    
    logger.error("❌ All provided selectors failed to find the element.")
    return None

def download_file_via_selenium_click(driver, link_element, company_code, year_download_dir):
    """
    Mengklik elemen tautan unduhan menggunakan Selenium, memantau unduhan file 'instance.zip',
    lalu mengubah namanya menjadi '<company_code>_instance.zip'.
    """
    original_filename = "instance.zip" # Nama file yang akan diunduh oleh browser
    original_filepath = os.path.join(year_download_dir, original_filename)
    renamed_filepath = os.path.join(year_download_dir, f"{company_code}_instance.zip")

    try:
        # Hapus file sementara dari upaya unduhan sebelumnya
        for f_name in os.listdir(year_download_dir):
            if f_name.startswith(original_filename) or f_name.startswith(f"{company_code}_instance.zip"):
                try:
                    os.remove(os.path.join(year_download_dir, f_name))
                    logger.info(f"🗑️ Removed existing potential partial file: {f_name}")
                except Exception as e:
                    logger.debug(f"Could not remove {f_name}: {e}")

        # Klik tautan menggunakan JavaScript untuk menghindari masalah interseptasi
        driver.execute_script("arguments[0].scrollIntoView(true);", link_element)
        time.sleep(0.5) # Beri sedikit waktu untuk scroll
        driver.execute_script("arguments[0].click();", link_element)
        logger.info(f"📥 Attempting download of '{original_filename}' for {company_code} via Selenium click...")

        # Polling direktori unduhan untuk file 'instance.zip'
        max_wait_download = 90 # Detik untuk menunggu unduhan selesai
        download_start_time = time.time()
        file_downloaded_successfully = False
        
        while time.time() - download_start_time < max_wait_download:
            # Cari file 'instance.zip' atau 'instance.zip.part'
            if os.path.exists(original_filepath) and os.path.getsize(original_filepath) > 0:
                # Periksa apakah unduhan sudah selesai (tidak ada ekstensi .part, .crdownload, .tmp)
                if not any(f.endswith(('.part', '.crdownload', '.tmp')) for f in os.listdir(year_download_dir) if f.startswith(original_filename)):
                    file_downloaded_successfully = True
                    logger.info(f"✅ File '{original_filename}' appears to be fully downloaded for {company_code}.")
                    break
            time.sleep(1) # Cek setiap 1 detik
        
        if file_downloaded_successfully:
            # Ubah nama file 'instance.zip' menjadi '<company_code>_instance.zip'
            if os.path.exists(original_filepath):
                os.rename(original_filepath, renamed_filepath)
                # --- PERBAIKAN F-STRING DI SINI ---
                # Menggunakan single quotes (') untuk f-string bagian dalam
                logger.info(f"✅ Renamed '{original_filename}' to '{f'{company_code}_instance.zip'}' for {company_code}.")
                # --- AKHIR PERBAIKAN F-STRING ---
                return renamed_filepath
            else:
                logger.error(f"❌ '{original_filename}' not found after successful download detection for {company_code}. Renaming failed.")
                return None
        else:
            logger.error(f"❌ Download of '{original_filename}' for {company_code} timed out or failed via Selenium click in {year_download_dir}.")
            take_screenshot(driver, f"error_download_timeout_{company_code}.png", year_download_dir)
            return None

    except StaleElementReferenceException:
        logger.warning(f"Link element became stale for {company_code}. This might happen if the page reloaded. The task will retry.")
        return None 
    except Exception as e:
        logger.error(f"❌ Failed to click and monitor download for {company_code}: {e}", exc_info=True)
        take_screenshot(driver, f"error_download_click_{company_code}.png", year_download_dir)
        return None

def extract_and_cleanup_zip(zip_filepath, company_code, year_download_dir):
    """Ekstrak ZIP file kemudian hapus ZIP file."""
    company_dir = os.path.join(year_download_dir, company_code)
    try:
        logger.info(f"📂 Extracting ZIP for {company_code} from {zip_filepath}...")
        
        os.makedirs(company_dir, exist_ok=True)
        
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            for member in zip_ref.namelist():
                member_path = os.path.join(company_dir, member)
                if not os.path.abspath(member_path).startswith(os.path.abspath(company_dir) + os.sep):
                    raise zipfile.BadZipFile("Attempted Path Traversal in Zip File during extraction.")
                zip_ref.extract(member, company_dir)
        
        logger.info(f"✅ ZIP extracted to: {company_dir}")
        
        os.remove(zip_filepath)
        logger.info(f"🗑️ ZIP file deleted: {zip_filepath}")
        
        xml_content = None
        found_files = []
        for root, dirs, files in os.walk(company_dir):
            for file in files:
                if file.lower().endswith(('.xml', '.xbrl')):
                    file_path = os.path.join(root, file)
                    found_files.append(file_path)
                    if 'instance' in file.lower():
                        with open(file_path, 'rb') as f:
                            xml_content = f.read()
                        logger.info(f"📄 Found primary instance file: {file_path}")
                        break
            if xml_content:
                break
        
        if not xml_content and found_files:
            file_path = found_files[0]
            with open(file_path, 'rb') as f:
                xml_content = f.read()
            logger.info(f"📄 No specific instance file, loaded first XML/XBRL found: {file_path}")
            
        if not xml_content:
            logger.warning(f"No XML/XBRL content found after extracting for {company_code}")

        return xml_content
        
    except zipfile.BadZipFile as e:
        logger.error(f"❌ Corrupted or invalid ZIP file for {company_code}: {e}")
        if os.path.exists(company_dir):
            shutil.rmtree(company_dir)
        return None
    except Exception as e:
        logger.error(f"❌ Failed to extract ZIP for {company_code}: {e}", exc_info=True)
        return None

# === DATA PROCESSING FUNCTIONS ===

def xml_to_json_flat(xml_content_bytes):
    """Konversi XML ke JSON flat structure."""
    def remove_namespace(tag):
        return tag.split('}')[-1] if '}' in tag else tag

    try:
        logger.info("🔄 Converting XML to flattened JSON...")
        
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
            for attr_name, attr_value in elem.attrib.items():
                json_data[f"{tag_name}_@{remove_namespace(attr_name)}"] = attr_value

        if not json_data:
            logger.warning("JSON data is empty after XML conversion. This might indicate an issue with the XML structure or content.")
            
        logger.info("✅ XML successfully converted to JSON.")
        return json_data
        
    except ET.ParseError as e:
        logger.error(f"❌ Failed to parse XML: {e}. Content might be malformed or invalid XML.", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred during XML to JSON conversion: {e}", exc_info=True)
        return None

def save_to_mongodb(data_to_save, company_code, year_str, period_str, mongo_uri):
    """Simpan data ke MongoDB."""
    if data_to_save is None:
        logger.warning(f"No data provided to save for: {company_code} - {year_str} {period_str}")
        return

    data_to_save_processed = data_to_save
    if isinstance(data_to_save, bytes): 
        try:
            data_to_save_processed = data_to_save.decode('utf-8')
        except UnicodeDecodeError:
            data_to_save_processed = data_to_save.decode('latin-1', errors='replace')
    elif isinstance(data_to_save, dict): 
        pass
    else: 
        data_to_save_processed = str(data_to_save)

    client = None
    try:
        collection_name = f"{COLLECTION_NAME_PREFIX}_{year_str}_{period_str}"
        logger.info(f"💾 Saving data for {company_code} to collection '{collection_name}'...")
        
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000) 
        client.admin.command('ping') 
        db = client[DB_NAME]
        collection = db[collection_name]
        
        document = {
            "company_code": company_code,
            "year": year_str,
            "period": period_str,
            "retrieved_at": pd.Timestamp.now(tz='UTC'), 
            "financial_data_source": "IDX_Instance_XBRL",
            "raw_data_type": "XML_Flat_JSON" if isinstance(data_to_save_processed, dict) else "Raw_XML_String",
            "data": data_to_save_processed 
        }
        
        collection.update_one(
            {"company_code": company_code, "year": year_str, "period": period_str},
            {"$set": document}, 
            upsert=True 
        )
        
        logger.info(f"✅ Data successfully saved/updated for {company_code}.")
        
    except ConnectionFailure as e:
        logger.error(f"❌ MongoDB connection failed for {company_code}: {e}. Check MONGO_URI and network access.")
    except OperationFailure as e:
        logger.error(f"❌ MongoDB operation failed for {company_code}: {e}. Check database/collection permissions or schema issues.")
    except Exception as e:
        logger.error(f"❌ An unexpected error occurred while saving data to MongoDB for {company_code}: {e}", exc_info=True)
        
    finally:
        if client:
            client.close() 

# === MAIN SCRAPING FUNCTION ===

def scrape_financial_data(driver, mongo_uri, year_to_scrape, periods_to_scrape_str, year_download_dir):
    """
    Orchestrates the web scraping process for financial data from the IDX website.
    """
    periods_to_scrape = [p.strip().lower() for p in periods_to_scrape_str.split(',')]

    for period_val_str in periods_to_scrape:
        logger.info(f"\n🌐 Starting scraping: Year {year_to_scrape}, Period {period_val_str}")
        
        try:
            logger.info(f"Loading page: {IDX_URL}")
            driver.get(IDX_URL)
            wait_for_page_load(driver)
            time.sleep(random.uniform(2, 4))
            take_screenshot(driver, f"debug_page_load_{year_to_scrape}_{period_val_str}.png", year_download_dir)
            
            form_selectors = [ 
                (By.CSS_SELECTOR, "form.search-form"), (By.CSS_SELECTOR, ".filter-form"), 
                (By.CSS_SELECTOR, ".search-container"), (By.CSS_SELECTOR, "div.filter-section"), 
                (By.CSS_SELECTOR, "div.search-section"), (By.CSS_SELECTOR, "[class*='filter']"), 
                (By.CSS_SELECTOR, "[class*='search']"), (By.TAG_NAME, "form")
            ]
            form_element = find_element_with_multiple_selectors(driver, form_selectors, timeout=45)
            if not form_element:
                logger.error("❌ Could not find search/filter form on the page.")
                take_screenshot(driver, f"error_no_form_{year_to_scrape}_{period_val_str}.png", year_download_dir)
                continue

            logger.info("✅ Filter form found.")
            time.sleep(random.uniform(1, 2))

            year_selectors = [ 
                (By.CSS_SELECTOR, f"input[name='year'][value='{year_to_scrape}']"),
                (By.XPATH, f"//input[contains(@name, 'year') and @value='{year_to_scrape}']"),
                (By.CSS_SELECTOR, f"label[for*='year'] input[value='{year_to_scrape}']"),
                (By.XPATH, f"//label[contains(., '{year_to_scrape}')]/input[@type='radio']"),
                (By.CSS_SELECTOR, f"input[type='radio'][value='{year_to_scrape}']")
            ]
            year_radio = find_element_with_multiple_selectors(driver, year_selectors, timeout=20)
            if year_radio:
                driver.execute_script("arguments[0].click();", year_radio)
                logger.info(f"✅ Year selected: {year_to_scrape}")
                time.sleep(random.uniform(1, 2))
            else:
                logger.error(f"❌ Could not find year selector for {year_to_scrape}. Check website DOM or year value.")
                take_screenshot(driver, f"error_no_year_selector_{year_to_scrape}_{period_val_str}.png", year_download_dir)
                continue
                
            period_mapping = { 
                'tw1': ['tw1', 'triwulan1', 'q1', '1'], 'tw2': ['tw2', 'triwulan2', 'q2', '2'],  
                'tw3': ['tw3', 'triwulan3', 'q3', '3'], 'audit': ['audit', 'tahunan', 'annual', 'yearly', 'full']
            }
            period_values = period_mapping.get(period_val_str, [period_val_str])
            
            period_radio = None
            for period_val in period_values: 
                period_selectors = [
                    (By.CSS_SELECTOR, f"input[name='period'][value='{period_val}']"),
                    (By.XPATH, f"//input[contains(@name, 'period') and @value='{period_val}']"),
                    (By.CSS_SELECTOR, f"label[for*='period'] input[value='{period_val}']"),
                    (By.XPATH, f"//label[contains(text(), '{period_val.upper()}')]/input[@type='radio']"), 
                    (By.XPATH, f"//label[contains(text(), '{period_val.capitalize()}')]/input[@type='radio']"),
                    (By.CSS_SELECTOR, f"input[type='radio'][value='{period_val}']")
                ]
                period_radio = find_element_with_multiple_selectors(driver, period_selectors, timeout=10)
                if period_radio: break
                    
            if period_radio:
                driver.execute_script("arguments[0].click();", period_radio)
                logger.info(f"✅ Period selected: {period_val_str}")
                time.sleep(random.uniform(1, 2))
            else:
                logger.error(f"❌ Could not find period selector for {period_val_str}. Check website DOM or period value.")
                take_screenshot(driver, f"error_no_period_selector_{year_to_scrape}_{period_val_str}.png", year_download_dir)
                continue

            button_selectors = [ 
                (By.CSS_SELECTOR, "button.btn--primary"), (By.CSS_SELECTOR, "button.btn-primary"), 
                (By.CSS_SELECTOR, "button[type='submit']"), (By.CSS_SELECTOR, "input[type='submit']"), 
                (By.CSS_SELECTOR, ".btn-filter"), (By.CSS_SELECTOR, ".btn-search"), 
                (By.CSS_SELECTOR, ".apply-btn"), (By.XPATH, "//button[contains(text(), 'Terapkan')]"), 
                (By.XPATH, "//button[contains(text(), 'Apply')]"), (By.XPATH, "//button[contains(text(), 'Filter')]"), 
                (By.XPATH, "//input[@value='Terapkan']"), (By.CSS_SELECTOR, "button[id*='submit'], button[name*='submit']")
            ]
            apply_button = find_element_with_multiple_selectors(driver, button_selectors, timeout=20)
            if apply_button:
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", apply_button)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", apply_button)
                    logger.info("✅ Apply/Filter button clicked.")
                    time.sleep(random.uniform(3, 5))
                    wait_for_page_load(driver)
                except ElementClickInterceptedException:
                    logger.warning(f"Click on apply button intercepted. Retrying with JS click for {year_to_scrape}-{period_val_str}")
                    driver.execute_script("arguments[0].click();", apply_button)
                    time.sleep(random.uniform(3, 5))
                    wait_for_page_load(driver)
            else:
                logger.error(f"❌ Could not find apply/submit button for {year_to_scrape}-{period_val_str}.")
                take_screenshot(driver, f"error_no_button_{year_to_scrape}_{period_val_str}.png", year_download_dir)
                continue

            result_selectors = [ 
                (By.CSS_SELECTOR, "div.bzg_c > div.box"), (By.CSS_SELECTOR, ".result-box"), 
                (By.CSS_SELECTOR, ".company-box"), (By.CSS_SELECTOR, "[class*='box']"), 
                (By.CSS_SELECTOR, ".result-item"), (By.CSS_SELECTOR, ".company-item"), 
                (By.CSS_SELECTOR, "div.financial-report-item") 
            ]
            results_found = False
            for selector_type, selector_value in result_selectors:
                try:
                    WebDriverWait(driver, 45).until(EC.presence_of_element_located((selector_type, selector_value)))
                    logger.info(f"✅ Results (or at least one item) found with selector: {selector_type} = {selector_value}")
                    results_found = True
                    break
                except TimeoutException:
                    continue
            if not results_found:
                logger.warning(f"⚠️ No result items found on the page after filtering for {year_to_scrape}-{period_val_str}.")
                take_screenshot(driver, f"warning_no_results_content_{year_to_scrape}_{period_val_str}.png", year_download_dir)
                continue

            current_page = 1
            total_processed = 0
            
            while True: 
                logger.info(f"📄 Processing page {current_page} for {year_to_scrape}-{period_val_str}")
                
                company_boxes = []
                for selector_type, selector_value in result_selectors:
                    try:
                        boxes = driver.find_elements(selector_type, selector_value)
                        if boxes:
                            company_boxes.extend(boxes)
                            logger.debug(f"Found {len(boxes)} companies on page {current_page} with selector: {selector_value}")
                            break
                    except Exception as e:
                        logger.debug(f"Error finding company boxes with {selector_value}: {e}")
                        continue
                        
                if not company_boxes:
                    logger.info(f"No more companies found on page {current_page} for {year_to_scrape}-{period_val_str}. Moving to next period.")
                    break

                for box_idx, box in enumerate(company_boxes):
                    company_code = f"UNKNOWN_{year_to_scrape}_{period_val_str}_P{current_page}_B{box_idx}"
                    extracted_company_dir = None # Inisialisasi untuk pembersihan
                    try:
                        code_selectors = [ 
                            "div.box-title span", ".company-code", ".company-name span", "h3 span", 
                            "h4 span", "[class*='title'] span", "[class*='code']", "span.company-ticker"
                        ]
                        company_element = None
                        for code_selector in code_selectors:
                            try:
                                company_element = box.find_element(By.CSS_SELECTOR, code_selector)
                                if company_element and company_element.text.strip():
                                    company_code = company_element.text.strip()
                                    break
                            except NoSuchElementException:
                                continue
                            except Exception as e:
                                logger.debug(f"Error finding code with {code_selector} for box {box_idx}: {e}")
                                continue
                                
                        if company_element: logger.info(f"🏢 Processing: {company_code}")
                        else: logger.warning(f"Could not extract company code for a box on page {current_page}. Using fallback: {company_code}")

                        link_selectors = [ 
                            "a.link-download", "a[href*='instance.zip']", "a[href*='instance']",
                            "a[href*='.zip']", ".download-link", "[class*='download']",
                            "a[download]", "a[title*='Unduh']", "a[title*='Download']"
                        ]
                        download_links_in_box = []
                        for link_selector in link_selectors:
                            try:
                                links = box.find_elements(By.CSS_SELECTOR, link_selector)
                                download_links_in_box.extend(links)
                            except Exception as e:
                                logger.debug(f"Error finding links with {link_selector} for box {box_idx}: {e}")
                                continue
                                
                        instance_download_attempted = False
                        for link_el in download_links_in_box: 
                            try:
                                href = link_el.get_attribute('href')
                                if href and ('instance.zip' in href.lower() or 'instance' in href.lower()): 
                                    logger.info(f"🔗 Found potential instance download link for {company_code}. Attempting download via Selenium.")
                                    
                                    zip_filepath_renamed = download_file_via_selenium_click(driver, link_el, company_code, year_download_dir)
                                    
                                    if zip_filepath_renamed and os.path.exists(zip_filepath_renamed):
                                        # Simpan path direktori hasil ekstrak untuk pembersihan nanti
                                        extracted_company_dir = os.path.join(year_download_dir, company_code)
                                        xml_content = extract_and_cleanup_zip(zip_filepath_renamed, company_code, year_download_dir)
                                        
                                        if xml_content:
                                            json_data = xml_to_json_flat(xml_content)
                                            save_to_mongodb(json_data, company_code, year_to_scrape, period_val_str, mongo_uri)
                                            total_processed += 1
                                            
                                            # --- PEMBARUAN: HAPUS FOLDER EMITEN SETELAH BERHASIL DISIMPAN KE MONGO DB ---
                                            if extracted_company_dir and os.path.exists(extracted_company_dir):
                                                try:
                                                    shutil.rmtree(extracted_company_dir)
                                                    logger.info(f"🗑️ Cleaned up extracted company directory: {extracted_company_dir}")
                                                except Exception as clean_e:
                                                    logger.warning(f"Failed to remove extracted company directory {extracted_company_dir}: {clean_e}")
                                            # --- AKHIR PEMBARUAN ---
                                        else:
                                            logger.warning(f"No XML content extracted or converted for {company_code} after download.")
                                    else:
                                        logger.warning(f"ZIP file not found or failed download for {company_code}. Skipping extraction for this company.")

                                    instance_download_attempted = True
                                    break 
                            except Exception as link_error:
                                logger.error(f"❌ Link processing failed for {company_code}: {link_error}")

                        if not instance_download_attempted:
                            logger.info(f"No relevant instance download link found or download attempted for {company_code} on page {current_page}.")

                    except Exception as e_box:
                        logger.error(f"❌ Failed to process company box for {company_code}: {e_box}", exc_info=True)

                # --- DEBUG MODE: Stops after processing the first page ---
                # Hapus atau komentari 2 baris ini untuk menjalankan seluruh halaman
                logger.info("💡 DEBUG MODE: Stopping after processing the first page of results.")
            
                # --- END OF DEBUG MODE MODIFICATION ---
                
                # Logic untuk navigasi ke halaman berikutnya (akan dilewati jika break di atas aktif)
                next_selectors = [ 
                    (By.CSS_SELECTOR, "a[rel='next']"), (By.CSS_SELECTOR, ".next-page"), 
                    (By.CSS_SELECTOR, ".pagination-next"), (By.XPATH, "//a[contains(text(), 'Next')]"), 
                    (By.XPATH, "//a[contains(text(), 'Selanjutnya')]"), (By.CSS_SELECTOR, "li.paginate_button.next a"), 
                    (By.CSS_SELECTOR, "[class*='next'] a")
                ]
                next_button = find_element_with_multiple_selectors(driver, next_selectors, timeout=5)
                
                if next_button and next_button.is_enabled() and "disabled" not in next_button.get_attribute("class").lower():
                    try:
                        driver.execute_script("arguments[0].click();", next_button)
                        logger.info("➡️ Moving to next page...")
                        time.sleep(random.uniform(3, 5))
                        current_page += 1
                        wait_for_page_load(driver)
                    except Exception as next_error:
                        logger.error(f"Failed to click next button: {next_error}")
                        break
                else:
                    logger.info("🏁 No more pages or next button disabled. Finished for this period.")
                    break

            logger.info(f"✅ Completed {year_to_scrape}-{period_val_str}. Total processed: {total_processed} companies.")

        except Exception as e_period:
            logger.error(f"❌ Failed to process {year_to_scrape}-{period_val_str}: {e_period}", exc_info=True)
            take_screenshot(driver, f"error_{year_to_scrape}_{period_val_str}.png", year_download_dir)
            continue

# === MAIN ENTRY POINT ===

def main_extraction_task(**kwargs):
    """
    Fungsi utama yang dipanggil oleh Airflow DAG.
    """
    year_to_process = kwargs.get('year')
    all_periods_string = kwargs.get('periods', DEFAULT_PERIODS) 
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://mongodb-external:27017/") # Ambil dari env var sebagai default

    logger.info(f"🚀 Starting extraction task for Year: {year_to_process}")
    logger.info(f"Periods to process: {all_periods_string}")
    logger.info(f"MongoDB URI (host/port): {mongo_uri.split('@')[-1] if '@' in mongo_uri else mongo_uri.split('//')[-1].split('/')[0]}")

    year_download_dir = ensure_year_download_dir(year_to_process)

    driver = None
    try:
        # Pembersihan file dan direktori sementara sebelum memulai proses tahun ini
        for item in os.listdir(year_download_dir):
            item_path = os.path.join(year_download_dir, item)
            # Hapus file ZIP/part/tmp/png yang tersisa dari percobaan sebelumnya
            if os.path.isfile(item_path) and item.lower().endswith((".zip", ".part", ".tmp", ".png")):
                try:
                    os.remove(item_path)
                    logger.info(f"Removed transient file before extraction: {item}")
                except Exception as e:
                    logger.warning(f"Failed to remove transient file {item}: {e}")
            # Hapus juga direktori emiten yang mungkin kosong atau parsial dari run sebelumnya
            # Ini hanya untuk direktori yang tidak berisi file lagi (sudah dibersihkan per emiten)
            elif os.path.isdir(item_path) and not any(os.listdir(item_path)):
                try:
                    shutil.rmtree(item_path)
                    logger.info(f"Removed empty directory: {item}")
                except Exception as e:
                    logger.warning(f"Failed to remove empty directory {item}: {e}")


        driver = init_driver(year_download_dir)
        scrape_financial_data(driver, mongo_uri, year_to_process, all_periods_string, year_download_dir)
        
        logger.info(f"✅ Extraction task for Year {year_to_process} completed successfully.")
        
        return {
            'status': 'completed',
            'year': year_to_process,
            'periods': all_periods_string,
            'download_dir': year_download_dir,
            'message': f'Successfully processed year {year_to_process}'
        }

    except Exception as e:
        error_msg = f"🔥 Fatal error in extraction task for year {year_to_process}: {e}"
        logger.error(error_msg, exc_info=True)
        raise Exception(error_msg)
        
    finally:
        if driver:
            try:
                driver.quit()
                logger.info("🚪 Browser closed.")
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")