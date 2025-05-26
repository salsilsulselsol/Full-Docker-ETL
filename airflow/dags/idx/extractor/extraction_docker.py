import os
import time
import io
import pandas as pd
import requests
import zipfile
import random
import xml.etree.ElementTree as ET
import json
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from pymongo import MongoClient
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 🔧 CONFIGURATION
DOWNLOAD_DIR = "/app/downloads" 
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
IDX_URL = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan/"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo_db:27017/") 
DB_NAME = "idx_financial_data_staging" 
COLLECTION_NAME_PREFIX = "reports" 

def init_driver(download_dir_container):
    logger.info("Initializing WebDriver...")
    options = Options()
    options.add_argument("--headless") 
    options.add_argument("--no-sandbox") 
    options.add_argument("--disable-dev-shm-usage") 
    options.add_argument("--disable-gpu") 
    options.add_argument("window-size=1920x1080") 
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", download_dir_container) 
    options.set_preference("browser.helperApps.neverAsk.saveToDisk",
                           "application/zip,application/octet-stream,application/x-zip-compressed,multipart/x-zip,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/pdf,application/vnd.ms-excel,application/xbrl+xml,application/xml,text/xml")
    
    try:
        driver = webdriver.Firefox(options=options) 
        logger.info(f"WebDriver initialized. Downloads will be saved to: {download_dir_container}")
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {e}")
        logger.error("Ensure Firefox and Geckodriver are correctly installed and in PATH within the Docker container.")
        raise

def extract_xml_from_zip(zip_content_bytes):
    try:
        logger.info("🔓 Extracting XML/XBRL from ZIP...")
        with zipfile.ZipFile(io.BytesIO(zip_content_bytes)) as zip_file:
            potential_files = [f for f in zip_file.namelist() if f.lower().endswith(('.xml', '.xbrl'))]
            if not potential_files:
                logger.warning("❌ No XML or XBRL files found in ZIP.")
                return None
            xbrl_files = [f for f in potential_files if f.lower().endswith('.xbrl')]
            if xbrl_files:
                preferred_xbrl = [f for f in xbrl_files if 'instance' in f.lower() or 'financial' in f.lower()]
                target_file = preferred_xbrl[0] if preferred_xbrl else xbrl_files[0]
            else: 
                preferred_xml = [f for f in potential_files if 'instance' in f.lower() or 'financial' in f.lower()]
                target_file = preferred_xml[0] if preferred_xml else potential_files[0]
            logger.info(f"Selected file for extraction: {target_file}")
            xml_content = zip_file.read(target_file)
        logger.info("✅ XML/XBRL extracted successfully.")
        return xml_content
    except Exception as e:
        logger.error(f"❌ Error extracting XML/XBRL from ZIP: {e}", exc_info=True)
        return None

def remove_namespace(tag):
    return tag.split('}')[-1] if '}' in tag else tag

def xml_to_json_flat(xml_content_bytes):
    try:
        logger.info("🔄 Converting XML to flat JSON...")
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
            logger.warning("JSON data is empty after conversion.")
        logger.info("✅ XML converted to JSON successfully (flat structure).")
        return json_data
    except ET.ParseError as e:
        logger.error(f"❌ XML ParseError converting XML to JSON: {e}")
        logger.error(f"Problematic XML content (first 500 chars): {xml_content_str[:500] if 'xml_content_str' in locals() else 'Content not decoded'}")
        return None
    except Exception as e:
        logger.error(f"❌ General error converting XML to JSON: {e}", exc_info=True)
        return None

def save_to_mongodb(data_to_save, company_code, year_str, period_str):
    if data_to_save is None:
        logger.warning(f"No data to save for {company_code} - {year_str} {period_str}")
        return
    try:
        collection_name = f"{COLLECTION_NAME_PREFIX}_{year_str}_{period_str}"
        logger.info(f"💾 Saving data for {company_code} - {year_str} - {period_str} to MongoDB collection '{collection_name}'...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000) 
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
        logger.info(f"✅ Data saved/updated for {company_code} - {year_str} {period_str}")
    except Exception as e:
        logger.error(f"❌ Error saving data to MongoDB for {company_code}: {e}", exc_info=True)
    finally:
        if 'client' in locals() and client: client.close()

def scrape_financial_data(driver, download_dir_container):
    scrape_years_str = os.environ.get("SCRAPE_YEARS", "2023") 
    scrape_periods_str = os.environ.get("SCRAPE_PERIODS", "audit,tw1") 
    
    years_to_scrape = [y.strip() for y in scrape_years_str.split(',')]
    periods_to_scrape = [p.strip().lower() for p in scrape_periods_str.split(',')]

    logger.info(f"Starting scrape for Years: {years_to_scrape}, Periods: {periods_to_scrape}")

    for year_val_str in years_to_scrape:
        for period_val_str in periods_to_scrape: 
            logger.info(f"\n🌐 Scraping: Year {year_val_str}, Period {period_val_str}")
            
            max_page_load_retries = 3
            for attempt in range(max_page_load_retries):
                try:
                    driver.get(IDX_URL)
                    time.sleep(random.uniform(3, 5)) 
                    
                    logger.info("Waiting for year input elements to be visible...")
                    WebDriverWait(driver, 50).until( 
                        EC.visibility_of_element_located((By.CSS_SELECTOR, f"input[name='year'][value='{year_val_str}']"))
                    )
                    logger.info(f"✅ Page loaded for Year {year_val_str}, Period {period_val_str} (Attempt {attempt + 1})")

                    year_radio_css = f"input[name='year'][value='{year_val_str}']"
                    year_radio = WebDriverWait(driver, 35).until( 
                        EC.element_to_be_clickable((By.CSS_SELECTOR, year_radio_css))
                    )
                    driver.execute_script("arguments[0].click();", year_radio)
                    logger.info(f"✅ Selected year {year_val_str}")
                    time.sleep(random.uniform(2, 4)) 

                    period_radio_css = f"input[name='period'][value='{period_val_str}']"
                    logger.info(f"Attempting to select period with CSS: {period_radio_css}")
                    period_radio = WebDriverWait(driver, 35).until( 
                        EC.element_to_be_clickable((By.CSS_SELECTOR, period_radio_css))
                    )
                    driver.execute_script("arguments[0].click();", period_radio)
                    logger.info(f"✅ Selected period {period_val_str}")
                    time.sleep(random.uniform(2, 4))

                    apply_button_css = "button.btn--primary" 
                    logger.info(f"Attempting to click apply button with CSS: {apply_button_css}")
                    apply_button = WebDriverWait(driver, 35).until( 
                        EC.element_to_be_clickable((By.CSS_SELECTOR, apply_button_css))
                    )
                    driver.execute_script("arguments[0].scrollIntoView(true);", apply_button) 
                    time.sleep(random.uniform(0.5, 1))
                    driver.execute_script("arguments[0].click();", apply_button)
                    logger.info("✅ Clicked 'Terapkan' button")
                    logger.info("Waiting for results to load after applying filter (e.g., 10-15 seconds)...")
                    time.sleep(random.uniform(10, 15)) 

                    current_page_num = 1
                    while True: 
                        logger.info(f"\n📄 Processing page {current_page_num} for {year_val_str} - {period_val_str}")
                        
                        box_css_selector = "div.bzg_c > div.box" 
                        
                        logger.info(f"Waiting for company boxes with selector: {box_css_selector}")
                        try:
                            WebDriverWait(driver, 45).until( 
                                EC.visibility_of_element_located((By.CSS_SELECTOR, box_css_selector))
                            )
                            logger.info("✅ At least one company box is visible.")
                            company_boxes = driver.find_elements(By.CSS_SELECTOR, box_css_selector)
                            logger.info(f"Found {len(company_boxes)} company boxes on page.")
                        except Exception as e_box_wait: 
                            logger.warning(f"No company boxes found or timed out on page {current_page_num}. Error: {e_box_wait}")
                            screenshot_path = os.path.join(download_dir_container, f"debug_no_boxes_{year_val_str}_{period_val_str}_page{current_page_num}.png")
                            try:
                                driver.save_screenshot(screenshot_path)
                                logger.info(f"📷 Debug screenshot saved to: {screenshot_path}")
                            except Exception as e_ss:
                                logger.error(f"Failed to save debug screenshot: {e_ss}")
                            company_boxes = [] 

                        if not company_boxes:
                            logger.info(f"No company boxes to process on page {current_page_num}. End of results for {year_val_str}-{period_val_str}.")
                            break

                        for box_idx, company_box in enumerate(company_boxes):
                            company_code_str = "UnknownCompany"
                            try:
                                company_code_elem = company_box.find_element(By.CSS_SELECTOR, "div.box-title span.f-20.f-m-30")
                                company_code_str = company_code_elem.text.strip()
                                logger.info(f"🏢 Processing company: {company_code_str} (Box {box_idx + 1}/{len(company_boxes)})")

                                download_links = company_box.find_elements(By.CSS_SELECTOR, "table.table a.link-download")
                                instance_zip_link_found = False
                                for link_elem in download_links:
                                    href_attr = link_elem.get_attribute("href")
                                    td_parent = link_elem.find_element(By.XPATH, "..") 
                                    file_name_text = td_parent.text.strip().lower() 
                                    
                                    if href_attr and ('instance.zip' in href_attr.lower() or 'instance.zip' in file_name_text):
                                        logger.info(f"🔗 Found 'instance.zip' link for {company_code_str}: {href_attr} (File name text: {file_name_text})")
                                        
                                        default_download_filename = "instance.zip"
                                        specific_download_path = os.path.join(download_dir_container, default_download_filename)
                                        if os.path.exists(specific_download_path):
                                            try: os.remove(specific_download_path)
                                            except OSError as e_remove: logger.error(f"Error removing old file: {e_remove}")
                                        
                                        driver.execute_script("arguments[0].scrollIntoView(true);", link_elem)
                                        time.sleep(random.uniform(0.5,1))
                                        # --- PERUBAHAN: Menggunakan JavaScript click ---
                                        driver.execute_script("arguments[0].click();", link_elem)
                                        # -------------------------------------------
                                        logger.info(f"✅ Clicked download for {company_code_str} using JavaScript.")
                                        
                                        download_timeout_seconds = 90; time_waited_seconds = 0; downloaded_file_path = None
                                        if os.path.exists(specific_download_path): downloaded_file_path = specific_download_path
                                        
                                        if not downloaded_file_path:
                                            logger.info(f"Waiting up to {download_timeout_seconds}s for new zip file...")
                                            initial_files = set(os.listdir(download_dir_container))
                                            while time_waited_seconds < download_timeout_seconds:
                                                time.sleep(2); time_waited_seconds += 2
                                                current_files = set(os.listdir(download_dir_container))
                                                new_files = current_files - initial_files
                                                new_zip_files = [f for f in new_files if f.lower().endswith(".zip")]
                                                if new_zip_files:
                                                    downloaded_file_path = os.path.join(download_dir_container, new_zip_files[0])
                                                    logger.info(f"📥 New ZIP file: {downloaded_file_path}"); break
                                                if os.path.exists(specific_download_path):
                                                    downloaded_file_path = specific_download_path
                                                    logger.info(f"📥 Default file appeared: {downloaded_file_path}"); break
                                            if not downloaded_file_path: logger.error(f"❌ Download timed out for {company_code_str}.")

                                        if downloaded_file_path and os.path.exists(downloaded_file_path):
                                            logger.info(f"📥 File confirmed: {downloaded_file_path}")
                                            with open(downloaded_file_path, 'rb') as f_zip: zip_content_bytes_data = f_zip.read()
                                            xml_data_bytes_content = extract_xml_from_zip(zip_content_bytes_data)
                                            if xml_data_bytes_content:
                                                json_data_content = xml_to_json_flat(xml_data_bytes_content)
                                                if json_data_content: save_to_mongodb(json_data_content, company_code_str, year_val_str, period_val_str)
                                                else: logger.warning(f"JSON conversion failed for {company_code_str}")
                                            else: logger.warning(f"XML extraction failed for {company_code_str}")
                                            try: os.remove(downloaded_file_path); logger.info(f"🗑️ Removed: {downloaded_file_path}")
                                            except OSError as e_rm: logger.error(f"Error removing file: {e_rm}")
                                        else: logger.error(f"❌ Download processing failed for {company_code_str}")
                                        instance_zip_link_found = True; break 
                                
                                if not instance_zip_link_found: logger.info(f"No 'instance.zip' download link found for {company_code_str}")
                                time.sleep(random.uniform(1, 3)) 
                            except Exception as e_company:
                                logger.error(f"❌ Error processing box for {company_code_str}: {e_company}", exc_info=True)
                                driver.execute_script("window.scrollBy(0, 100);"); time.sleep(0.5); continue 

                        logger.info(f"✅ Finished page {current_page_num}")
                        try:
                            next_button_css = "ul.pagination li.page-item a[rel='next'], button.btn-arrow.--next"
                            # Beri waktu sedikit untuk next button muncul dan bisa diklik
                            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, next_button_css)))
                            next_button_elem = driver.find_element(By.CSS_SELECTOR, next_button_css)
                            
                            is_clickable_tag = next_button_elem.tag_name == 'a' or \
                                           (next_button_elem.tag_name == 'button' and not next_button_elem.get_attribute("disabled"))
                            
                            if next_button_elem.is_displayed() and next_button_elem.is_enabled() and is_clickable_tag:
                                logger.info("➡️ Clicking 'Next Page'...")
                                driver.execute_script("arguments[0].scrollIntoView(true);", next_button_elem)
                                time.sleep(0.5)
                                driver.execute_script("arguments[0].click();", next_button_elem)
                                time.sleep(random.uniform(4, 7)) 
                                current_page_num += 1
                            else: logger.info("Next button not active/found. Last page."); break 
                        except Exception: logger.info("No 'Next Page' button. End of pagination."); break 
                    
                    logger.info(f"✅ All pages processed for {year_val_str} - {period_val_str}")
                    break # Sukses untuk kombinasi tahun/periode ini
                
                except Exception as e_page_load:
                    logger.error(f"❌ Error during page setup for {year_val_str} - {period_val_str} (Attempt {attempt + 1}/{max_page_load_retries}): {e_page_load}", exc_info=False) 
                    if attempt < max_page_load_retries - 1:
                        logger.info(f"Retrying page load in {5 * (attempt + 1)} seconds...")
                        time.sleep(5 * (attempt + 1))
                        try: 
                            screenshot_path_retry = os.path.join(download_dir_container, f"debug_retry_before_refresh_{year_val_str}_{period_val_str}_attempt{attempt+1}.png")
                            driver.save_screenshot(screenshot_path_retry)
                            logger.info(f"📷 Debug screenshot (before refresh on retry) saved to: {screenshot_path_retry}")
                            driver.refresh(); 
                            logger.info("Page refreshed."); 
                            time.sleep(3)
                        except Exception as refresh_err: logger.error(f"Failed to refresh page or take screenshot on retry: {refresh_err}")
                    else: logger.error(f"Max retries for page load {year_val_str} - {period_val_str}. Skipping.")
    logger.info("\n🚪 All specified years and periods have been processed.")

def main():
    driver = None
    try:
        logger.info(f"Preparing download directory: {DOWNLOAD_DIR}")
        for item in os.listdir(DOWNLOAD_DIR):
            if item.lower().endswith((".zip", ".part", ".crdownload")): 
                try: os.remove(os.path.join(DOWNLOAD_DIR, item)); logger.info(f"Cleaned: {item}")
                except OSError as e_cl: logger.error(f"Error cleaning {item}: {e_cl}")
        driver = init_driver(DOWNLOAD_DIR) 
        scrape_financial_data(driver, DOWNLOAD_DIR)
    except Exception as e_main:
        logger.error(f"Critical error in main execution: {e_main}", exc_info=True)
    finally:
        if driver: driver.quit(); logger.info("\n🚪 Browser closed.")

if __name__ == "__main__":
    logger.info("🚀 Starting IDX Financial Data Extraction Script...")
    main()