from airflow import DAG
from airflow.operators.python import PythonOperator
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_mongo_client():
    """Get MongoDB client with error handling"""
    try:
        logger.info("Connecting to MongoDB...")
        # client = MongoClient("mongodb://host.docker.internal:27017/")
        client = MongoClient("mongodb://mongodb-external:27017/")
        client.admin.command('ping')  # Test connection
        logger.info("Successfully connected to MongoDB!")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def setup_selenium_driver():
    """Configure and return Selenium WebDriver with robust options"""
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--ignore-certificate-errors")
        
        # Set binary location from environment
        chrome_bin = os.environ.get("CHROME_BIN", "/usr/bin/chromium")
        chrome_options.binary_location = chrome_bin
        
        # Configure ChromeDriver
        chromedriver_path = os.environ.get("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver")
        chrome_service = Service(executable_path=chromedriver_path)
        
        # Initialize WebDriver with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
                driver.set_page_load_timeout(30)
                return driver
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"WebDriver initialization failed (attempt {attempt + 1}), retrying...")
                time.sleep(5)
    except Exception as e:
        logger.error(f"Failed to setup Selenium: {e}")
        raise

def extract_article_content(driver, article_url, base_url):
    """Extract article content with robust error handling"""
    full_url = base_url + article_url if not article_url.startswith("http") else article_url
    try:
        driver.get(full_url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "zoomthis"))
        )
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        zoom_div = soup.find("div", id="zoomthis")
        if not zoom_div:
            logger.warning(f"Content not found in {full_url}")
            return None, None
        
        # Extract date
        date_element = zoom_div.find("small")
        date_text = date_element.text.strip() if date_element else "No date available"
        
        # Remove unwanted elements
        for element in zoom_div.find_all(["small", "h3", "div"]):
            element.extract()
        
        # Clean content
        content = zoom_div.get_text(separator=" ", strip=True)
        content = re.sub(r'\s+', ' ', content).strip()
        
        return date_text, content
        
    except Exception as e:
        logger.error(f"Error scraping article {full_url}: {e}")
        return None, None

def scrape_page(driver, url, base_url, collection):
    """Scrape a single page of news articles"""
    article_count = 0
    new_article_ids = []
    
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "load_news"))
        )
        time.sleep(2)  # Allow dynamic content to load
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        news_list = soup.select_one("#load_news .box ul.news")
        if not news_list:
            logger.warning("News list element not found")
            return 0, []
        
        news_items = news_list.find_all("li")
        if not news_items:
            logger.warning("No news items found in list")
            return 0, []
        
        for item in news_items:
            try:
                time_text = item.find("b").text.strip() if item.find("b") else "No time"
                title_tag = item.find("a")
                
                if not title_tag or not title_tag.has_attr("href"):
                    continue
                
                title = title_tag.text.strip()
                link = title_tag["href"]
                
                # Check if article already exists
                if collection.find_one({"judul": title}):
                    continue
                
                # Extract article content
                article_date, article_content = extract_article_content(driver, link, base_url)
                if not article_content:
                    continue
                
                # Prepare article data
                article_data = {
                    "judul": title,
                    "waktu": time_text,
                    "link": base_url + link if not link.startswith("http") else link,
                    "tanggal_artikel": article_date,
                    "konten": article_content,
                    "tanggal_scrape": datetime.now().isoformat(),
                    "status_transformasi": "belum"
                }
                
                # Save to database
                result = collection.insert_one(article_data)
                new_article_ids.append(result.inserted_id)
                article_count += 1
                logger.info(f"Saved new article: {title[:50]}...")
                
            except Exception as e:
                logger.error(f"Error processing news item: {e}")
                continue
        
        return article_count, new_article_ids
        
    except Exception as e:
        logger.error(f"Error scraping page {url}: {e}")
        return 0, []

def extract_news_from_iqplus(news_category: str, news_url_path: str, **kwargs):
    """Main extraction function with comprehensive error handling for a specific news category"""
    driver = None # Initialize driver to None
    try:
        logger.info(f"Starting IQPlus news extraction for category: {news_category}")
        
        # Setup MongoDB
        client = get_mongo_client()
        db = client["Iqplus"]
        collection = db["Iqplus_News_Extract"] 
        
        # Configuration
        base_url = "http://www.iqplus.info"
        start_url = f"{base_url}{news_url_path}/go-to-page,0.html"
        max_pages = int(os.getenv(f'MAX_PAGES_{news_category.upper().replace(" ", "_")}', '210')) # Env var for max pages per category
        
        # Setup Selenium
        driver = setup_selenium_driver()
        
        # Get total pages to scrape
        def get_last_page():
            try:
                driver.get(start_url)
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "nav"))
                )
                
                soup = BeautifulSoup(driver.page_source, "html.parser")
                nav_span = soup.find("span", class_="nav")
                if nav_span:
                    # Find all page links, the second to last is usually the last page number
                    page_links = nav_span.find_all("a")
                    if len(page_links) >= 2:
                        last_page_link = page_links[-2] 
                        if last_page_link and last_page_link.text.isdigit():
                            return min(int(last_page_link.text), max_pages)
            except Exception as e:
                logger.error(f"Error getting last page for {news_category}: {e}")
            return 1  # Fallback to just first page
        
        last_page = get_last_page()
        logger.info(f"Will scrape {last_page} pages for {news_category}")
        
        # Scrape pages
        total_articles = 0
        all_new_article_ids = []
        
        for page in range(0, last_page + 1):
            # Adjust URL structure based on how IQPlus handles pagination for each category
            # Assuming it's consistent: /news/category/go-to-page,X.html
            page_url = f"{base_url}{news_url_path}/go-to-page,{page}.html"
            logger.info(f"Scraping page: {page_url} for {news_category}")
            
            articles_scraped, new_ids = scrape_page(driver, page_url, base_url, collection)
            total_articles += articles_scraped
            all_new_article_ids.extend(new_ids)
            
            logger.info(f"Articles scraped from page {page} ({news_category}): {articles_scraped}")
            time.sleep(2)  # Be polite to the server
        
        logger.info(f"Finished scraping {news_category}. Total new articles: {total_articles}")
        
        # Push results to XCom
        kwargs['ti'].xcom_push(key=f'total_extracted_articles_{news_category.lower().replace(" ", "_")}', value=total_articles)
        kwargs['ti'].xcom_push(key=f'new_article_ids_{news_category.lower().replace(" ", "_")}', value=[str(id) for id in all_new_article_ids])
        
    except Exception as e:
        logger.error(f"Extraction for {news_category} failed: {e}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Error closing WebDriver: {e}")

# DAG Definition
with DAG(
    dag_id='iqplus_scraper_parallel', # Changed DAG ID to reflect parallel scraping
    description='Robust IQPlus news scraper for Market and Stock news in parallel',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(minutes=60),
    },
    tags=['iqplus', 'news', 'scraper', 'parallel'],
) as dag:

    # Define tasks for Market News
    extract_market_news_task = PythonOperator(
        task_id='extract_market_news',
        python_callable=extract_news_from_iqplus,
        op_kwargs={
            'news_category': 'Market News',
            'news_url_path': '/news/market_news', # Specific path for Market News
        },
        provide_context=True,
    )

    # Define tasks for Stock News
    extract_stock_news_task = PythonOperator(
        task_id='extract_stock_news',
        python_callable=extract_news_from_iqplus,
        op_kwargs={
            'news_category': 'Stock News',
            'news_url_path': '/news/stock_news', # Specific path for Stock News
        },
        provide_context=True,
    )

    # You can add more tasks for other categories if needed
    # extract_other_news_task = PythonOperator(
    #     task_id='extract_other_news',
    #     python_callable=extract_news_from_iqplus,
    #     op_kwargs={
    #         'news_category': 'Other News',
    #         'news_url_path': '/news/other_category', 
    #     },
    #     provide_context=True,
    # )

    # Set task dependencies
    # Since these are independent, they can run in parallel
    [extract_market_news_task, extract_stock_news_task]