from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import pymongo
import json
import logging
import time
import os
import pickle
from pymongo import MongoClient, errors
import psutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Application constants
MONGODB_URI = "mongodb://mongodb-external:27017/"
DB_EXTRACT = "Yfinance_Extract"
BATCH_SIZE = 1000
TEMP_DIR = "/opt/airflow/data/temp"

# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# ======= UTILITY FUNCTIONS =======

def log_memory_usage(task_name):
    """Log current memory usage"""
    process = psutil.Process()
    memory_info = process.memory_info()
    logger.info(f"[{task_name}] Memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")

def get_mongo_client(retries=3, retry_delay=5):
    """Create MongoDB connection with retry mechanism"""
    for attempt in range(retries + 1):
        try:
            logger.info(f"Attempting to connect to MongoDB (attempt {attempt+1}/{retries+1})...")
            client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=30000
            )
            client.admin.command('ping')
            logger.info("✅ Successfully connected to MongoDB!")
            return client
        except (errors.ConnectionFailure, errors.ServerSelectionTimeoutError) as e:
            if attempt < retries:
                logger.warning(f"⚠️ MongoDB connection failed: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ Failed to connect to MongoDB after {retries} attempts: {e}")
                raise

def estimate_execution_time(total_stocks):
    """Estimate execution time based on the number of stocks."""
    # Conservative estimate:
    # - 1 second delay per stock (more polite to yfinance)
    # - 7.5 seconds processing per stock (download + insert)
    # - Buffer for error handling
    
    delay_time = total_stocks * 1  # seconds
    processing_time = total_stocks * 7.5  # average 7.5 seconds per stock
    buffer_time = total_stocks * 2  # 2 seconds buffer per stock
    
    total_seconds = delay_time + processing_time + buffer_time
    total_minutes = total_seconds / 60
    
    logger.info(f"⏱️ ESTIMATED EXECUTION TIME:")
    logger.info(f"   📊 Total stocks: {total_stocks}")
    logger.info(f"   ⏳ Delay time: {delay_time:.0f} seconds")
    logger.info(f"   🔄 Processing time: {processing_time:.0f} seconds")
    logger.info(f"   🛡️ Buffer time: {buffer_time:.0f} seconds")
    logger.info(f"   ⏱️ Total estimated: {total_minutes:.1f} minutes ({total_seconds/3600:.1f} hours)")
    
    return total_seconds

# ======= EXTRACT TASK =======

def extract_data_from_yfinance(**kwargs):
    """Extract stock data from YFinance and save to MongoDB"""
    start_time = time.time()
    
    # Initialize lists to store processed/failed stock information
    processed_stocks = []
    failed_stocks = []

    try:
        logger.info("🚀 Starting YFinance data extraction")
        log_memory_usage("extract_start")
        
        # Read stock list
        daftar_saham_path = '/opt/airflow/data/Daftar_Saham.csv'
        if not os.path.exists(daftar_saham_path):
            logger.error(f"File {daftar_saham_path} not found!")
            raise FileNotFoundError(f"File {daftar_saham_path} not found!")
            
        data = pd.read_csv(daftar_saham_path)
        logger.info(f"📋 Total stocks found in CSV: {len(data)}")
        
        client = get_mongo_client()
        db = client[DB_EXTRACT]
        
        # Set end_date to current date/time, start_date fixed
        end_date = datetime.now() 
        start_date = "2014-01-01"
        logger.info(f"🗓️ Data range: {start_date} to {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Limit number of stocks for testing (set to None for production)
        MAX_SAHAM = None  # Set to a small number for testing, e.g., 50; None for all
        
        if MAX_SAHAM:
            total_to_process = min(MAX_SAHAM, len(data))
            logger.info(f"🔒 Testing mode: processing only the first {total_to_process} stocks")
        else:
            total_to_process = len(data)
            logger.info(f"🔄 Production mode: processing all {total_to_process} stocks")
        
        # Estimate execution time
        estimate_execution_time(total_to_process)
        
        total_saham_diambil = 0
        total_data_dalam_db = 0
        
        for idx, row in data.iterrows():
            if MAX_SAHAM and idx >= MAX_SAHAM:
                logger.info(f"🔒 Limiting execution to the first {MAX_SAHAM} stocks.")
                break
                
            kode_saham = row['Kode'] + '.JK'
            nama_perusahaan_raw = row['Nama Perusahaan']
            # Sanitize company name for MongoDB collection (replace spaces with underscores)
            nama_perusahaan_collection = nama_perusahaan_raw.replace(" ", "_")
            
            # Progress indicator
            progress = ((idx + 1) / total_to_process) * 100
            elapsed_time = time.time() - start_time
            
            if idx > 0:  # Avoid division by zero
                avg_time_per_stock = elapsed_time / idx
                remaining_stocks = total_to_process - idx
                eta_seconds = remaining_stocks * avg_time_per_stock
                eta_minutes = eta_seconds / 60
                logger.info(f"📈 Progress: {progress:.1f}% | ETA: {eta_minutes:.1f} minutes")
            
            try:
                logger.info(f"📊 [{idx+1}/{total_to_process}] Fetching data for {kode_saham} ({nama_perusahaan_raw})")
                
                collection = db[nama_perusahaan_collection]
                existing_count = collection.count_documents({})
                
                # Option to skip existing collections or overwrite
                # if existing_count > 0:
                #     logger.info(f"⚠️ Collection {nama_perusahaan_collection} already has {existing_count} records. Skipping.")
                #     continue
                
                ticker = yf.Ticker(kode_saham)
                
                logger.info(f"⏳ Downloading historical data for {kode_saham}...")
                # Fetch historical data directly from start_date to end_date
                hist_combined = ticker.history(start=start_date, end=end_date)
                time.sleep(1)  # Add a delay to avoid YFinance rate limits

                if hist_combined.empty:
                    logger.warning(f"⚠️ No data found for {kode_saham}")
                    failed_stocks.append({
                        'kode_saham': kode_saham,
                        'nama_perusahaan': nama_perusahaan_raw,
                        'reason': 'NO_DATA'
                    })
                    continue
                
                logger.info(f"📊 Data received: {len(hist_combined)} records")
                logger.info(f"📅 Period: {hist_combined.index[0].strftime('%Y-%m-%d')} to {hist_combined.index[-1].strftime('%Y-%m-%d')}")
                
                hist_combined.reset_index(inplace=True)
                
                # Delete old data if existing (to prevent duplication on re-run)
                if existing_count > 0:
                    logger.info(f"🗑️ Deleting {existing_count} old records for {nama_perusahaan_collection}...")
                    collection.delete_many({})
                
                # Insert data in batches
                inserted = 0
                total_batches = (len(hist_combined) + BATCH_SIZE - 1) // BATCH_SIZE
                
                for i in range(0, len(hist_combined), BATCH_SIZE):
                    batch_num = (i // BATCH_SIZE) + 1
                    batch = hist_combined.iloc[i:i+BATCH_SIZE]
                    
                    # Convert to JSON, ensuring all types are JSON-serializable
                    # Using default_handler=str handles datetime objects
                    data_json = json.loads(batch.to_json(orient="records", date_format="iso", default_handler=str))

                    if data_json:
                        try:
                            collection.insert_many(data_json, ordered=False) # ordered=False allows partial inserts on error
                            inserted += len(data_json)
                            logger.info(f"✅ Batch {batch_num}/{total_batches}: {len(data_json)} records inserted")
                        except pymongo.errors.BulkWriteError as e:
                            logger.warning(f"⚠️ Batch {batch_num}: {len(e.details.get('writeErrors', []))} records failed in bulk write")
                            inserted += len(data_json) - len(e.details.get('writeErrors', []))
                
                total_saham_diambil += 1
                total_data_dalam_db += inserted
                processed_stocks.append({
                    'collection_name': nama_perusahaan_collection,
                    'company_name': nama_perusahaan_raw,
                    'stock_code': kode_saham
                })
                
                logger.info(f"✅ Data for {kode_saham} successfully saved: {inserted} records")
                
                # Explicitly clear DataFrame to free memory
                del hist_combined 
                
            except Exception as e:
                logger.error(f"❌ Failed to fetch or store data for {kode_saham}: {e}")
                failed_stocks.append({
                    'kode_saham': kode_saham,
                    'nama_perusahaan': nama_perusahaan_raw,
                    'reason': str(e)
                })
        
        # Save list of successfully processed stocks for downstream DAGs
        processed_stocks_file = f"{TEMP_DIR}/processed_stocks.pkl"
        failed_stocks_file = f"{TEMP_DIR}/failed_stocks.json"
        
        with open(processed_stocks_file, "wb") as f:
            pickle.dump(processed_stocks, f)
            
        with open(failed_stocks_file, "w") as f:
            json.dump(failed_stocks, f, indent=2)
            
        logger.info(f"🗳️ List of processed stocks saved to: {processed_stocks_file}")
        logger.info(f"⚠️ List of failed stocks saved to: {failed_stocks_file}")
        
        # Final statistics
        total_time = time.time() - start_time
        avg_time_per_stock = total_time / max(total_saham_diambil, 1)
        
        logger.info(f"📊 EXTRACTION SUMMARY:")
        logger.info(f"   ✅ Successful: {total_saham_diambil} stocks")
        logger.info(f"   ❌ Failed: {len(failed_stocks)} stocks")
        logger.info(f"   📊 Total records inserted: {total_data_dalam_db:,}")
        logger.info(f"   ⏱️ Total time: {total_time/60:.1f} minutes")
        logger.info(f"   ⚡ Avg time per stock: {avg_time_per_stock:.1f} seconds")
        
        if total_saham_diambil > 0:
            avg_records = total_data_dalam_db / total_saham_diambil
            logger.info(f"   📈 Avg records per stock: {avg_records:.0f}")
        
        log_memory_usage("extract_end")
        
    except Exception as e:
        logger.error(f"🚨 Critical error in extract_data_from_yfinance: {e}")
        raise
    finally:
        # Close MongoDB connection
        if 'client' in locals() and client:
            client.close()
            logger.info("MongoDB connection closed.")

# --- DAG Definition ---
default_args_extract = {
    'owner': 'airflow',
    'retries': 1,  # Reduced retries for faster failure feedback during development
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=8),  # Increased timeout for potentially large data sets
    'email_on_failure': False,
    'email_on_retry': False,
}

# --- IMPORTANT SCHEDULE ADJUSTMENT ---
# Bandung, West Java, Indonesia is WIB (Western Indonesian Time), which is UTC+7.
# To run daily at 06:00 WIB, we need to convert this to UTC.
# 06:00 WIB - 7 hours = 23:00 UTC of the *previous* day.
# So, the cron expression should be '0 23 * * *'.
# For example, if you want it to run on Tuesday at 06:00 WIB, the Airflow scheduler will trigger it
# on Monday at 23:00 UTC.

with DAG(
    dag_id='etl_yfinance_extract_fixed_dag',
    description='DAG for Stock Data Extraction from YFinance to MongoDB',
    schedule_interval='0 23 * * *', # Run daily at 23:00 UTC (06:00 WIB the next day)
    start_date=days_ago(1), # This is good for immediate testing/initial run
    catchup=False, # Do not backfill for past missed runs
    default_args=default_args_extract,
    max_active_runs=1,
    tags=['saham', 'yfinance', 'mongodb', 'extract', 'fixed'],
) as dag_extract:
    
    extract_yfinance_data_task = PythonOperator(
        task_id='extract_yfinance_data',
        python_callable=extract_data_from_yfinance,
        pool='default_pool', # Use a resource pool if configured
    )

    extract_yfinance_data_task