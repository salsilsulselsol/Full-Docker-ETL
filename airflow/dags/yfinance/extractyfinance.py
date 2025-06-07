# dag_extract_yfinance_fixed.py

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

# Pengaturan logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfigurasi konstanta aplikasi
MONGODB_URI = "mongodb://mongodb-external:27017/" 
DB_EXTRACT = "Yfinance_Extract"
BATCH_SIZE = 1000
TEMP_DIR = "/opt/airflow/data/temp" 

# Pastikan direktori temp ada
os.makedirs(TEMP_DIR, exist_ok=True)

# ======= FUNGSI UTILITAS =======

def log_memory_usage(task_name):
    """Log penggunaan memori saat ini"""
    process = psutil.Process()
    memory_info = process.memory_info()
    logger.info(f"[{task_name}] Penggunaan memori: {memory_info.rss / (1024 * 1024):.2f} MB")

def get_mongo_client(retries=3, retry_delay=5):
    """Membuat koneksi MongoDB dengan mekanisme retry"""
    for attempt in range(retries + 1):
        try:
            logger.info(f"Mencoba koneksi ke MongoDB (percobaan {attempt+1}/{retries+1})...")
            client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=30000
            )
            client.admin.command('ping')
            logger.info("✅ Berhasil terhubung ke MongoDB!")
            return client
        except (errors.ConnectionFailure, errors.ServerSelectionTimeoutError) as e:
            if attempt < retries:
                logger.warning(f"⚠️ Koneksi MongoDB gagal: {e}. Mencoba lagi dalam {retry_delay} detik...")
                time.sleep(retry_delay)
            else:
                logger.error(f"❌ Gagal terhubung ke MongoDB setelah {retries} percobaan: {e}")
                raise

def estimate_execution_time(total_stocks):
    """Estimasi waktu eksekusi berdasarkan jumlah saham"""
    # Estimasi konservatif:
    # - 0.5 detik delay per saham
    # - 5-10 detik processing per saham (download + insert)
    # - Buffer untuk error handling
    
    delay_time = total_stocks * 0.5  # seconds
    processing_time = total_stocks * 7.5  # average 7.5 seconds per stock
    buffer_time = total_stocks * 2  # 2 seconds buffer per stock
    
    total_seconds = delay_time + processing_time + buffer_time
    total_minutes = total_seconds / 60
    
    logger.info(f"⏱️ ESTIMASI WAKTU EKSEKUSI:")
    logger.info(f"   📊 Total saham: {total_stocks}")
    logger.info(f"   ⏳ Delay time: {delay_time:.0f} detik")
    logger.info(f"   🔄 Processing time: {processing_time:.0f} detik")
    logger.info(f"   🛡️ Buffer time: {buffer_time:.0f} detik")
    logger.info(f"   ⏱️ Total estimasi: {total_minutes:.1f} menit ({total_seconds/3600:.1f} jam)")
    
    return total_seconds

# ======= TASK EXTRACT =======

def extract_data_from_yfinance(**kwargs):
    """Extract data saham dari YFinance dan simpan ke MongoDB"""
    start_time = time.time()
    
    try:
        logger.info("🚀 Memulai extract data dari YFinance")
        log_memory_usage("extract_start")
        
        # Baca daftar saham
        daftar_saham_path = '/opt/airflow/data/Daftar_Saham.csv'
        if not os.path.exists(daftar_saham_path):
            logger.error(f"File {daftar_saham_path} tidak ditemukan!")
            raise FileNotFoundError(f"File {daftar_saham_path} tidak ditemukan!")
            
        data = pd.read_csv(daftar_saham_path)
        logger.info(f"📋 Total saham ditemukan: {len(data)}")
        
        client = get_mongo_client()
        db = client[DB_EXTRACT]
        
        # Mengatur end_date ke tanggal dan waktu saat ini
        end_date = datetime.now() 
        # Mengatur start_date tetap dari 2014-01-01
        start_date = "2014-01-01"
        logger.info(f"🗓️ Rentang tanggal data: {start_date} hingga {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Batasi jumlah saham untuk pengujian
        MAX_SAHAM = None  # Set ke angka kecil untuk testing, None untuk semua
        
        if MAX_SAHAM:
            total_to_process = min(MAX_SAHAM, len(data))
            logger.info(f"🔒 Mode testing: hanya memproses {total_to_process} saham pertama")
        else:
            total_to_process = len(data)
            logger.info(f"🔄 Mode produksi: memproses semua {total_to_process} saham")
        
        # Estimasi waktu eksekusi
        estimate_execution_time(total_to_process)
        
        total_saham_diambil = 0
        total_data_dalam_db = 0
        processed_stocks = []  # FIX: Inisialisasi yang benar
        failed_stocks = []
        
        for idx, row in data.iterrows():
            if MAX_SAHAM and idx >= MAX_SAHAM:
                logger.info(f"🔒 Membatasi eksekusi hanya pada {MAX_SAHAM} saham pertama.")
                break
                
            kode_saham = row['Kode'] + '.JK'
            nama_perusahaan = row['Nama Perusahaan']
            nama_perusahaan = nama_perusahaan.replace(" ", "_")
            
            # Progress indicator
            progress = ((idx + 1) / total_to_process) * 100
            elapsed_time = time.time() - start_time
            
            if idx > 0:  # Avoid division by zero
                avg_time_per_stock = elapsed_time / idx
                remaining_stocks = total_to_process - idx
                eta_seconds = remaining_stocks * avg_time_per_stock
                eta_minutes = eta_seconds / 60
                logger.info(f"📈 Progress: {progress:.1f}% | ETA: {eta_minutes:.1f} menit")
            
            try:
                logger.info(f"📊 [{idx+1}/{total_to_process}] Mengambil data untuk {kode_saham} ({nama_perusahaan})")
                
                # Cek apakah collection sudah ada dan berapa banyak data
                collection = db[nama_perusahaan]
                existing_count = collection.count_documents({})
                
                if existing_count > 0:
                    logger.info(f"⚠️ Collection {nama_perusahaan} sudah memiliki {existing_count} record")
                    # Uncomment baris berikut untuk skip data yang sudah ada
                    # logger.info(f"⏭️ Melewati {kode_saham} karena data sudah ada")
                    # continue
                
                ticker = yf.Ticker(kode_saham)
                
                # Mengambil data historis langsung dari start_date hingga end_date
                logger.info(f"⏳ Mengunduh data historis...")
                hist_combined = ticker.history(start=start_date, end=end_date)
                time.sleep(0.5)  # Delay untuk menghindari rate limit
                
                if hist_combined.empty:
                    logger.warning(f"⚠️ Tidak ada data untuk {kode_saham}")
                    failed_stocks.append({
                        'kode_saham': kode_saham,
                        'nama_perusahaan': nama_perusahaan,
                        'reason': 'NO_DATA'
                    })
                    continue
                
                # Log informasi data yang diterima
                logger.info(f"📊 Data diterima: {len(hist_combined)} record")
                logger.info(f"📅 Periode: {hist_combined.index[0].strftime('%Y-%m-%d')} sampai {hist_combined.index[-1].strftime('%Y-%m-%d')}")
                
                hist_combined.reset_index(inplace=True)
                
                # Hapus data lama jika ada (untuk mencegah duplikasi)
                if existing_count > 0:
                    logger.info(f"🗑️ Menghapus {existing_count} data lama...")
                    collection.delete_many({})
                
                # Insert data dalam batch
                inserted = 0
                total_batches = (len(hist_combined) + BATCH_SIZE - 1) // BATCH_SIZE
                
                for i in range(0, len(hist_combined), BATCH_SIZE):
                    batch_num = (i // BATCH_SIZE) + 1
                    batch = hist_combined.iloc[i:i+BATCH_SIZE]
                    
                    # Pastikan semua tipe data aman untuk JSON sebelum konversi
                    data_json = json.loads(batch.to_json(orient="records", date_format="iso", default_handler=str))

                    if data_json:
                        try:
                            collection.insert_many(data_json, ordered=False)
                            inserted += len(data_json)
                            logger.info(f"✅ Batch {batch_num}/{total_batches}: {len(data_json)} record diinsert")
                        except pymongo.errors.BulkWriteError as e:
                            logger.warning(f"⚠️ Batch {batch_num}: {len(e.details.get('writeErrors', []))} record gagal")
                            inserted += len(data_json) - len(e.details.get('writeErrors', []))
                
                total_saham_diambil += 1
                total_data_dalam_db += inserted
                processed_stocks.append(nama_perusahaan)  # FIX: Tambahkan ke list
                
                logger.info(f"✅ Data {kode_saham} berhasil disimpan: {inserted} record")
                

                
                del hist_combined  # Membersihkan variabel yang tidak lagi dibutuhkan
                
            except Exception as e:
                logger.error(f"❌ Gagal ambil data untuk {kode_saham}: {e}")
                failed_stocks.append({
                    'kode_saham': kode_saham,
                    'nama_perusahaan': nama_perusahaan,
                    'reason': str(e)
                })
        
        # Simpan daftar saham yang berhasil untuk digunakan oleh DAG berikutnya
        processed_stocks_file = f"{TEMP_DIR}/processed_stocks.pkl"
        failed_stocks_file = f"{TEMP_DIR}/failed_stocks.json"
        
        with open(processed_stocks_file, "wb") as f:
            pickle.dump(processed_stocks, f)
            
        with open(failed_stocks_file, "w") as f:
            json.dump(failed_stocks, f, indent=2)
            
        logger.info(f"🗳️ Daftar saham yang diproses disimpan di: {processed_stocks_file}")
        logger.info(f"⚠️ Daftar saham yang gagal disimpan di: {failed_stocks_file}")
        
        # Statistik akhir
        total_time = time.time() - start_time
        avg_time_per_stock = total_time / max(total_saham_diambil, 1)
        
        logger.info(f"📊 RINGKASAN EKSTRAKSI:")
        logger.info(f"   ✅ Berhasil: {total_saham_diambil} saham")
        logger.info(f"   ❌ Gagal: {len(failed_stocks)} saham")
        logger.info(f"   📊 Total record: {total_data_dalam_db:,}")
        logger.info(f"   ⏱️ Total waktu: {total_time/60:.1f} menit")
        logger.info(f"   ⚡ Rata-rata per saham: {avg_time_per_stock:.1f} detik")
        
        if total_saham_diambil > 0:
            avg_records = total_data_dalam_db / total_saham_diambil
            logger.info(f"   📈 Rata-rata record per saham: {avg_records:.0f}")
        
        log_memory_usage("extract_end")
        
        # Tutup koneksi MongoDB
        client.close()
        
    except Exception as e:
        logger.error(f"🚨 Error di extract_data_from_yfinance: {e}")
        raise

# --- DAG Definition ---
default_args_extract = {
    'owner': 'airflow',
    'retries': 2,  # Kurangi retry untuk testing
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),  # Increased timeout untuk banyak saham
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='etl_yfinance_extract_fixed_dag',
    description='DAG untuk Ekstraksi Data Saham dari YFinance ke MongoDB (Fixed)',
    schedule_interval=timedelta(days=1),  # Jalan setiap hari
    start_date=days_ago(1),
    catchup=False,  # Tidak catch up untuk run yang terlewat
    default_args=default_args_extract,
    max_active_runs=1,  # Hanya 1 instance yang jalan bersamaan
    tags=['saham', 'yfinance', 'mongodb', 'extract', 'fixed'],
) as dag_extract:
    
    extract_yfinance_data_task = PythonOperator(
        task_id='extract_yfinance_data_fixed',
        python_callable=extract_data_from_yfinance,
        pool='default_pool',  # Gunakan resource pool
    )

    extract_yfinance_data_task