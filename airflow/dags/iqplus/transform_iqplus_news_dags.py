from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient
from pymongo.operations import UpdateOne
from bson.objectid import ObjectId

# Konfigurasi logging yang lebih baik
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Peningkatan Performa: Cache Model & Client ---
# Variabel global untuk menyimpan model dan koneksi agar tidak diinisialisasi berulang kali
# dalam satu proses worker Airflow.
_MODELS = None
_MONGO_CLIENT = None

def get_mongo_client():
    """Menggunakan kembali koneksi MongoClient yang sudah ada jika memungkinkan."""
    global _MONGO_CLIENT
    if _MONGO_CLIENT is None:
        try:
            logger.info("Menghubungkan ke MongoDB untuk pertama kali...")
            client = MongoClient("mongodb://mongodb-external:27017/", serverSelectionTimeoutMS=10000)
            client.admin.command('ping')
            _MONGO_CLIENT = client
            logger.info("Koneksi MongoDB berhasil dan siap digunakan!")
        except Exception as e:
            logger.error(f"Koneksi MongoDB gagal: {e}")
            raise
    return _MONGO_CLIENT

def load_models():
    """
    Memuat model NLP hanya sekali dan menyimpannya di cache.
    Ini secara drastis meningkatkan performa pada eksekusi task berikutnya.
    """
    global _MODELS
    if _MODELS is None:
        try:
            from transformers import pipeline, AutoTokenizer
            logger.info("Inisialisasi model NLP untuk pertama kali (ini mungkin lambat)...")
            
            model_name = "facebook/bart-large-cnn"
            # Pastikan model sudah ada di image docker Anda untuk performa terbaik
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            summarizer = pipeline("summarization", model=model_name, tokenizer=tokenizer, device=-1) # -1 untuk CPU
            
            _MODELS = {"summarizer": summarizer}
            logger.info("Model NLP berhasil diinisialisasi dan siap digunakan!")
        except Exception as e:
            logger.error(f"Inisialisasi model gagal: {e}")
            raise
    return _MODELS["summarizer"]

def transform_and_load_news(**kwargs):
    """
    Fungsi utama untuk transformasi dan load.
    Kini memprioritaskan artikel dari DAG pemicu.
    """
    # Dapatkan dag_run object dari context
    dag_run = kwargs.get('dag_run')
    client = get_mongo_client()
    summarizer = load_models()

    db = client["Iqplus"]
    source_collection = db["Iqplus_News_Extract"]
    target_collection = db["Iqplus_News_Transform"]

    query = {}
    # --- Perbaikan Logika: Prioritaskan data dari trigger ---
    # Cek apakah DAG ini dijalankan dengan konfigurasi dari DAG scraper
    if dag_run and dag_run.conf and 'new_article_ids' in dag_run.conf:
        new_ids = dag_run.conf['new_article_ids']
        if not new_ids:
            logger.info("DAG dipicu tetapi tidak ada ID artikel baru yang diberikan. Tidak ada yang diproses.")
            return
        
        logger.info(f"Memproses {len(new_ids)} artikel baru dari DAG pemicu.")
        # Buat query untuk mengambil hanya dokumen dengan ID yang diberikan
        query = {"_id": {"$in": [ObjectId(id_str) for id_str in new_ids]}}
    else:
        # Fallback jika DAG dijalankan manual: proses semua yang belum selesai
        logger.warning("Menjalankan dalam mode fallback (manual trigger). Memproses semua artikel yang belum ditransformasi.")
        query = {
            "$or": [
                {"status_transformasi": "belum"},
                {"status_transformasi": {"$exists": False}}
            ]
        }

    pending_articles = list(source_collection.find(query))

    if not pending_articles:
        logger.info("Tidak ada artikel yang perlu diproses.")
        return

    processed_count = 0
    failed_count = 0
    # --- Peningkatan Efisiensi: Batch Processing ---
    update_operations = []
    batch_size = 50 # Proses 50 artikel sebelum update ke DB

    for i, article in enumerate(pending_articles):
        article_id = article["_id"]
        try:
            content = article.get("konten", "")
            if not content:
                raise ValueError("Konten artikel kosong.")

            summary_result = summarizer(
                content, max_length=150, min_length=50, truncation=True, do_sample=False
            )
            summary = summary_result[0]['summary_text']
            
            # Mendapatkan kategori berita dari dokumen sumber
            # Asumsi field ini ditambahkan di scraper
            category_match = article.get("link", "").split('/')
            category = "unknown"
            if "market_news" in category_match:
                category = "Market News"
            elif "stock_news" in category_match:
                category = "Stock News"

            transformed_doc = {
                "original_id": str(article_id),
                "title": article["judul"],
                "link": article["link"],
                "original_content": content,
                "summary": summary,
                "category": category,
                "source": "IQPlus",
                "metadata": {
                    "original_date": article.get("tanggal_artikel"),
                    "scraped_date": article.get("tanggal_scrape"),
                    "transform_date": datetime.now().isoformat(),
                    "word_count": len(content.split()),
                    "summary_length": len(summary.split()),
                }
            }
            
            # Cek duplikat di koleksi target sebelum insert
            if not target_collection.find_one({"original_id": str(article_id)}):
                target_collection.insert_one(transformed_doc)
            else:
                logger.warning(f"Artikel dengan original_id {article_id} sudah ada di koleksi target. Melewatkan insert.")

            update_operations.append(
                UpdateOne({"_id": article_id}, {"$set": {"status_transformasi": "selesai"}})
            )
            processed_count += 1
            logger.info(f"Berhasil memproses artikel: {article['judul'][:50]}...")

        except Exception as e:
            failed_count += 1
            logger.error(f"Gagal memproses artikel {article_id}: {e}", exc_info=True)
            update_operations.append(
                UpdateOne({"_id": article_id}, {"$set": {"status_transformasi": "gagal", "error_message": str(e)}})
            )
        
        # Lakukan bulk update ketika batch sudah penuh atau di iterasi terakhir
        if (i + 1) % batch_size == 0 or (i + 1) == len(pending_articles):
            if update_operations:
                logger.info(f"Menyimpan status untuk {len(update_operations)} artikel ke database...")
                source_collection.bulk_write(update_operations)
                update_operations = [] # Reset batch

    logger.info(f"Pemrosesan selesai! Sukses: {processed_count}, Gagal: {failed_count}")

# Definisi DAG
default_args = {
    "owner": "airflow",
    "retries": 1, # Kurangi retry untuk task berat seperti NLP
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2), # Sesuaikan timeout
}

with DAG(
    dag_id="transform_load_iqplus",
    description="Transform and load IQPlus news articles triggered by scraper.",
    # --- Perbaikan Utama: Hapus schedule_interval agar hanya berjalan via trigger ---
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["iqplus", "transform", "nlp", "triggered"],
) as dag:
    
    transform_task = PythonOperator(
        task_id="transform_and_load_new_articles",
        python_callable=transform_and_load_news,
    )