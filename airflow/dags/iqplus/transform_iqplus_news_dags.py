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

# --- Cache Model & Client ---
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
    Memuat model NLP dan tokenizer hanya sekali dan menyimpannya di cache.
    """
    global _MODELS
    if _MODELS is None:
        try:
            from transformers import pipeline, AutoTokenizer
            logger.info("Inisialisasi model NLP & tokenizer untuk pertama kali (ini mungkin lambat)...")
            
            model_name = "facebook/bart-large-cnn"
            tokenizer = AutoTokenizer.from_pretrained(model_name)
            summarizer = pipeline("summarization", model=model_name, tokenizer=tokenizer, device=-1)
            
            # --- PERUBAHAN: Simpan tokenizer dan summarizer ---
            _MODELS = {"summarizer": summarizer, "tokenizer": tokenizer}
            logger.info("Model NLP & tokenizer berhasil diinisialisasi dan siap digunakan!")
        except Exception as e:
            logger.error(f"Inisialisasi model gagal: {e}")
            raise
    # --- PERUBAHAN: Kembalikan dictionary berisi keduanya ---
    return _MODELS

def transform_and_load_news(**kwargs):
    """
    Fungsi utama untuk transformasi dan load dengan solusi pertahanan dua lapis.
    """
    dag_run = kwargs.get('dag_run')
    client = get_mongo_client()
    models = load_models()
    tokenizer = models['tokenizer']
    summarizer = models['summarizer']

    db = client["Iqplus"]
    source_collection = db["Iqplus_News_Extract"]
    target_collection = db["Iqplus_News_Transform"]

    query = {}
    if dag_run and dag_run.conf and 'new_article_ids' in dag_run.conf:
        new_ids = dag_run.conf['new_article_ids']
        if not new_ids:
            logger.info("DAG dipicu tetapi tidak ada ID artikel baru yang diberikan. Tidak ada yang diproses.")
            return
        
        logger.info(f"Memproses {len(new_ids)} artikel baru dari DAG pemicu.")
        query = {"_id": {"$in": [ObjectId(id_str) for id_str in new_ids]}}
    else:
        logger.warning("Menjalankan dalam mode fallback (manual trigger). Memproses semua artikel yang belum ditransformasi.")
        query = {
            "$or": [{"status_transformasi": "belum"}, {"status_transformasi": {"$exists": False}}]
        }

    pending_articles = list(source_collection.find(query))

    if not pending_articles:
        logger.info("Tidak ada artikel yang perlu diproses.")
        return

    processed_count = 0
    failed_count = 0
    update_operations = []
    batch_size = 50

    for i, article in enumerate(pending_articles):
        article_id = article["_id"]
        try:
            content = article.get("konten", "")
            if not content:
                raise ValueError("Konten artikel kosong.")

            # #######################################################################
            # ### SOLUSI FINAL: PERTAHANAN DUA LAPIS ###
            # #######################################################################
            
            # --- LAPIS 1: Jaring Pengaman Karakter untuk Mencegah OverflowError ---
            # Mencegah string yang luar biasa panjang masuk ke tokenizer.
            # 50,000 adalah batas yang sangat besar namun aman.
            SANITY_CHAR_LIMIT = 50000 
            if len(content) > SANITY_CHAR_LIMIT:
                logger.warning(f"Konten SANGAT PANJANG ({len(content)} chars). Dipotong ke {SANITY_CHAR_LIMIT} chars sebagai jaring pengaman.")
                content = content[:SANITY_CHAR_LIMIT]

            # --- LAPIS 2: Presisi Token untuk Mencegah IndexError ---
            # Menggunakan tokenizer untuk memotong secara presisi berdasarkan batas model.
            # Kita hardcode 1024 untuk kepastian absolut.
            token_ids = tokenizer.encode(
                content,
                truncation=True,
                max_length=1024 
            )
            safe_content = tokenizer.decode(
                token_ids,
                skip_special_tokens=True
            )
            # #######################################################################
            
            summary_result = summarizer(
                safe_content, 
                max_length=150, 
                min_length=50, 
                do_sample=False
            )
            summary = summary_result[0]['summary_text']
            
            # (Sisa kode sama persis seperti sebelumnya)
            category_match = article.get("link", "").split('/')
            category = "unknown"
            if "market_news" in category_match: category = "Market News"
            elif "stock_news" in category_match: category = "Stock News"

            transformed_doc = {
                "original_id": str(article_id), "title": article["judul"],
                "link": article["link"], "original_content": content,
                "summary": summary, "category": category, "source": "IQPlus",
                "metadata": {
                    "original_date": article.get("tanggal_artikel"),
                    "scraped_date": article.get("tanggal_scrape"),
                    "transform_date": datetime.now().isoformat(),
                    "word_count": len(safe_content.split()),
                    "summary_length": len(summary.split()),
                }
            }
            
            if not target_collection.find_one({"original_id": str(article_id)}):
                target_collection.insert_one(transformed_doc)
            else:
                logger.warning(f"Artikel dg original_id {article_id} sudah ada. Melewatkan insert.")

            update_operations.append(UpdateOne({"_id": article_id}, {"$set": {"status_transformasi": "selesai"}}))
            processed_count += 1
            logger.info(f"Berhasil memproses artikel: {article['judul'][:50]}...")

        except Exception as e:
            failed_count += 1
            logger.error(f"Gagal memproses artikel {article_id}: {type(e).__name__} - {e}")
            update_operations.append(UpdateOne({"_id": article_id}, {"$set": {"status_transformasi": "gagal", "error_message": str(e)}}))
        
        if (i + 1) % batch_size == 0 or (i + 1) == len(pending_articles):
            if update_operations:
                logger.info(f"Menyimpan status untuk {len(update_operations)} artikel ke database...")
                source_collection.bulk_write(update_operations)
                update_operations = []

    logger.info(f"Pemrosesan selesai! Sukses: {processed_count}, Gagal: {failed_count}")

# (Definisi DAG tetap sama persis seperti sebelumnya)
default_args = {
    "owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="transform_load_iqplus",
    description="Transform and load IQPlus news articles triggered by scraper.",
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