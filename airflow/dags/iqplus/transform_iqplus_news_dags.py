from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_mongo_client():
    """Get MongoDB client with error handling"""
    try:
        logger.info("Connecting to MongoDB...")
        client = MongoClient("mongodb://mongodb-external:27017/")
        client.admin.command('ping')
        logger.info("MongoDB connection successful!")
        return client
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

def load_models():
    """Initialize transformer models for CPU-only usage"""
    try:
        from transformers import AutoTokenizer, pipeline
        
        logger.info("Initializing NLP models (CPU-only mode)...")
        
        # Initialize tokenizer
        tokenizer = AutoTokenizer.from_pretrained("facebook/bart-large-cnn")
        
        # Initialize summarization pipeline (newer versions handle CPU automatically)
        summarizer = pipeline(
            "summarization",
            model="facebook/bart-large-cnn",
            tokenizer=tokenizer
        )
        
        logger.info("Models initialized successfully!")
        return tokenizer, summarizer
        
    except Exception as e:
        logger.error(f"Model initialization failed: {e}")
        raise
    
def transform_and_load_news(**kwargs):
    """Main transformation and loading function"""
    try:
        # Initialize components
        tokenizer, summarizer = load_models()
        client = get_mongo_client()
        
        # MongoDB collections
        db = client["Iqplus"]
        source_collection = db["Iqplus_News_Extract"]
        target_collection = db["Iqplus_News_Transform"]
        
        # Find articles needing processing
        pending_articles = list(source_collection.find({
            "$or": [
                {"status_transformasi": "belum"},
                {"status_transformasi": {"$exists": False}}
            ]
        }))
        
        if not pending_articles:
            logger.info("No articles need processing")
            return

        processed_count = 0
        failed_count = 0
        
        for article in pending_articles:
            try:
                article_id = article["_id"]
                content = article["konten"]
                
                # Generate summary (with error handling)
                summary_result = summarizer(
                    content,
                    max_length=150,
                    min_length=50,
                    truncation=True,
                    do_sample=False
                )
                summary = summary_result[0]['summary_text']
                
                # Create transformed document
                transformed_doc = {
                    "original_id": str(article_id),
                    "title": article["judul"],
                    "original_content": content,
                    "summary": summary,
                    "category": article.get("news_category", "unknown"),
                    "source": "IQPlus",
                    "metadata": {
                        "original_date": article.get("tanggal_artikel"),
                        "transform_date": datetime.now().isoformat(),
                        "word_count": len(content.split()),
                        "summary_length": len(summary.split()),
                        "device_used": "CPU"
                    }
                }
                
                # Save to target collection
                target_collection.insert_one(transformed_doc)
                
                # Update status in source collection
                source_collection.update_one(
                    {"_id": article_id},
                    {"$set": {"status_transformasi": "selesai"}}
                )
                
                processed_count += 1
                logger.info(f"Processed article: {article['judul'][:50]}...")
                
            except Exception as e:
                failed_count += 1
                source_collection.update_one(
                    {"_id": article_id},
                    {"$set": {
                        "status_transformasi": "gagal",
                        "error_message": str(e)
                    }}
                )
                logger.error(f"Failed to process article {article_id}: {e}")
                continue
                
        logger.info(f"Processing complete! Success: {processed_count}, Failed: {failed_count}")
        
    except Exception as e:
        logger.error(f"Fatal error in transform_and_load_news: {e}")
        raise

# DAG definition
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=540),
}

with DAG(
    dag_id="transform_load_iqplus",
    description="Transform and load IQPlus news articles",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["iqplus", "transform", "nlp"],
) as dag:
    
    transform_task = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load_news,
    )