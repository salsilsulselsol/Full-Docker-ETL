import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, udf
from pyspark.sql.types import StringType, DoubleType, TimestampType
from pymongo import MongoClient

# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Konfigurasi MongoDB (sesuaikan dengan nama service di docker-compose teman Anda)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
SOURCE_DATABASE_NAME = "idx_financial_data_staging"
OUTPUT_DATABASE_NAME = "idx_financial_data_production"
SOURCE_COLLECTION_PREFIX = "reports_"
OUTPUT_COLLECTION_PREFIX = "processed_reports_"

def create_spark_session():
    """Membuat dan mengembalikan Spark Session."""
    try:
        spark_builder = (SparkSession.builder
                  .appName("IDXFinancialDataTransformation")
                  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
                  .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
                  .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
                  .config("spark.mongodb.input.uri", MONGO_URI)
                  .config("spark.mongodb.output.uri", MONGO_URI)
                  .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
                 )
        spark = spark_builder.getOrCreate()
        logger.info(f"Spark session berhasil dibuat. Versi: {spark.version}")
        return spark
    except Exception as e:
        logger.error(f"Gagal membuat Spark session: {str(e)}", exc_info=True)
        raise

def safe_get_key_case_insensitive(data_map, key_name_target):
    """Mencari nilai dalam map tanpa memperhatikan besar kecil huruf pada key."""
    if data_map is None or key_name_target is None:
        return None
    key_name_target_lower = key_name_target.lower()
    for key_map, value_map in data_map.items():
        if key_map.lower() == key_name_target_lower:
            return str(value_map) if value_map else None
    return None

safe_get_key_udf = udf(safe_get_key_case_insensitive, StringType())

def process_financial_data(df_raw, source_collection_name_str):
    """Memproses DataFrame mentah menjadi struktur data yang bersih."""
    try:
        logger.info(f"Memulai proses data untuk koleksi: {source_collection_name_str}")
        
        financial_tags_map = {
            "company_name": "EntityRegistrantName", "sector": "IndustrySector", "subsector": "Subsector",
            "revenue": "RevenueFromContractsWithCustomers", "cost_of_revenue": "CostOfGoodsSoldAndServices",
            "gross_profit": "GrossProfit", "operating_expenses": "OperatingExpenses",
            "profit_loss_from_operating_activities": "ProfitLossFromOperatingActivities",
            "finance_income": "FinanceIncome", "finance_costs": "FinanceCosts",
            "profit_loss_before_tax": "ProfitLossBeforeTax", "income_tax_expense_benefit": "IncomeTaxExpenseBenefit",
            "profit_loss": "ProfitLoss", "total_assets": "Assets", "current_assets": "CurrentAssets",
            "non_current_assets": "NoncurrentAssets", "total_liabilities": "Liabilities",
            "current_liabilities": "CurrentLiabilities", "non_current_liabilities": "NoncurrentLiabilities",
            "total_equity": "Equity",
        }

        df_processed = df_raw.select(
            col("company_code"), col("year"), col("period"),
            safe_get_key_udf(col("data"), lit(financial_tags_map["company_name"])).alias("company_name"),
            safe_get_key_udf(col("data"), lit(financial_tags_map["sector"])).alias("sector"),
            safe_get_key_udf(col("data"), lit(financial_tags_map["subsector"])).alias("subsector"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["revenue"])).cast(DoubleType()), lit(0.0)).alias("revenue"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["cost_of_revenue"])).cast(DoubleType()), lit(0.0)).alias("cost_of_revenue"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["gross_profit"])).cast(DoubleType()), lit(0.0)).alias("gross_profit"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["profit_loss"])).cast(DoubleType()), lit(0.0)).alias("net_profit_loss"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["total_assets"])).cast(DoubleType()), lit(0.0)).alias("total_assets"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["total_liabilities"])).cast(DoubleType()), lit(0.0)).alias("total_liabilities"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["total_equity"])).cast(DoubleType()), lit(0.0)).alias("total_equity"),
            col("retrieved_at").cast(TimestampType())
        )

        df_processed = df_processed.withColumn(
            "net_profit_margin_pct",
            when(col("revenue") != 0, (col("net_profit_loss") / col("revenue")) * 100).otherwise(None)
        ).withColumn(
            "debt_to_equity_ratio",
            when(col("total_equity") != 0, col("total_liabilities") / col("total_equity")).otherwise(None)
        )
        
        logger.info(f"Data berhasil diproses untuk {source_collection_name_str}.")
        return df_processed
    
    except Exception as e:
        logger.error(f"Error pada proses data untuk {source_collection_name_str}: {str(e)}", exc_info=True)
        raise

def list_source_collections_from_db(mongo_uri_str, db_name_str, prefix_str):
    """Mendapatkan daftar koleksi dari MongoDB berdasarkan prefix."""
    try:
        client = MongoClient(mongo_uri_str, serverSelectionTimeoutMS=10000)
        db = client[db_name_str]
        collection_names = [name for name in db.list_collection_names() if name.startswith(prefix_str)]
        client.close()
        logger.info(f"Ditemukan koleksi sumber data: {collection_names}")
        return collection_names
    except Exception as e:
        logger.error(f"Error saat mengambil daftar koleksi dari DB '{db_name_str}': {e}", exc_info=True)
        return []

def process_single_mongodb_collection(spark_session, source_collection_full_name):
    """Membaca, memproses, dan menulis data untuk satu koleksi."""
    try:
        output_collection_full_name = OUTPUT_COLLECTION_PREFIX + source_collection_full_name[len(SOURCE_COLLECTION_PREFIX):]

        logger.info(f"Memulai proses untuk: '{SOURCE_DATABASE_NAME}.{source_collection_full_name}'")
        
        df_raw_data = spark_session.read.format("mongodb") \
            .option("database", SOURCE_DATABASE_NAME) \
            .option("collection", source_collection_full_name) \
            .load()
        
        if df_raw_data.rdd.isEmpty():
            logger.info(f"Koleksi '{source_collection_full_name}' kosong. Dilewati.")
            return

        df_processed_data = process_financial_data(df_raw_data, source_collection_full_name)
        
        if df_processed_data.rdd.isEmpty():
            logger.warning(f"Tidak ada data setelah diproses untuk '{source_collection_full_name}'. Penulisan dilewati.")
            return

        logger.info(f"Menulis data yang diproses ke: '{OUTPUT_DATABASE_NAME}.{output_collection_full_name}'")
        
        (df_processed_data.write.format("mongodb")
            .option("database", OUTPUT_DATABASE_NAME)
            .option("collection", output_collection_full_name)
            .mode("overwrite")
            .save())
        
        logger.info(f"✅ Berhasil memproses '{source_collection_full_name}' dan menyimpan ke '{output_collection_full_name}'")
    
    except Exception as e:
        logger.error(f"❌ Error saat memproses koleksi '{source_collection_full_name}': {str(e)}", exc_info=True)
        raise

def main_transformation_task():
    """Fungsi utama yang akan dipanggil oleh Airflow."""
    logger.info("🚀 Memulai Skrip Transformasi dan Load Data IDX...")
    spark_session = None
    try:
        spark_session = create_spark_session()
        
        source_collections = list_source_collections_from_db(MONGO_URI, SOURCE_DATABASE_NAME, prefix_str=SOURCE_COLLECTION_PREFIX)
        
        if not source_collections:
            logger.warning(f"Tidak ada koleksi sumber data ditemukan. Proses dihentikan.")
            return

        logger.info(f"Ditemukan {len(source_collections)} koleksi untuk diproses: {source_collections}")
        
        for collection_name in source_collections:
            process_single_mongodb_collection(spark_session, collection_name)
        
        logger.info("🏁 Semua koleksi data finansial telah berhasil diproses.")
    
    except Exception as e:
        logger.error(f"🔥 Terjadi error fatal pada proses transformasi utama: {str(e)}", exc_info=True)
        raise
    
    finally:
        if spark_session:
            spark_session.stop()
            logger.info("🚪 Spark session ditutup.")