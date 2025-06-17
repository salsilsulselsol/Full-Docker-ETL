import os
import sys
import logging
import time
import xml.etree.ElementTree as ET
import locale

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, coalesce, udf
from pyspark.sql.types import (
    StringType, DoubleType, TimestampType,
    StructType, StructField, MapType, ArrayType
)

# MongoDB imports
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# PERBAIKAN 1: Tambahkan pengecekan modul kustom 'idx' jika diperlukan
try:
    import idx
    logging.info("Modul 'idx' berhasil diimpor.")
except ImportError:
    logging.warning("Modul 'idx' tidak ditemukan. Jika ini adalah modul kustom, pastikan sudah terinstal atau tersedia di PYTHONPATH.")
    idx = None  # Set ke None untuk penanganan kondisional jika diperlukan


# Konfigurasi logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# PERBAIKAN 1: Konfigurasi MongoDB dengan fallback yang lebih baik
# Coba beberapa kemungkinan URI MongoDB


MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://mongodb-external:27017/")

SOURCE_DATABASE_NAME = "idx_financial_data_staging"
OUTPUT_DATABASE_NAME = "idx_financial_data_production"
SOURCE_COLLECTION_PREFIX = "reports_"
OUTPUT_COLLECTION_PREFIX = "processed_reports_"

target_contexts = ["CurrentYearDuration", "CurrentYearInstant", "Year", "YTD"]

# --- UTILITY FUNCTIONS ---
def create_spark_session(app_name="IDXFinancialDataTransformation", max_retries=3, retry_delay=10):
    """Membuat dan mengembalikan Spark Session dengan retry."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Mencoba membuat Spark session (percobaan {attempt+1}/{max_retries})...")
            logger.info(f"Spark akan menggunakan MongoDB URI: {MONGODB_URI}")

            spark_builder = (SparkSession.builder
                             .appName(app_name)
                             .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
                             .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))  # Kurangi memory
                             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "1g"))      # Kurangi memory
                             .config("spark.mongodb.input.uri", MONGODB_URI)
                             .config("spark.mongodb.output.uri", MONGODB_URI)
                             
                             .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
                             .config("spark.sql.adaptive.enabled", "true")
                             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                             .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
                             .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "2")
                             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                             .config("spark.sql.adaptive.skewJoin.enabled", "true")
                             .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                             .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
                             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                             .config("spark.sql.execution.arrow.maxRecordsPerBatch", "500")
                             .config("spark.sql.shuffle.partitions", "4") 
                             .config("spark.default.parallelism", "4")
                             .config("spark.network.timeout", "600s")  # Tingkatkan timeout
                             .config("spark.executor.heartbeatInterval", "60s")
                             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                             # PERBAIKAN: Tingkatkan timeout MongoDB
                             .config("spark.mongodb.input.connection.timeoutMS", "300000")  # 2 menit
                             .config("spark.mongodb.output.connection.timeoutMS", "300000") # 2 menit
                             .config("spark.mongodb.input.readTimeout", "300000")          # 2 menit
                             .config("spark.mongodb.output.readTimeout", "300000")         # 2 menit
                             .config("spark.mongodb.driver.logging.level", "INFO")
                             # PERBAIKAN: Konfigurasi tambahan untuk koneksi MongoDB
                             .config("spark.mongodb.input.maxConnectionIdleTime", "300000")
                             .config("spark.mongodb.input.maxConnectionLifeTime", "600000")
                             .config("spark.mongodb.input.minConnectionPoolSize", "1")
                             .config("spark.mongodb.input.maxConnectionPoolSize", "5")
                             )
            spark = spark_builder.getOrCreate()
            logger.info(f"✅ Spark session berhasil dibuat. Versi: {spark.version}")
            spark.sparkContext.setLogLevel("WARN")
            return spark
        except Exception as e:
            logger.error(f"❌ Gagal membuat Spark session (percobaan {attempt+1}): {str(e)}", exc_info=True)
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                try: 
                    if 'spark' in locals() and spark is not None:
                        spark.stop()
                except:
                    pass
            else:
                logger.error(f"❌ Gagal membuat Spark session setelah {max_retries} percobaan.")
                raise

def parse_xbrl_xml_to_map(xml_string: str) -> dict:
    if not xml_string:
        return {}
    
    data_map = {}
    try:
        root = ET.fromstring(xml_string)
        ns = {'xbrli': 'http://www.xbrl.org/2003/instance',
              'idx-cor': 'http://www.idx.co.id/xbrl/taxonomy/2014-04-30/cor',
              'idx-dei': 'http://www.idx.co.id/xbrl/taxonomy/2014-04-30/dei'}

        for elem in root.findall(".//*", namespaces=ns):
            tag_name = elem.tag.split('}')[-1]
            context_ref = elem.get('contextRef')

            if context_ref and any(ctx in context_ref for ctx in target_contexts):
                if tag_name not in data_map:
                    data_map[tag_name] = elem.text

        for elem in root.findall(".//*", namespaces=ns):
            tag_name = elem.tag.split('}')[-1]
            context_ref = elem.get('contextRef')
            if not context_ref and tag_name not in data_map:
                data_map[tag_name] = elem.text
                                
        return data_map
    except ET.ParseError as pe:
        logger.warning(f"Gagal mengurai XML: {pe}. XML string: {xml_string[:500]}...")
        return {}
    except Exception as e:
        logger.error(f"Error tak terduga saat mengurai XML: {e}", exc_info=True)
        return {}

parse_xbrl_udf = udf(parse_xbrl_xml_to_map, MapType(StringType(), StringType()))

def safe_get_key_case_insensitive(data_map, key_name_target):
    if data_map is None or key_name_target is None:
        return None
    key_name_target_lower = key_name_target.lower()
    for key_map, value_map in data_map.items():
        if key_map and key_map.lower() == key_name_target_lower:
            return str(value_map) if value_map is not None else None
    return None

safe_get_key_udf = udf(safe_get_key_case_insensitive, StringType())

try:
    locale.setlocale(locale.LC_ALL, 'id_ID.UTF-8')
    logger.info("Locale set to id_ID.UTF-8 for Rupiah formatting.")
except locale.Error as e:
    logger.warning(f"Could not set locale for Rupiah formatting: {e}. Formatting might default to system locale or fail.")

def format_rupiah_spark_udf(amount):
    if amount is None:
        return None
    try:
        float_amount = float(amount)
        formatted_str = f"{float_amount:,.2f}"
        formatted_str = formatted_str.replace('.', '#').replace(',', '.').replace('#', ',')
        return f"Rp {formatted_str}"
    except (ValueError, TypeError):
        return None

format_rupiah_udf = udf(format_rupiah_spark_udf, StringType())


# --- PROCESS FINANCIAL DATA ---
def process_financial_data(df_raw, source_collection_name_str):
    try:
        logger.info(f"[DEBUG] Kolom tersedia di df_raw: {df_raw.columns}")
        logger.info(f"[DEBUG] Contoh baris pertama:")
        df_raw.limit(1).show(truncate=False)

        logger.info(f"Memulai proses data untuk koleksi: {source_collection_name_str}.")

        # DEBUG: tes parsing satu baris untuk validasi manual (TIDAK MENGGANGGU df_parsed)
        try:
            test_data_row = df_raw.select("data").limit(1).collect()[0]["data"]
            parsed_result = parse_xbrl_udf.func(test_data_row)
            logger.info(f"[DEBUG] Hasil parse_xbrl contoh baris: {parsed_result}")
        except Exception as e:
            logger.error(f"[DEBUG] Error saat parsing data langsung: {str(e)}", exc_info=True)

        # APPLY parsing untuk seluruh DataFrame
        df_parsed = df_raw.withColumn("parsed_data", parse_xbrl_udf(col("data")))

        logger.info(f"[DEBUG] Menampilkan parsed_data hasil UDF:")
        df_parsed.select("parsed_data").show(5, truncate=False)

        # Mapping tag ke field
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

        # Extract dan transform field
        df_processed = df_parsed.select(
            col("company_code"), col("year"), col("period"),
            safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["company_name"])).alias("company_name"),
            safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["sector"])).alias("sector"),
            safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["subsector"])).alias("subsector"),
            
            (coalesce(safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["revenue"])).cast(DoubleType()), lit(0.0))).alias("revenue"),
            (coalesce(safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["cost_of_revenue"])).cast(DoubleType()), lit(0.0))).alias("cost_of_revenue"),
            (coalesce(safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["gross_profit"])).cast(DoubleType()), lit(0.0))).alias("gross_profit"),
            (coalesce(safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["profit_loss"])).cast(DoubleType()), lit(0.0))).alias("net_profit_loss"),
            (coalesce(safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["total_assets"])).cast(DoubleType()), lit(0.0))).alias("total_assets"),
            (coalesce(safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["total_liabilities"])).cast(DoubleType()), lit(0.0))).alias("total_liabilities"),
            (coalesce(safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["total_equity"])).cast(DoubleType()), lit(0.0))).alias("total_equity"),
            
            col("retrieved_at").cast(TimestampType())
        )

        logger.info("[DEBUG] Menampilkan hasil df_processed (sebelum margin/ratio):")
        df_processed.show(5, truncate=False)

        # Hitung metrik tambahan
        df_processed = df_processed.withColumn(
            "net_profit_margin_pct",
            when(col("revenue") != 0, (col("net_profit_loss") / col("revenue")) * 100).otherwise(None)
        ).withColumn(
            "debt_to_equity_ratio",
            when(col("total_equity") != 0, col("total_liabilities") / col("total_equity")).otherwise(None)
        )

        # Format nilai rupiah
        currency_columns = [
            "revenue", "cost_of_revenue", "gross_profit", "net_profit_loss",
            "total_assets", "total_liabilities", "total_equity"
        ]

        for col_name in currency_columns:
            df_processed = df_processed.withColumn(
                f"{col_name}_rupiah",
                format_rupiah_udf(col(col_name))
            )

        logger.info("[DEBUG] Menampilkan hasil akhir df_processed (dengan rupiah):")
        df_processed.show(5, truncate=False)

        logger.info(f"✅ Data berhasil diproses untuk {source_collection_name_str}.")
        return df_processed

    except Exception as e:
        logger.error(f"❌ Error pada proses data untuk {source_collection_name_str}: {str(e)}", exc_info=True)
        raise




# --- GET SOURCE COLLECTIONS ---
def get_source_collections(**context):
    client = None
    try:
        logger.info(f"Mencoba mendapatkan daftar koleksi dari DB '{SOURCE_DATABASE_NAME}' di '{MONGODB_URI}'...")
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=30000)  # Tingkatkan timeout
        db = client[SOURCE_DATABASE_NAME]
        
        client.admin.command('ping') 
        logger.info("✅ Koneksi MongoDB berhasil.")

        collection_names_only = [name for name in db.list_collection_names() if name.startswith(SOURCE_COLLECTION_PREFIX)]
        
        if not collection_names_only:
            logger.warning(f"Tidak ada koleksi ditemukan di database '{SOURCE_DATABASE_NAME}' dengan prefix '{SOURCE_COLLECTION_PREFIX}'.")
        else:
            logger.info(f"Ditemukan {len(collection_names_only)} koleksi sumber data: {collection_names_only}")
        
        ti = context['ti']
        ti.xcom_push(key='source_collections', value=collection_names_only)
        
        return collection_names_only
    except ConnectionFailure as ce:
        logger.error(f"❌ Gagal terhubung ke MongoDB di '{MONGODB_URI}': {ce}", exc_info=True)
        raise
    except OperationFailure as of:
        logger.error(f"❌ Operasi MongoDB gagal di DB '{SOURCE_DATABASE_NAME}': {of}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"❌ Error umum saat mengambil daftar koleksi dari DB '{SOURCE_DATABASE_NAME}': {e}", exc_info=True)
        raise
    finally:
        if client:
            client.close()
            logger.info("🚪 Koneksi MongoDB ditutup.")


# PERBAIKAN 2: Tambahkan parameter **context untuk mengatasi error parameter
def process_and_load_single_collection(source_collection_name: str, **context):
    """
    PERBAIKAN: Menambahkan **context untuk mengatasi error parameter dari Airflow
    """
    spark_session = None
    try:
        logger.info(f"🔥 Memulai proses untuk koleksi: '{SOURCE_DATABASE_NAME}.{source_collection_name}'")
        logger.info(f"MONGODB_URI di process_and_load_single_collection: {MONGODB_URI}")

        output_collection_name = OUTPUT_COLLECTION_PREFIX + source_collection_name[len(SOURCE_COLLECTION_PREFIX):]

        # Langkah 1: Buat Spark Session
        try:
            spark_session = create_spark_session(app_name=f"SparkTransformLoad_{source_collection_name}")
            logger.info("Spark session berhasil dibuat di process_and_load_single_collection.")
        except Exception as e:
            logger.error(f"❌ Tahap 1 GAGAL: Tidak dapat membuat Spark session untuk koleksi '{source_collection_name}': {e}", exc_info=True)
            raise

        # Langkah 2: Membaca data mentah dari MongoDB staging
        df_raw_data = None
        try:
            logger.info(f"Mencoba membaca data dari {SOURCE_DATABASE_NAME}.{source_collection_name} menggunakan URI: {MONGODB_URI}")
            
            # PERBAIKAN: Tambahkan opsi koneksi yang lebih lengkap
            df_raw_data = spark_session.read.format("mongodb") \
                .option("spark.mongodb.read.connection.uri", MONGODB_URI) \
                .option("database", SOURCE_DATABASE_NAME) \
                .option("collection", source_collection_name) \
                .option("spark.mongodb.read.readTimeout", "120000") \
                .option("spark.mongodb.read.connectionTimeout", "60000") \
                .option("spark.mongodb.read.maxConnectionIdleTime", "6o0000") \
                .option("spark.mongodb.read.maxConnectionLifeTime", "600000") \
                .load()
            
            # Cek apakah ada data
            count = df_raw_data.count()
            if count == 0:
                logger.info(f"Tahap 2 SUKSES: Koleksi '{source_collection_name}' kosong. Dilewati.")
                return

            logger.info(f"Tahap 2 SUKSES: Jumlah baris yang dibaca dari '{source_collection_name}': {count}")

        except Exception as e:
            logger.error(f"❌ Tahap 2 GAGAL: Error saat membaca data dari MongoDB untuk koleksi '{source_collection_name}': {e}", exc_info=True)
            # PERBAIKAN: Coba koneksi alternatif jika gagal
            logger.info("Mencoba koneksi alternatif...")
            alternative_uri = os.environ.get("MONGODB_URI", "mongodb://mongodb-external-alt:27017/")
            if alternative_uri != MONGODB_URI:
                try:
                    logger.info(f"Mencoba URI alternatif: {alternative_uri}")
                    df_raw_data = spark_session.read.format("mongodb") \
                        .option("spark.mongodb.read.connection.uri", alternative_uri) \
                        .option("database", SOURCE_DATABASE_NAME) \
                        .option("collection", source_collection_name) \
                        .load()
                    
                    count = df_raw_data.count()
                    logger.info(f"✅ Berhasil dengan URI alternatif! Jumlah baris: {count}")
                    if count == 0:
                        return
                        
                except Exception as e2:
                    logger.error(f"❌ URI alternatif juga gagal: {e2}")
                    raise e  # Raise error asli
            else:
                raise

        # Langkah 3: Memproses data
        df_processed_data = None
        try:
            logger.info(f"Tahap 3: Memulai pemrosesan data untuk koleksi '{source_collection_name}'.")
            df_processed_data = process_financial_data(df_raw_data, source_collection_name)
            
            if df_processed_data.rdd.isEmpty():
                logger.warning(f"Tahap 3 SUKSES: Tidak ada data setelah diproses untuk '{source_collection_name}'. Penulisan dilewati.")
                return

            logger.info(f"Tahap 3 SUKSES: Data berhasil diproses untuk koleksi '{source_collection_name}'.")

        except Exception as e:
            logger.error(f"❌ Tahap 3 GAGAL: Error saat memproses data untuk koleksi '{source_collection_name}': {e}", exc_info=True)
            raise

        # Langkah 4: Menulis data yang telah ditransformasi ke MongoDB produksi
        try:
            logger.info(f"Tahap 4: Mencoba menulis data yang diproses ke: '{OUTPUT_DATABASE_NAME}.{output_collection_name}' menggunakan URI: {MONGODB_URI}")
            (df_processed_data.write.format("mongodb")
                .option("spark.mongodb.write.connection.uri", MONGODB_URI) \
                .option("database", OUTPUT_DATABASE_NAME) \
                .option("collection", output_collection_name) \
                .option("spark.mongodb.write.writeTimeout", "120000") \
                .option("spark.mongodb.write.connectionTimeout", "60000") \
                .mode("overwrite")
                .save())
            
            logger.info(f"✅ Tahap 4 SUKSES: Berhasil memproses '{source_collection_name}' dan menyimpan ke '{output_collection_name}'")

        except Exception as e:
            logger.error(f"❌ Tahap 4 GAGAL: Error saat menulis data ke MongoDB untuk koleksi '{source_collection_name}': {e}", exc_info=True)
            raise

    except Exception as e:
        logger.critical(f"❌ Error kritis tak terduga dalam proses koleksi '{source_collection_name}': {str(e)}", exc_info=True)
        raise
    finally:
        if spark_session:
            spark_session.stop()
            logger.info(f"🚪 Spark session untuk '{source_collection_name}' ditutup.")


# PERBAIKAN 3: Fungsi untuk testing koneksi MongoDB
def test_mongodb_connection():
    """Fungsi untuk test koneksi MongoDB sebelum memulai proses"""
    try:
        logger.info("🔍 Testing koneksi MongoDB...")
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10000)
        
        # Test ping
        client.admin.command('ping')
        
        # Test akses database
        db = client[SOURCE_DATABASE_NAME]
        collections = db.list_collection_names()
        
        logger.info(f"✅ Koneksi MongoDB berhasil! Ditemukan {len(collections)} koleksi")
        client.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Test koneksi MongoDB gagal: {e}")
        return False