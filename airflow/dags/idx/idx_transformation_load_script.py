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

# PERBAIKAN 1: Pengecekan modul kustom 'idx' yang lebih robust
try:
    import idx
    logging.info("Modul 'idx' berhasil diimpor.")
except ImportError:
    logging.warning("Modul 'idx' tidak ditemukan. Jika ini adalah modul kustom, pastikan sudah terinstal atau tersedia di PYTHONPATH.")
    idx = None

# Konfigurasi logging yang lebih baik
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('spark_mongodb_debug.log', mode='a')  # Tambah file logging
    ]
)
logger = logging.getLogger(__name__)

# PERBAIKAN 2: Konfigurasi MongoDB dengan fallback dan validasi
def get_mongodb_uri():
    """Mendapatkan MongoDB URI dengan validasi dan fallback"""
    primary_uri = os.environ.get("MONGODB_URI", "mongodb://mongodb-external:27017/")
    fallback_uris = [
        "mongodb://localhost:27017/",
        "mongodb://mongodb:27017/",
        "mongodb://127.0.0.1:27017/"
    ]
    
    # Test koneksi primary URI
    if test_single_mongodb_uri(primary_uri):
        return primary_uri
    
    # Coba fallback URIs
    for uri in fallback_uris:
        if test_single_mongodb_uri(uri):
            logger.info(f"Menggunakan fallback URI: {uri}")
            return uri
    
    logger.warning("Tidak ada URI MongoDB yang valid ditemukan, menggunakan default")
    return primary_uri

def test_single_mongodb_uri(uri, timeout=5000):
    """Test koneksi ke single MongoDB URI"""
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout)
        client.admin.command('ping')
        client.close()
        return True
    except Exception:
        return False

MONGODB_URI = get_mongodb_uri()
logger.info(f"MongoDB URI yang digunakan: {MONGODB_URI}")

SOURCE_DATABASE_NAME = "idx_financial_data_staging"
OUTPUT_DATABASE_NAME = "idx_financial_data_production"
SOURCE_COLLECTION_PREFIX = "reports_"
OUTPUT_COLLECTION_PREFIX = "processed_reports_"

target_contexts = ["CurrentYearDuration", "CurrentYearInstant", "Year", "YTD"]

# --- UTILITY FUNCTIONS ---
def create_spark_session(app_name="IDXFinancialDataTransformation", max_retries=3, retry_delay=10):
    """Membuat dan mengembalikan Spark Session dengan retry dan konfigurasi optimal."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Mencoba membuat Spark session (percobaan {attempt+1}/{max_retries})...")
            
            spark_builder = (SparkSession.builder
                             .appName(app_name)
                             .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
                             
                             # PERBAIKAN 3: Memory management yang lebih baik
                             .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
                             .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
                             .config("spark.driver.maxResultSize", "1g")
                             .config("spark.executor.memoryFraction", "0.8")
                             .config("spark.executor.memoryStorageFraction", "0.3")
                             
                             # MongoDB configuration
                             .config("spark.mongodb.input.uri", MONGODB_URI)
                             .config("spark.mongodb.output.uri", MONGODB_URI)
                             
                             # Spark master configuration
                             .master(os.environ.get("SPARK_MASTER_URL", "local[2]"))  # Kurangi parallelism
                             
                             # PERBAIKAN 4: Adaptive query execution yang lebih konservatif
                             .config("spark.sql.adaptive.enabled", "true")
                             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                             .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
                             .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "2")
                             .config("spark.sql.shuffle.partitions", "2")  # Kurangi partitions
                             .config("spark.default.parallelism", "2")
                             
                             # Serialization
                             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                             .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Disable Arrow untuk stability
                             
                             # PERBAIKAN 5: Network dan timeout configuration yang lebih robust
                             .config("spark.network.timeout", "800s")
                             .config("spark.executor.heartbeatInterval", "60s")
                             .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                             
                             # PERBAIKAN 6: MongoDB connection pool yang optimal
                             .config("spark.mongodb.input.connection.timeoutMS", "300000")
                             .config("spark.mongodb.output.connection.timeoutMS", "300000")
                             .config("spark.mongodb.input.readTimeout", "300000")
                             .config("spark.mongodb.output.readTimeout", "300000")
                             .config("spark.mongodb.input.maxConnectionIdleTime", "600000")
                             .config("spark.mongodb.input.maxConnectionLifeTime", "1200000")
                             .config("spark.mongodb.input.minConnectionPoolSize", "1")
                             .config("spark.mongodb.input.maxConnectionPoolSize", "3")  # Kurangi pool size
                             
                             # Logging configuration
                             .config("spark.mongodb.driver.logging.level", "WARN")
                             )
            
            spark = spark_builder.getOrCreate()
            logger.info(f"✅ Spark session berhasil dibuat. Versi: {spark.version}")
            spark.sparkContext.setLogLevel("WARN")
            
           # PERBAIKAN UTAMA: Distribute script Python ke semua worker nodes
            current_script_path = os.path.abspath(__file__)
            if os.path.exists(current_script_path):
                logger.info(f"Mendistribusikan script {current_script_path} ke worker nodes...")
                spark.sparkContext.addPyFile(current_script_path)

                # Jika ingin mendistribusikan banyak file, lebih baik zip foldernya
                parent_dir = os.path.dirname(current_script_path)
                zip_path = os.path.join(parent_dir, "scripts.zip")

                if os.path.exists(parent_dir):
                    import zipfile

                    # Membuat zip dari direktori jika belum ada
                    if not os.path.exists(zip_path):
                        logger.info(f"Membuat arsip ZIP dari {parent_dir} ke {zip_path}...")
                        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            for root, dirs, files in os.walk(parent_dir):
                                for file in files:
                                    if file.endswith(".py"):
                                        abs_file = os.path.join(root, file)
                                        rel_file = os.path.relpath(abs_file, parent_dir)
                                        zipf.write(abs_file, arcname=rel_file)

                    # Distribute ZIP ke Spark workers
                    logger.info(f"Mendistribusikan ZIP {zip_path} ke worker nodes...")
                    spark.sparkContext.addPyFile(zip_path)

            
            # PERBAIKAN 7: Test koneksi Spark ke MongoDB
            test_spark_mongodb_connection(spark)
            
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

def test_spark_mongodb_connection(spark):
    """Test koneksi Spark ke MongoDB"""
    try:
        logger.info("Testing koneksi Spark ke MongoDB...")
        # Coba baca dari collection yang mungkin ada atau buat test
        test_df = spark.read.format("mongodb") \
            .option("spark.mongodb.read.connection.uri", MONGODB_URI) \
            .option("database", "test") \
            .option("collection", "test_connection") \
            .load()
        
        # Tidak perlu ada data, cukup koneksi berhasil
        logger.info("✅ Koneksi Spark ke MongoDB berhasil")
    except Exception as e:
        logger.warning(f"⚠️ Test koneksi Spark ke MongoDB gagal: {e}")
        # Tidak throw error karena collection mungkin tidak ada

# PERBAIKAN UTAMA: Pindahkan semua UDF function ke dalam fungsi yang dapat di-serialize
def get_parse_xbrl_udf():
    """Factory function untuk UDF parsing XBRL - menghindari serialization error"""
    
    def parse_xbrl_xml_to_map_internal(xml_string):
        """Internal function untuk parsing XBRL XML"""
        import xml.etree.ElementTree as ET
        
        if not xml_string or not isinstance(xml_string, str):
            return {}
        
        # Trim whitespace
        xml_string = xml_string.strip()
        if not xml_string:
            return {}
        
        data_map = {}
        try:
            # Validasi XML
            if not xml_string.startswith('<'):
                return {}
                
            root = ET.fromstring(xml_string)
            ns = {
                'xbrli': 'http://www.xbrl.org/2003/instance',
                'idx-cor': 'http://www.idx.co.id/xbrl/taxonomy/2014-04-30/cor',
                'idx-dei': 'http://www.idx.co.id/xbrl/taxonomy/2014-04-30/dei'
            }

            target_contexts = ["CurrentYearDuration", "CurrentYearInstant", "Year", "YTD"]

            # Prioritas 1: Element dengan context yang diinginkan
            for elem in root.findall(".//*", namespaces=ns):
                if elem.tag and elem.text:
                    tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    context_ref = elem.get('contextRef')

                    if context_ref and any(ctx in context_ref for ctx in target_contexts):
                        if tag_name not in data_map:
                            data_map[tag_name] = elem.text.strip()

            # Prioritas 2: Element tanpa context (fallback)
            for elem in root.findall(".//*", namespaces=ns):
                if elem.tag and elem.text:
                    tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    context_ref = elem.get('contextRef')
                    if not context_ref and tag_name not in data_map:
                        data_map[tag_name] = elem.text.strip()
                                    
            return data_map
            
        except ET.ParseError as pe:
            return {}
        except Exception as e:
            return {}
    
    return udf(parse_xbrl_xml_to_map_internal, MapType(StringType(), StringType()))

def get_safe_get_key_udf():
    """Factory function untuk UDF safe key extraction"""
    
    def safe_get_key_case_insensitive_internal(data_map, key_name_target):
        """Internal function untuk safely extract key from map"""
        if data_map is None or key_name_target is None:
            return None
            
        if not isinstance(data_map, dict):
            return None
            
        key_name_target_lower = str(key_name_target).lower()
        
        for key_map, value_map in data_map.items():
            if key_map and str(key_map).lower() == key_name_target_lower:
                return str(value_map).strip() if value_map is not None else None
        return None
    
    return udf(safe_get_key_case_insensitive_internal, StringType())

def get_format_rupiah_udf():
    """Factory function untuk UDF format rupiah"""
    
    def format_rupiah_spark_udf_internal(amount):
        """Internal function untuk format number ke format Rupiah"""
        if amount is None:
            return None
        try:
            float_amount = float(amount)
            if float_amount == 0:
                return "Rp 0,00"
            
            # Format dengan 2 decimal places
            formatted_str = f"{abs(float_amount):,.2f}"
            # Ganti separator (US -> ID format)
            formatted_str = formatted_str.replace('.', '#').replace(',', '.').replace('#', ',')
            
            # Tambah tanda minus jika negatif
            prefix = "Rp " if float_amount >= 0 else "Rp -"
            return f"{prefix}{formatted_str}"
            
        except (ValueError, TypeError) as e:
            return None
    
    return udf(format_rupiah_spark_udf_internal, StringType())

# PERBAIKAN 10: Locale handling yang lebih robust
def setup_locale():
    """Setup locale untuk formatting Rupiah"""
    locales_to_try = ['id_ID.UTF-8', 'id_ID', 'Indonesian_Indonesia', 'C']
    
    for loc in locales_to_try:
        try:
            locale.setlocale(locale.LC_ALL, loc)
            logger.info(f"Locale berhasil diset ke: {loc}")
            return True
        except locale.Error:
            continue
    
    logger.warning("Tidak dapat set locale Indonesia, menggunakan default")
    return False

setup_locale()

# --- PROCESS FINANCIAL DATA ---
def process_financial_data(df_raw, source_collection_name_str):
    """Process financial data dengan error handling yang comprehensive"""
    try:
        logger.info(f"Memulai proses data untuk koleksi: {source_collection_name_str}")
        
        # PERBAIKAN 11: Validasi input DataFrame
        if df_raw is None:
            raise ValueError("DataFrame input adalah None")
            
        if df_raw.rdd.isEmpty():
            logger.warning(f"DataFrame kosong untuk koleksi: {source_collection_name_str}")
            return df_raw  # Return empty DataFrame
        
        logger.info(f"Jumlah baris input: {df_raw.count()}")
        logger.info(f"Kolom tersedia: {df_raw.columns}")
        
        # Validasi kolom yang diperlukan
        required_columns = ['data', 'company_code', 'year', 'period', 'retrieved_at']
        missing_columns = [col for col in required_columns if col not in df_raw.columns]
        if missing_columns:
            logger.error(f"Kolom yang hilang: {missing_columns}")
            raise ValueError(f"Kolom yang diperlukan tidak ditemukan: {missing_columns}")

        # Debug: Cek sample data
        sample_data = df_raw.select("data").limit(1).collect()
        if sample_data:
            logger.info(f"Sample data preview: {str(sample_data[0]['data'])[:200]}...")
        
        # PERBAIKAN UTAMA: Gunakan factory functions untuk UDF
        parse_xbrl_udf = get_parse_xbrl_udf()
        safe_get_key_udf = get_safe_get_key_udf()
        format_rupiah_udf = get_format_rupiah_udf()
        
        # APPLY parsing untuk seluruh DataFrame
        logger.info("Melakukan parsing XML...")
        df_parsed = df_raw.withColumn("parsed_data", parse_xbrl_udf(col("data")))

        # Debug: Cek hasil parsing
        parsed_sample = df_parsed.select("parsed_data").limit(1).collect()
        if parsed_sample:
            parsed_result = parsed_sample[0]['parsed_data']
            logger.info(f"Sample parsed data: {parsed_result}")
            if parsed_result:
                logger.info(f"Jumlah fields yang diparsing: {len(parsed_result)}")

        # PERBAIKAN 12: Mapping tag yang lebih comprehensive
        financial_tags_map = {
            "company_name": "EntityRegistrantName", 
            "sector": "IndustrySector", 
            "subsector": "Subsector",
            "revenue": "RevenueFromContractsWithCustomers", 
            "cost_of_revenue": "CostOfGoodsSoldAndServices",
            "gross_profit": "GrossProfit", 
            "operating_expenses": "OperatingExpenses",
            "profit_loss_from_operating_activities": "ProfitLossFromOperatingActivities",
            "finance_income": "FinanceIncome", 
            "finance_costs": "FinanceCosts",
            "profit_loss_before_tax": "ProfitLossBeforeTax", 
            "income_tax_expense_benefit": "IncomeTaxExpenseBenefit",
            "profit_loss": "ProfitLoss", 
            "total_assets": "Assets", 
            "current_assets": "CurrentAssets",
            "non_current_assets": "NoncurrentAssets", 
            "total_liabilities": "Liabilities",
            "current_liabilities": "CurrentLiabilities", 
            "non_current_liabilities": "NoncurrentLiabilities",
            "total_equity": "Equity",
        }

        # PERBAIKAN 13: Extract dan transform field dengan null handling
        logger.info("Melakukan ekstraksi dan transformasi field...")
        df_processed = df_parsed.select(
            col("company_code"), 
            col("year"), 
            col("period"),
            
            # String fields
            safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["company_name"])).alias("company_name"),
            safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["sector"])).alias("sector"),
            safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["subsector"])).alias("subsector"),
            
            # Numeric fields dengan proper casting dan null handling
            coalesce(
                safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["revenue"])).cast(DoubleType()), 
                lit(0.0)
            ).alias("revenue"),
            
            coalesce(
                safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["cost_of_revenue"])).cast(DoubleType()), 
                lit(0.0)
            ).alias("cost_of_revenue"),
            
            coalesce(
                safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["gross_profit"])).cast(DoubleType()), 
                lit(0.0)
            ).alias("gross_profit"),
            
            coalesce(
                safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["profit_loss"])).cast(DoubleType()), 
                lit(0.0)
            ).alias("net_profit_loss"),
            
            coalesce(
                safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["total_assets"])).cast(DoubleType()), 
                lit(0.0)
            ).alias("total_assets"),
            
            coalesce(
                safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["total_liabilities"])).cast(DoubleType()), 
                lit(0.0)
            ).alias("total_liabilities"),
            
            coalesce(
                safe_get_key_udf(col("parsed_data"), lit(financial_tags_map["total_equity"])).cast(DoubleType()), 
                lit(0.0)
            ).alias("total_equity"),
            
            # Timestamp field
            col("retrieved_at").cast(TimestampType())
        )

        # PERBAIKAN 14: Hitung metrik tambahan dengan safe division
        logger.info("Menghitung metrik finansial...")
        df_processed = df_processed.withColumn(
            "net_profit_margin_pct",
            when(
                (col("revenue").isNotNull()) & (col("revenue") != 0), 
                (col("net_profit_loss") / col("revenue")) * 100
            ).otherwise(None)
        ).withColumn(
            "debt_to_equity_ratio",
            when(
                (col("total_equity").isNotNull()) & (col("total_equity") != 0), 
                col("total_liabilities") / col("total_equity")
            ).otherwise(None)
        )

        # PERBAIKAN 15: Format currency dengan error handling
        logger.info("Memformat nilai currency...")
        currency_columns = [
            "revenue", "cost_of_revenue", "gross_profit", "net_profit_loss",
            "total_assets", "total_liabilities", "total_equity"
        ]

        for col_name in currency_columns:
            df_processed = df_processed.withColumn(
                f"{col_name}_rupiah",
                format_rupiah_udf(col(col_name))
            )

        # Validasi hasil akhir
        final_count = df_processed.count()
        logger.info(f"Jumlah baris setelah processing: {final_count}")
        
        if final_count > 0:
            logger.info("Sample hasil processing:")
            df_processed.select("company_name", "revenue", "net_profit_loss").show(5, truncate=False)

        logger.info(f"✅ Data berhasil diproses untuk {source_collection_name_str}")
        return df_processed

    except Exception as e:
        logger.error(f"❌ Error pada proses data untuk {source_collection_name_str}: {str(e)}", exc_info=True)
        raise

# --- GET SOURCE COLLECTIONS ---
def get_source_collections(**context):
    """Mendapatkan daftar koleksi sumber dengan error handling yang robust"""
    client = None
    try:
        logger.info(f"Mencoba mendapatkan daftar koleksi dari DB '{SOURCE_DATABASE_NAME}' di '{MONGODB_URI}'...")
        
        # PERBAIKAN 16: Timeout dan retry untuk MongoDB connection
        max_retries = 3
        for attempt in range(max_retries):
            try:
                client = MongoClient(
                    MONGODB_URI, 
                    serverSelectionTimeoutMS=30000,
                    connectTimeoutMS=20000,
                    socketTimeoutMS=20000,
                    maxPoolSize=10,
                    retryWrites=True
                )
                
                # Test connection
                client.admin.command('ping')
                logger.info("✅ Koneksi MongoDB berhasil.")
                break
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} gagal: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(5)

        db = client[SOURCE_DATABASE_NAME]
        
        # Get collections dengan timeout
        collection_names_only = [
            name for name in db.list_collection_names() 
            if name.startswith(SOURCE_COLLECTION_PREFIX)
        ]
        
        if not collection_names_only:
            logger.warning(f"Tidak ada koleksi ditemukan di database '{SOURCE_DATABASE_NAME}' dengan prefix '{SOURCE_COLLECTION_PREFIX}'.")
        else:
            logger.info(f"Ditemukan {len(collection_names_only)} koleksi sumber data: {collection_names_only}")
        
        # Store in XCom jika menggunakan Airflow
        if 'ti' in context:
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
            try:
                client.close()
                logger.info("🚪 Koneksi MongoDB ditutup.")
            except:
                pass

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
                .option("spark.mongodb.read.maxConnectionIdleTime", "600000") \
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