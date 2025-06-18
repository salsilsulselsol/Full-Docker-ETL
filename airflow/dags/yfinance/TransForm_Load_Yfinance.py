from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils import timezone as airflow_timezone
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import json
import logging
import time
import os
import pickle
import psutil

# Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth, date_format,
    avg, sum as spark_sum, max as spark_max, min as spark_min,
    stddev, count, lit, when, isnan, isnull,
    concat, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, DateType, TimestampType, LongType
)

# Logging settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Application constants configuration
MONGODB_URI = "mongodb://mongodb-external:27017/"
DB_EXTRACT = "Yfinance_Extract"
DB_TRANSFORM = "Yfinance_Transform"
DB_LOAD = "Yfinance_Load"
BATCH_SIZE = 1000
TEMP_DIR = "/opt/airflow/data/temp"

# Ensure temp directory exists
os.makedirs(TEMP_DIR, exist_ok=True)

# ======= UTILITY FUNCTIONS =======
def log_memory_usage(task_name):
    """Log current memory usage"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        logger.info(f"[{task_name}] Memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")
    except Exception as e:
        logger.warning(f"Failed to get memory usage for {task_name}: {e}")

def get_spark_session(app_name="YFinance_ETL", max_retries=3):
    """Create and configure Spark session with error handling"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Creating Spark session (attempt {attempt+1}/{max_retries})...")
            spark = SparkSession.builder \
                .appName(app_name) \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
                .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "2") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.sql.adaptive.skewJoin.enabled", "true") \
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
                .config("spark.mongodb.input.uri", f"{MONGODB_URI}{DB_EXTRACT}") \
                .config("spark.mongodb.output.uri", f"{MONGODB_URI}{DB_LOAD}") \
                .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .config("spark.sql.execution.arrow.maxRecordsPerBatch", "500") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.maxResultSize", "1g") \
                .config("spark.sql.shuffle.partitions", "4") \
                .config("spark.default.parallelism", "4") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
                .config("spark.network.timeout", "300s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .getOrCreate()
            spark.sparkContext.setLogLevel("WARN")

            # Test Spark session
            test_df = spark.range(1, 5)
            test_count = test_df.count()
            logger.info(f"✅ Spark session created successfully (test count: {test_count})")
            return spark
        except Exception as e:
            logger.error(f"❌ Failed to create Spark session (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(10)
                # Try to stop any existing session
                try:
                    if 'spark' in locals():
                        spark.stop()
                except:
                    pass
            else:
                logger.error(f"❌ Failed to create Spark session after {max_retries} attempts")
                raise

def safe_spark_operation(operation_func, *args, **kwargs):
    """Wrapper for safe Spark operations with error handling"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return operation_func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Spark operation failed (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise

def read_mongo_to_spark(spark, database, collection, max_retries=3):
    """Read data from MongoDB using Spark with retry mechanism"""
    for attempt in range(max_retries):
        try:
            mongo_uri = f"{MONGODB_URI}{database}.{collection}"
            logger.info(f"Reading from MongoDB: {mongo_uri} (attempt {attempt+1})")

            df = spark.read \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", mongo_uri) \
                .option("readPreference.name", "secondaryPreferred") \
                .option("partitioner", "MongoSamplePartitioner") \
                .option("partitionerOptions.partitionSizeMB", "64") \
                .load()
            # Test the DataFrame
            count = df.count()
            logger.info(f"✅ Successfully read {count} records from {database}.{collection}")
            return df
        except Exception as e:
            logger.error(f"❌ Error reading from MongoDB {database}.{collection} (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(10)
            else:
                raise

def write_spark_to_mongo(df, database, collection, mode="overwrite", max_retries=3):
    """Write Spark DataFrame to MongoDB with retry mechanism"""
    for attempt in range(max_retries):
        try:
            mongo_uri = f"{MONGODB_URI}{database}.{collection}"
            logger.info(f"Writing to MongoDB: {mongo_uri} (attempt {attempt+1})")

            df.write \
                .format("com.mongodb.spark.sql.DefaultSource") \
                .option("uri", mongo_uri) \
                .mode(mode) \
                .save()
            logger.info(f"✅ Successfully wrote data to {database}.{collection}")
            return
        except Exception as e:
            logger.error(f"❌ Error writing to MongoDB {database}.{collection} (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                time.sleep(10)
            else:
                raise

# ======= SPARK TRANSFORM & LOAD FUNCTIONS =======
def transform_single_stock_spark(spark, stock_info):
    """
    Transforms data for a single stock using Spark with comprehensive error handling.
    This function expects an active SparkSession to be passed to it.
    """
    collection_name = stock_info['collection_name']
    company_name = stock_info['company_name']
    stock_code = stock_info['stock_code']
    logger.info(f"🔄 Starting Spark transformation for {company_name} ({stock_code})")
    raw_df = None
    try:
        # Read raw data from MongoDB using Spark
        raw_df = read_mongo_to_spark(spark, DB_EXTRACT, collection_name)
        if raw_df.count() == 0:
            logger.warning(f"⚠️ Empty data in DB_EXTRACT for {company_name}. Skipping transformation.")
            return 0

        # Cache the raw dataframe
        raw_df.cache()
        logger.info(f"🚀 Cached raw dataframe for {company_name}")

        # Handle column name inconsistencies
        available_columns = raw_df.columns
        logger.info(f"📋 Available columns for {company_name}: {available_columns}")
        if "Stock Splits" in raw_df.columns:
            raw_df = raw_df.withColumnRenamed("Stock Splits", "Stock_Splits")

        # Ensure all required columns are present
        expected_cols = ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits"]
        for col_name in expected_cols:
            if col_name not in raw_df.columns:
                raw_df = raw_df.withColumn(col_name, lit(0.0).cast(DoubleType()))
                logger.warning(f"⚠️ Column {col_name} not found in {company_name}. Added with default 0.0.")
            else:
                # Ensure numeric type and handle NaNs/nulls
                target_type = LongType() if col_name == "Volume" else DoubleType()
                raw_df = raw_df.withColumn(col_name,
                                         when(isnan(col(col_name)) | isnull(col(col_name)), 0.0)
                                         .otherwise(col(col_name).cast(target_type)))

        # === 1. DAILY DATA (agg_type = "day") ===
        logger.info(f"📅 Processing daily data for {company_name}")
        daily_df = raw_df.select(
            date_format(col("Date"), "yyyy-MM-dd").alias("period_key"),
            lit("day").alias("agg_type"),
            col("Date"),
            col("Open").cast("double"),
            col("High").cast("double"),
            col("Low").cast("double"),
            col("Close").cast("double"),
            col("Volume").cast("long"),
            col("Dividends").cast("double"),
            col("Stock_Splits").cast("double"),
            lit(company_name).alias("company_name"),
            lit(stock_code).alias("stock_code"),
            lit(datetime.now().isoformat()).alias("processed_timestamp")
        ).distinct()
        daily_count = daily_df.count()
        logger.info(f"✅ Daily data processed for {company_name}: {daily_count} records")

        # === 2. MONTHLY AGGREGATION ===
        logger.info(f"📅 Creating monthly aggregation for {company_name}")
        monthly_df = raw_df.withColumn("year_month", date_format(col("Date"), "yyyy-MM")) \
            .groupBy("year_month") \
            .agg(
                avg("Open").alias("avg_open"), avg("High").alias("avg_high"),
                avg("Low").alias("avg_low"), avg("Close").alias("avg_close"),
                avg("Volume").alias("avg_volume"),
                spark_sum("Dividends").alias("sum_dividends"),
                spark_sum("Stock_Splits").alias("sum_stock_splits"),
                spark_max("Open").alias("max_open"), spark_max("High").alias("max_high"),
                spark_max("Low").alias("max_low"), spark_max("Close").alias("max_close"),
                spark_max("Volume").alias("max_volume"),
                spark_min("Open").alias("min_open"), spark_min("High").alias("min_high"),
                spark_min("Low").alias("min_low"), spark_min("Close").alias("min_close"),
                spark_min("Volume").alias("min_volume"),
                stddev("Open").alias("std_open"), stddev("High").alias("std_high"),
                stddev("Low").alias("std_low"), stddev("Close").alias("std_close"),
                count("*").alias("row_count")
            ) \
            .select(
                col("year_month").alias("period_key"),
                lit("month").alias("agg_type"),
                to_date(concat(col("year_month"), lit("-01")), "yyyy-MM-dd").alias("Date"),
                col("avg_open").cast("double"), col("avg_high").cast("double"),
                col("avg_low").cast("double"), col("avg_close").cast("double"),
                col("avg_volume").cast("long"),
                col("sum_dividends").cast("double"), col("sum_stock_splits").cast("double"),
                col("max_open").cast("double"), col("max_high").cast("double"),
                col("max_low").cast("double"), col("max_close").cast("double"),
                col("max_volume").cast("long"),
                col("min_open").cast("double"), col("min_high").cast("double"),
                col("min_low").cast("double"), col("min_close").cast("double"),
                col("min_volume").cast("long"),
                col("std_open").cast("double"), col("std_high").cast("double"),
                col("std_low").cast("double"), col("std_close").cast("double"),
                lit(company_name).alias("company_name"),
                lit(stock_code).alias("stock_code"),
                col("row_count").cast("int"),
                lit(datetime.now().isoformat()).alias("processed_timestamp")
            )
        monthly_count = monthly_df.count()
        logger.info(f"✅ Monthly data processed for {company_name}: {monthly_count} records")

        # === 3. YEARLY AGGREGATION ===
        logger.info(f"📅 Creating yearly aggregation for {company_name}")
        yearly_df = raw_df.withColumn("year", year(col("Date")).cast("string")) \
            .groupBy("year") \
            .agg(
                avg("Open").alias("avg_open"), avg("High").alias("avg_high"),
                avg("Low").alias("avg_low"), avg("Close").alias("avg_close"),
                avg("Volume").alias("avg_volume"),
                spark_sum("Dividends").alias("sum_dividends"),
                spark_sum("Stock_Splits").alias("sum_stock_splits"),
                spark_max("Open").alias("max_open"), spark_max("High").alias("max_high"),
                spark_max("Low").alias("max_low"), spark_max("Close").alias("max_close"),
                spark_max("Volume").alias("max_volume"),
                spark_min("Open").alias("min_open"), spark_min("High").alias("min_high"),
                spark_min("Low").alias("min_low"), spark_min("Close").alias("min_close"),
                spark_min("Volume").alias("min_volume"),
                stddev("Open").alias("std_open"), stddev("High").alias("std_high"),
                stddev("Low").alias("std_low"), stddev("Close").alias("std_close"),
                count("*").alias("row_count")
            ) \
            .select(
                col("year").alias("period_key"),
                lit("year").alias("agg_type"),
                to_date(concat(col("year"), lit("-01-01")), "yyyy-MM-dd").alias("Date"),
                col("avg_open").cast("double"), col("avg_high").cast("double"),
                col("avg_low").cast("double"), col("avg_close").cast("double"),
                col("avg_volume").cast("long"),
                col("sum_dividends").cast("double"), col("sum_stock_splits").cast("double"),
                col("max_open").cast("double"), col("max_high").cast("double"),
                col("max_low").cast("double"), col("max_close").cast("double"),
                col("max_volume").cast("long"),
                col("min_open").cast("double"), col("min_high").cast("double"),
                col("min_low").cast("double"), col("min_close").cast("double"),
                col("min_volume").cast("long"),
                col("std_open").cast("double"), col("std_high").cast("double"),
                col("std_low").cast("double"), col("std_close").cast("double"),
                lit(company_name).alias("company_name"),
                lit(stock_code).alias("stock_code"),
                col("row_count").cast("int"),
                lit(datetime.now().isoformat()).alias("processed_timestamp")
            )
        yearly_count = yearly_df.count()
        logger.info(f"✅ Yearly data processed for {company_name}: {yearly_count} records")

        # === 4. WRITE TO MONGODB ===
        logger.info(f"💾 Writing aggregated data to MongoDB for {company_name}")

        # Write each aggregation level separately
        write_spark_to_mongo(daily_df, DB_TRANSFORM, f"{collection_name}_daily", mode="overwrite")
        write_spark_to_mongo(monthly_df, DB_TRANSFORM, f"{collection_name}_monthly", mode="overwrite")
        write_spark_to_mongo(yearly_df, DB_TRANSFORM, f"{collection_name}_yearly", mode="overwrite")

        # === 5. COMBINED COLLECTION ===
        logger.info(f"🔄 Creating combined collection for {company_name}")

        # Simple union of all data with basic columns
        combined_df = daily_df.select(
            "period_key", "agg_type", "Date", "company_name", "stock_code", "processed_timestamp"
        ).unionByName(
            monthly_df.select(
                "period_key", "agg_type", "Date", "company_name", "stock_code", "processed_timestamp"
            )
        ).unionByName(
            yearly_df.select(
                "period_key", "agg_type", "Date", "company_name", "stock_code", "processed_timestamp"
            )
        )

        write_spark_to_mongo(combined_df, DB_TRANSFORM, collection_name, mode="overwrite")

        total_docs_transformed = daily_count + monthly_count + yearly_count
        logger.info(f"✅ Spark transformation completed for {company_name}:")
        logger.info(f"     - Daily: {daily_count} records")
        logger.info(f"     - Monthly: {monthly_count} records")
        logger.info(f"     - Yearly: {yearly_count} records")
        logger.info(f"     - Total: {total_docs_transformed} documents")
        return total_docs_transformed
    except Exception as e:
        logger.error(f"❌ Error during Spark transformation for {company_name}: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return 0
    finally:
        # Clean up cache
        if raw_df is not None:
            try:
                raw_df.unpersist()
                logger.info(f"🧹 Unpersisted cached dataframe for {company_name}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to unpersist dataframe for {company_name}: {e}")

def transform_data_spark(**kwargs):
    """
    Main transformation function with comprehensive error handling.
    Uses a single Spark session for the entire task.
    """
    logger.info("🔄 Starting Spark data transformation process")
    log_memory_usage("spark_transform_start")

    spark = None
    try:
        # Initialize Spark session
        spark = get_spark_session("YFinance_Transform")

        logger.info("📊 Spark session connected to MongoDB. Proceeding to process collections.")

        # Read CSV file
        daftar_saham_path = '/opt/airflow/data/Daftar_Saham.csv'
        if not os.path.exists(daftar_saham_path):
            logger.error(f"File {daftar_saham_path} not found!")
            raise FileNotFoundError(f"File {daftar_saham_path} not found!")
        logger.info("📋 Reading stock list from CSV")
        data_csv = pd.read_csv(daftar_saham_path)

        # Process limited number of stocks
        MAX_SAHAM = 50  # Reduced for testing
        logger.info(f"🎯 Processing first {MAX_SAHAM} stocks from CSV")
        transformed_stocks_list = []
        total_transformed_docs = 0

        # Process each stock within the same Spark session
        for idx, row in data_csv.iterrows():
            if idx >= MAX_SAHAM:
                logger.info(f"Limiting transformation to first {MAX_SAHAM} stocks.")
                break

            kode_saham = row['Kode'] + '.JK'
            nama_perusahaan = row['Nama Perusahaan']
            collection_name = nama_perusahaan.replace(" ", "_")
            logger.info(f"🔄 Processing Spark transformation for {nama_perusahaan} ({kode_saham}) [Stock {idx+1}/{MAX_SAHAM}]")

            stock_info = {
                'company_name': nama_perusahaan,
                'collection_name': collection_name,
                'stock_code': kode_saham,
                'csv_index': idx
            }
            try:
                # Call the single stock transformation function
                count = transform_single_stock_spark(spark, stock_info)
                if count > 0:
                    transformed_stocks_list.append(stock_info)
                    total_transformed_docs += count
                    logger.info(f"✅ Transformation {nama_perusahaan} completed: {count} documents")
                # Log memory usage periodically
                if (idx + 1) % 10 == 0:
                    log_memory_usage(f"spark_transform_progress_stock_{idx+1}")
            except Exception as e:
                logger.error(f"❌ Failed Spark transformation for {nama_perusahaan}: {e}")
                import traceback
                logger.error(f"❌ Traceback: {traceback.format_exc()}")
                # Crucial: Continue with next stock instead of failing the entire job
                continue

            # Add a small sleep to give the system some breathing room between stocks
            time.sleep(3) # Jeda 3 detik setelah memproses setiap saham

        # Save transformed stocks list
        transformed_stocks_file = f"{TEMP_DIR}/transformed_stocks.pkl"
        with open(transformed_stocks_file, "wb") as f:
            pickle.dump(transformed_stocks_list, f)
        logger.info(f"🗳️ List of transformed stocks saved to: {transformed_stocks_file}")
        logger.info(f"🎉 All Spark transformations completed: {len(transformed_stocks_list)} stocks, {total_transformed_docs} total documents")
        log_memory_usage("spark_transform_end")

    except Exception as e:
        logger.error(f"🚨 Error in transform_data_spark: {e}")
        import traceback
        logger.error(f"🚨 Full traceback: {traceback.format_exc()}")
        raise
    finally:
        # Clean up resources
        if spark:
            try:
                spark.stop()
                logger.info("🧹 Spark session stopped")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping Spark session: {e}")

def load_single_stock_spark(spark, stock_info):
    """Load data for a single stock from DB transform to DB load using Spark.
    This function expects an active SparkSession to be passed to it.
    """
    collection_name = stock_info['collection_name']
    company_name = stock_info['company_name']
    logger.info(f"⬆️ Starting Spark load for {company_name}")
    try:
        # Read data from DB_TRANSFORM using Spark
        transformed_df = read_mongo_to_spark(spark, DB_TRANSFORM, collection_name)
        if transformed_df.count() == 0:
            logger.warning(f"⚠️ No transformed data for {company_name} in DB_TRANSFORM. Skipping load.")
            return 0

        # Write data to DB_LOAD using Spark
        write_spark_to_mongo(transformed_df, DB_LOAD, collection_name, mode="overwrite")
        total_docs_loaded = transformed_df.count()
        logger.info(f"✅ Spark load {company_name} completed: {total_docs_loaded} documents")
        return total_docs_loaded
    except Exception as e:
        logger.error(f"❌ Error in Spark load for {company_name}: {e}")
        return 0

def load_data_to_mongodb_spark(**kwargs):
    """Load transformed data to final MongoDB using Spark.
    Uses a single Spark session for the entire task.
    """
    logger.info("⬆️ Starting Spark load to final MongoDB")
    log_memory_usage("spark_load_start")
    spark = None
    try:
        # Initialize Spark session
        spark = get_spark_session("YFinance_Load")

        transformed_stocks_file = f"{TEMP_DIR}/transformed_stocks.pkl"
        if not os.path.exists(transformed_stocks_file):
            logger.error(f"File {transformed_stocks_file} not found! Transformation might have failed.")
            logger.info("No transformed stocks file found. No data to load.")
            return 0

        with open(transformed_stocks_file, "rb") as f:
            transformed_stocks = pickle.load(f)

        total_stocks_loaded = 0
        total_docs_loaded = 0
        for stock_info in transformed_stocks:
            try:
                # Call the single stock load function
                count = load_single_stock_spark(spark, stock_info)
                if count > 0:
                    total_stocks_loaded += 1
                    total_docs_loaded += count
            except Exception as e:
                logger.error(f"❌ Failed Spark load for {stock_info['company_name']}: {e}")
                # Crucial: Continue to next stock if one fails
                continue

        logger.info(f"📦 Spark load total: {total_stocks_loaded} stocks loaded, {total_docs_loaded} documents")
        log_memory_usage("spark_load_end")

    except Exception as e:
        logger.error(f"🚨 Error in load_data_to_mongodb_spark: {e}")
        import traceback
        logger.error(f"🚨 Full traceback: {traceback.format_exc()}")
        raise
    finally:
        # Clean up temporary files
        try:
            transformed_stocks_file = f"{TEMP_DIR}/transformed_stocks.pkl"
            if os.path.exists(transformed_stocks_file):
                os.remove(transformed_stocks_file)
                logger.info(f"🧹 Temporary file {transformed_stocks_file} cleaned up")
        except Exception as e:
            logger.warning(f"⚠️ Failed to clean up temporary files: {e}")

        # Stop Spark session
        if spark:
            try:
                spark.stop()
                logger.info("🧹 Spark session stopped")
            except Exception as e:
                logger.warning(f"⚠️ Error stopping Spark session: {e}")

# --- DAG Definition ---
default_args_transform_load = {
    'owner': 'airflow',
    'retries': 1,    # Reduced retries
    'retry_delay': timedelta(minutes=15),    # Increased retry delay
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': airflow_timezone.datetime(2024, 6, 1),
}

with DAG(
    dag_id='etl_yfinance_spark_transform_load_dag',
    description='DAG for Stock Data Transformation and Load with Spark and Aggregation',
    schedule_interval=timedelta(days=1),    # Runs daily
    catchup=False,    # Prevents backfilling past runs
    default_args=default_args_transform_load,
    tags=['yfinance', 'etl', 'spark', 'mongodb', 'transform', 'load'],
) as dag:
    


    start_transform_load = DummyOperator(
        task_id='start_spark_transform_load',
    )

    transform_data = PythonOperator(
        task_id='transform_data_with_spark',
        python_callable=transform_data_spark,
    )

    load_data = PythonOperator(
        task_id='load_transformed_data_to_mongodb',
        python_callable=load_data_to_mongodb_spark,
    )

    end_transform_load = DummyOperator(
        task_id='end_spark_transform_load',
    )

    # Define task dependencie
    start_transform_load >> transform_data >> load_data >> end_transform_load