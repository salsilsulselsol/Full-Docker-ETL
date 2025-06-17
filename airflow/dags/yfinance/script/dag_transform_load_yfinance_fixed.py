from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

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
BATCH_SIZE = 1000 # This batch size is for MongoDB insert/read within Spark, less relevant for Spark itself
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
            
            # Action to trigger evaluation and potentially catch issues early
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
    raw_df = None # Initialize to None for finally block
    try:
        # Read raw data from MongoDB using Spark
        raw_df = read_mongo_to_spark(spark, DB_EXTRACT, collection_name)
        if raw_df.count() == 0:
            logger.warning(f"⚠️ Empty data in DB_EXTRACT for {company_name}. Skipping transformation.")
            return 0

        # Cache the raw dataframe to avoid re-reading
        raw_df.cache()
        logger.info(f"🚀 Cached raw dataframe for {company_name}")

        # Handle column name inconsistencies (e.g., "Stock Splits" vs "Stock_Splits")
        if "Stock Splits" in raw_df.columns:
            raw_df = raw_df.withColumnRenamed("Stock Splits", "Stock_Splits")

        # Ensure all required columns are present and cast to appropriate types
        expected_cols = ["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock_Splits"]
        for col_name in expected_cols:
            if col_name not in raw_df.columns:
                raw_df = raw_df.withColumn(col_name, lit(0.0).cast(DoubleType()))
                logger.warning(f"⚠️ Column {col_name} not found in {company_name}. Added with default 0.0.")
            else:
                # Ensure numeric type and handle NaNs/nulls by filling with 0.0
                target_type = LongType() if col_name == "Volume" else DoubleType()
                raw_df = raw_df.withColumn(col_name,
                                           when(isnan(col(col_name)) | isnull(col(col_name)), 0.0)
                                           .otherwise(col(col_name).cast(target_type)))
        
        # Ensure 'Date' column is a proper DateType
        if 'Date' not in raw_df.columns or str(raw_df.schema['Date'].dataType) != 'TimestampType':
             logger.warning(f"Date column is not TimestampType for {company_name}. Attempting to cast.")
             raw_df = raw_df.withColumn("Date", col("Date").cast(TimestampType()))
        
        # Drop rows where 'Date' is null after casting
        initial_count = raw_df.count()
        raw_df = raw_df.filter(col("Date").isNotNull())
        if raw_df.count() < initial_count:
            logger.warning(f"Dropped {initial_count - raw_df.count()} rows with null Date for {company_name}.")


        # === 1. DAILY DATA (agg_type = "day") ===
        logger.info(f"📅 Processing daily data for {company_name}")
        daily_df = raw_df.select(
            date_format(col("Date"), "yyyy-MM-dd").alias("period_key"),
            lit("day").alias("agg_type"),
            col("Date").alias("record_date"), # Renamed to avoid confusion with period_key
            col("Open"),
            col("High"),
            col("Low"),
            col("Close"),
            col("Volume"),
            col("Dividends"),
            col("Stock_Splits"),
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
                spark_max("Low").alias("min_low"), spark_max("Close").alias("max_close"), # Fix: Changed max_low to min_low here in aggregation
                spark_max("Volume").alias("max_volume"),
                spark_min("Open").alias("min_open"), spark_min("High").alias("min_high"),
                spark_min("Low").alias("min_low_fix"), spark_min("Close").alias("min_close"), # Fix: Changed min_low to min_low_fix to avoid conflict
                spark_min("Volume").alias("min_volume"),
                stddev("Open").alias("std_open"), stddev("High").alias("std_high"),
                stddev("Low").alias("std_low"), stddev("Close").alias("std_close"),
                count("*").alias("row_count")
            ) \
            .select(
                col("year_month").alias("period_key"),
                lit("month").alias("agg_type"),
                to_date(concat(col("year_month"), lit("-01")), "yyyy-MM-dd").alias("record_date"),
                col("avg_open").cast("double"), col("avg_high").cast("double"),
                col("avg_low").cast("double"), col("avg_close").cast("double"),
                col("avg_volume").cast("long"),
                col("sum_dividends").cast("double"), col("sum_stock_splits").cast("double"),
                col("max_open").cast("double"), col("max_high").cast("double"),
                col("min_low_fix").alias("max_low").cast("double"), col("max_close").cast("double"), # Fix: Renamed back and casted
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
                to_date(concat(col("year"), lit("-01-01")), "yyyy-MM-dd").alias("record_date"),
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

        # === 4. WRITE TO MONGODB (DB_TRANSFORM) ===
        logger.info(f"💾 Writing aggregated data to DB_TRANSFORM for {company_name}")

        write_spark_to_mongo(daily_df, DB_TRANSFORM, f"{collection_name}_daily", mode="overwrite")
        write_spark_to_mongo(monthly_df, DB_TRANSFORM, f"{collection_name}_monthly", mode="overwrite")
        write_spark_to_mongo(yearly_df, DB_TRANSFORM, f"{collection_name}_yearly", mode="overwrite")

        # === 5. COMBINED COLLECTION (metadata for reporting) ===
        logger.info(f"🔄 Creating combined collection for {company_name} (metadata)")

        # Create a metadata-like combined DataFrame for the main collection in DB_TRANSFORM
        # This can hold a summary or distinct entries
        # For simplicity, let's create a "master" collection that stores unique stock info and the latest processed timestamp
        # This part assumes you want ONE document per stock in the main collection, 
        # listing its metadata and last processed timestamp.
        
        # Get the latest timestamp from the daily data
        latest_daily_date = daily_df.select(spark_max("record_date")).collect()[0][0]
        
        combined_metadata_df = spark.createDataFrame(
            [(
                collection_name, 
                company_name, 
                stock_code, 
                daily_count, 
                monthly_count, 
                yearly_count, 
                latest_daily_date.isoformat(), 
                datetime.now().isoformat()
            )],
            ["collection_name", "company_name", "stock_code", "daily_records", 
             "monthly_records", "yearly_records", "latest_daily_data_date", "processed_timestamp"]
        )
        
        write_spark_to_mongo(combined_metadata_df, DB_TRANSFORM, collection_name, mode="append") # Append to main collection

        total_docs_transformed = daily_count + monthly_count + yearly_count
        logger.info(f"✅ Spark transformation completed for {company_name}:")
        logger.info(f"    - Daily: {daily_count} records")
        logger.info(f"    - Monthly: {monthly_count} records")
        logger.info(f"    - Yearly: {yearly_count} records")
        logger.info(f"    - Total aggregated documents: {total_docs_transformed} documents")
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
    transformed_stocks_list = [] # Initialize list here
    try:
        # Initialize Spark session
        spark = get_spark_session("YFinance_Transform")

        logger.info("📊 Spark session connected to MongoDB. Proceeding to process collections.")

        # Read the list of successfully processed stocks from the extraction DAG
        processed_stocks_file = f"{TEMP_DIR}/processed_stocks.pkl"
        if not os.path.exists(processed_stocks_file):
            logger.error(f"File {processed_stocks_file} not found! Extraction DAG might have failed or not run.")
            raise FileNotFoundError(f"File {processed_stocks_file} not found!")
            
        with open(processed_stocks_file, "rb") as f:
            extracted_stocks_info = pickle.load(f)
        logger.info(f"📋 Loaded {len(extracted_stocks_info)} stocks from previous extraction.")

        total_transformed_docs = 0

        # Process each stock within the same Spark session
        for idx, stock_info_from_extract in enumerate(extracted_stocks_info):
            company_name = stock_info_from_extract['company_name']
            stock_code = stock_info_from_extract['stock_code']
            collection_name = stock_info_from_extract['collection_name']

            logger.info(f"🔄 Processing Spark transformation for {company_name} ({stock_code}) [Stock {idx+1}/{len(extracted_stocks_info)}]")

            stock_info = {
                'company_name': company_name,
                'collection_name': collection_name,
                'stock_code': stock_code,
                'csv_index': idx # Original index from CSV if needed, though not directly used now
            }
            try:
                # Call the single stock transformation function
                count = transform_single_stock_spark(spark, stock_info)
                if count > 0:
                    transformed_stocks_list.append(stock_info)
                    total_transformed_docs += count
                    logger.info(f"✅ Transformation for {company_name} completed: {count} documents")
                
                # Log memory usage periodically
                if (idx + 1) % 10 == 0:
                    log_memory_usage(f"spark_transform_progress_stock_{idx+1}")
            except Exception as e:
                logger.error(f"❌ Failed Spark transformation for {company_name}: {e}")
                # Crucial: Continue with next stock instead of failing the entire job
                continue

            # Add a small sleep to give the system some breathing room between stocks
            time.sleep(3) # Pause 3 seconds after processing each stock

        # Save transformed stocks list for the load step
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
        # Clean up Spark session
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
    transformed_df = None # Initialize to None for finally block
    try:
        # Read data from DB_TRANSFORM using Spark (specifically the main collection, not the daily/monthly/yearly ones)
        # Assuming we are loading the aggregated daily, monthly, yearly data into the final DB_LOAD
        
        # Read each aggregated collection and union them for the final load, or load them separately.
        # For this example, I'll load them separately to keep the final DB structured similarly to transform.
        
        total_docs_loaded_current_stock = 0
        
        # Load daily data
        daily_collection_name = f"{collection_name}_daily"
        transformed_df_daily = read_mongo_to_spark(spark, DB_TRANSFORM, daily_collection_name)
        if transformed_df_daily.count() > 0:
            write_spark_to_mongo(transformed_df_daily, DB_LOAD, daily_collection_name, mode="overwrite")
            total_docs_loaded_current_stock += transformed_df_daily.count()
            logger.info(f"Loaded {transformed_df_daily.count()} daily records for {company_name}")
            transformed_df_daily.unpersist() # Release memory
            del transformed_df_daily

        # Load monthly data
        monthly_collection_name = f"{collection_name}_monthly"
        transformed_df_monthly = read_mongo_to_spark(spark, DB_TRANSFORM, monthly_collection_name)
        if transformed_df_monthly.count() > 0:
            write_spark_to_mongo(transformed_df_monthly, DB_LOAD, monthly_collection_name, mode="overwrite")
            total_docs_loaded_current_stock += transformed_df_monthly.count()
            logger.info(f"Loaded {transformed_df_monthly.count()} monthly records for {company_name}")
            transformed_df_monthly.unpersist() # Release memory
            del transformed_df_monthly
        
        # Load yearly data
        yearly_collection_name = f"{collection_name}_yearly"
        transformed_df_yearly = read_mongo_to_spark(spark, DB_TRANSFORM, yearly_collection_name)
        if transformed_df_yearly.count() > 0:
            write_spark_to_mongo(transformed_df_yearly, DB_LOAD, yearly_collection_name, mode="overwrite")
            total_docs_loaded_current_stock += transformed_df_yearly.count()
            logger.info(f"Loaded {transformed_df_yearly.count()} yearly records for {company_name}")
            transformed_df_yearly.unpersist() # Release memory
            del transformed_df_yearly
            
        # Also load the metadata collection (if you want it in DB_LOAD as well)
        metadata_collection_name = collection_name # The main collection in DB_TRANSFORM
        transformed_df_metadata = read_mongo_to_spark(spark, DB_TRANSFORM, metadata_collection_name)
        if transformed_df_metadata.count() > 0:
            write_spark_to_mongo(transformed_df_metadata, DB_LOAD, metadata_collection_name, mode="overwrite")
            total_docs_loaded_current_stock += transformed_df_metadata.count()
            logger.info(f"Loaded {transformed_df_metadata.count()} metadata records for {company_name}")
            transformed_df_metadata.unpersist() # Release memory
            del transformed_df_metadata


        if total_docs_loaded_current_stock == 0:
             logger.warning(f"⚠️ No data was loaded for {company_name} from DB_TRANSFORM to DB_LOAD.")
             return 0
             
        logger.info(f"✅ Spark load for {company_name} completed: {total_docs_loaded_current_stock} documents")
        return total_docs_loaded_current_stock
    except Exception as e:
        logger.error(f"❌ Error in Spark load for {company_name}: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
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
        logger.info(f"📋 Loaded {len(transformed_stocks)} stocks to load from previous transformation.")

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
    'retries': 1,  # Reduced retries for faster failure feedback
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=10), # Increased timeout for Spark tasks
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='etl_yfinance_spark_transform_load_dag',
    description='DAG for Stock Data Transformation and Load with Spark and Aggregation',
    start_date=days_ago(1),
    schedule_interval=None, # This DAG will be triggered by ExternalTaskSensor
    catchup=False,
    default_args=default_args_transform_load,
    max_active_runs=1,
    tags=['yfinance', 'etl', 'spark', 'mongodb', 'transform', 'load'],
) as dag:
    
    # Sensor to wait for the successful completion of the extraction DAG
    wait_for_extraction = ExternalTaskSensor(
        task_id='wait_for_yfinance_extraction',
        external_dag_id='etl_yfinance_extract_fixed_dag',
        external_task_id='extract_yfinance_data', # The task_id of the extract operation in the other DAG
        mode='poke', # Keep polling for completion
        timeout=timedelta(hours=9), # Timeout after 9 hours if extract doesn't complete
        poke_interval=60, # Check every 60 seconds
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )

    start_transform_load = DummyOperator(
        task_id='start_spark_transform_load',
    )

    transform_data = PythonOperator(
        task_id='transform_data_with_spark',
        python_callable=transform_data_spark,
        # Set a higher memory limit for this task if needed
        # executor_memory='4g', # This is more for kubernetes/mesos, not localExecutor
        # driver_memory='4g' # This is for spark configurations within the python callable
    )

    load_data = PythonOperator(
        task_id='load_transformed_data_to_mongodb',
        python_callable=load_data_to_mongodb_spark,
    )

    end_transform_load = DummyOperator(
        task_id='end_spark_transform_load',
    )

    # Define task dependencies
    wait_for_extraction >> start_transform_load >> transform_data >> load_data >> end_transform_load