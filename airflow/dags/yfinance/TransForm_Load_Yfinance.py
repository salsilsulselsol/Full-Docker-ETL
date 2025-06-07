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
from pymongo import MongoClient, errors

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
    IntegerType, DateType, TimestampType,LongType 

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
    process = psutil.Process()
    memory_info = process.memory_info()
    logger.info(f"[{task_name}] Memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")

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

def get_spark_session(app_name="YFinance_ETL"):
    """Create and configure Spark session with OPTIMIZED settings"""
    try:
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1") \
            .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "4") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
            .config("spark.mongodb.input.uri", f"{MONGODB_URI}{DB_EXTRACT}") \
            .config("spark.mongodb.output.uri", f"{MONGODB_URI}{DB_LOAD}") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.maxResultSize", "1g") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        logger.info("✅ Optimized Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"❌ Failed to create Spark session: {e}")
        raise

def read_mongo_to_spark(spark, database, collection):
    """Read data from MongoDB using Spark"""
    try:
        mongo_uri = f"{MONGODB_URI}{database}.{collection}"
        df = spark.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", mongo_uri) \
            .load()

        logger.info(f"✅ Successfully read {df.count()} records from {database}.{collection}")
        return df
    except Exception as e:
        logger.error(f"❌ Error reading from MongoDB {database}.{collection}: {e}")
        raise

def write_spark_to_mongo(df, database, collection, mode="overwrite"):
    """Write Spark DataFrame to MongoDB"""
    try:
        mongo_uri = f"{MONGODB_URI}{database}.{collection}"
        df.write \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("uri", mongo_uri) \
            .mode(mode) \
            .save()

        logger.info(f"✅ Successfully wrote data to {database}.{collection}")
    except Exception as e:
        logger.error(f"❌ Error writing to MongoDB {database}.{collection}: {e}")
        raise

# ======= SPARK TRANSFORM & LOAD FUNCTIONS =======

def transform_single_stock_spark(spark, stock_info):
    """
    Transforms data for a single stock using Spark - FIXED VERSION
    Creates proper daily, monthly, and yearly aggregations
    """
    collection_name = stock_info['collection_name']
    company_name = stock_info['company_name']
    stock_code = stock_info['stock_code']

    logger.info(f"🔄 Starting Spark transformation for {company_name} ({stock_code})")

    try:
        # Read raw data from MongoDB using Spark
        raw_df = read_mongo_to_spark(spark, DB_EXTRACT, collection_name)

        if raw_df.count() == 0:
            logger.warning(f"⚠️ Empty data in DB_EXTRACT for {company_name}. Skipping transformation.")
            return 0

        # Cache the raw dataframe
        raw_df.cache()
        logger.info(f"🚀 Cached raw dataframe for {company_name}")

        # Log available columns
        available_columns = raw_df.columns
        logger.info(f"📋 Available columns for {company_name}: {available_columns}")

        # Handle column name inconsistencies
        if "Stock Splits" in raw_df.columns:
            raw_df = raw_df.withColumnRenamed("Stock Splits", "Stock_Splits")
        if "Stock_Splits" not in raw_df.columns:
            raw_df = raw_df.withColumn("Stock_Splits", lit(0.0))
        if "Dividends" not in raw_df.columns:
            raw_df = raw_df.withColumn("Dividends", lit(0.0))

        # Clean and prepare data
        required_numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        for num_col in required_numeric_cols:
            if num_col in raw_df.columns:
                raw_df = raw_df.withColumn(num_col,
                    when(isnan(col(num_col)) | isnull(col(num_col)), 0.0)
                    .otherwise(col(num_col)))
            else:
                logger.warning(f"⚠️ Required column {num_col} not found in {company_name}")
                raw_df = raw_df.withColumn(num_col, lit(0.0))

        # Handle optional columns
        for opt_col in ["Dividends", "Stock_Splits"]:
            if opt_col in raw_df.columns:
                raw_df = raw_df.withColumn(opt_col,
                    when(isnan(col(opt_col)) | isnull(col(opt_col)), 0.0)
                    .otherwise(col(opt_col)))
            else:
                raw_df = raw_df.withColumn(opt_col, lit(0.0))

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
        ).distinct()  # Remove duplicates if any

        daily_df = daily_df.coalesce(2)
        daily_count = daily_df.count()
        logger.info(f"✅ Daily data processed for {company_name}: {daily_count} records")

        # === 2. MONTHLY AGGREGATION (agg_type = "month") ===
        logger.info(f"📅 Creating monthly aggregation for {company_name}")

        # Group by year-month and calculate all required aggregations
        monthly_df = raw_df.withColumn("year_month", date_format(col("Date"), "yyyy-MM")) \
            .groupBy("year_month") \
            .agg(
                avg("Open").alias("avg_open"),
                avg("High").alias("avg_high"),
                avg("Low").alias("avg_low"),
                avg("Close").alias("avg_close"),
                avg("Volume").alias("avg_volume"),
                spark_sum("Dividends").alias("sum_dividends"),
                spark_sum("Stock_Splits").alias("sum_stock_splits"),
                spark_max("Open").alias("max_open"),
                spark_max("High").alias("max_high"),
                spark_max("Low").alias("max_low"),
                spark_max("Close").alias("max_close"),
                spark_max("Volume").alias("max_volume"),
                spark_max("Dividends").alias("max_dividends"),
                spark_max("Stock_Splits").alias("max_stock_splits"),
                spark_min("Open").alias("min_open"),
                spark_min("High").alias("min_high"),
                spark_min("Low").alias("min_low"),
                spark_min("Close").alias("min_close"),
                spark_min("Volume").alias("min_volume"),
                spark_min("Dividends").alias("min_dividends"),
                spark_min("Stock_Splits").alias("min_stock_splits"),
                stddev("Open").alias("std_open"),
                stddev("High").alias("std_high"),
                stddev("Low").alias("std_low"),
                stddev("Close").alias("std_close"),
                stddev("Volume").alias("std_volume"),
                stddev("Dividends").alias("std_dividends"),
                stddev("Stock_Splits").alias("std_stock_splits"),
                count("*").alias("row_count")
            ) \
            .select(
                col("year_month").alias("period_key"),
                lit("month").alias("agg_type"),
                # Create a date for the first day of the month
                to_date(concat(col("year_month"), lit("-01")), "yyyy-MM-dd").alias("Date"),
                col("avg_open").cast("double"),
                col("avg_high").cast("double"),
                col("avg_low").cast("double"),
                col("avg_close").cast("double"),
                col("avg_volume").cast("long"),
                col("sum_dividends").cast("double"),
                col("sum_stock_splits").cast("double"),
                col("max_open").cast("double"),
                col("max_high").cast("double"),
                col("max_low").cast("double"),
                col("max_close").cast("double"),
                col("max_volume").cast("long"),
                col("max_dividends").cast("double"),
                col("max_stock_splits").cast("double"),
                col("min_open").cast("double"),
                col("min_high").cast("double"),
                col("min_low").cast("double"),
                col("min_close").cast("double"),
                col("min_volume").cast("long"),
                col("min_dividends").cast("double"),
                col("min_stock_splits").cast("double"),
                col("std_open").cast("double"),
                col("std_high").cast("double"),
                col("std_low").cast("double"),
                col("std_close").cast("double"),
                col("std_volume").cast("double"), # Volume stddev can be float
                col("std_dividends").cast("double"),
                col("std_stock_splits").cast("double"),
                lit(company_name).alias("company_name"),
                lit(stock_code).alias("stock_code"),
                col("row_count").cast("int"),
                # Add month_number as an example for monthly-specific data, if needed
                # (You might need to parse `year_month` further for this, or derive from Date)
                # lit(month(col("Date"))).alias("month_number"), # Example, needs proper date in group by
                lit(datetime.now().isoformat()).alias("processed_timestamp")
            )

        monthly_df = monthly_df.coalesce(1)
        monthly_count = monthly_df.count()
        logger.info(f"✅ Monthly data processed for {company_name}: {monthly_count} records")

        # === 3. YEARLY AGGREGATION (agg_type = "year") ===
        logger.info(f"📅 Creating yearly aggregation for {company_name}")

        yearly_df = raw_df.withColumn("year", year(col("Date")).cast("string")) \
            .groupBy("year") \
            .agg(
                avg("Open").alias("avg_open"),
                avg("High").alias("avg_high"),
                avg("Low").alias("avg_low"),
                avg("Close").alias("avg_close"),
                avg("Volume").alias("avg_volume"),
                spark_sum("Dividends").alias("sum_dividends"),
                spark_sum("Stock_Splits").alias("sum_stock_splits"),
                spark_max("Open").alias("max_open"),
                spark_max("High").alias("max_high"),
                spark_max("Low").alias("max_low"),
                spark_max("Close").alias("max_close"),
                spark_max("Volume").alias("max_volume"),
                spark_max("Dividends").alias("max_dividends"),
                spark_max("Stock_Splits").alias("max_stock_splits"),
                spark_min("Open").alias("min_open"),
                spark_min("High").alias("min_high"),
                spark_min("Low").alias("min_low"),
                spark_min("Close").alias("min_close"),
                spark_min("Volume").alias("min_volume"),
                spark_min("Dividends").alias("min_dividends"),
                spark_min("Stock_Splits").alias("min_stock_splits"),
                stddev("Open").alias("std_open"),
                stddev("High").alias("std_high"),
                stddev("Low").alias("std_low"),
                stddev("Close").alias("std_close"),
                stddev("Volume").alias("std_volume"),
                stddev("Dividends").alias("std_dividends"),
                stddev("Stock_Splits").alias("std_stock_splits"),
                count("*").alias("row_count")
            ) \
            .select(
                col("year").alias("period_key"),
                lit("year").alias("agg_type"),
                # Create a date for January 1st of the year
                to_date(concat(col("year"), lit("-01-01")), "yyyy-MM-dd").alias("Date"),
                col("avg_open").cast("double"),
                col("avg_high").cast("double"),
                col("avg_low").cast("double"),
                col("avg_close").cast("double"),
                col("avg_volume").cast("long"),
                col("sum_dividends").cast("double"),
                col("sum_stock_splits").cast("double"),
                col("max_open").cast("double"),
                col("max_high").cast("double"),
                col("max_low").cast("double"),
                col("max_close").cast("double"),
                col("max_volume").cast("long"),
                col("max_dividends").cast("double"),
                col("max_stock_splits").cast("double"),
                col("min_open").cast("double"),
                col("min_high").cast("double"),
                col("min_low").cast("double"),
                col("min_close").cast("double"),
                col("min_volume").cast("long"),
                col("min_dividends").cast("double"),
                col("min_stock_splits").cast("double"),
                col("std_open").cast("double"),
                col("std_high").cast("double"),
                col("std_low").cast("double"),
                col("std_close").cast("double"),
                col("std_volume").cast("double"), # Volume stddev can be float
                col("std_dividends").cast("double"),
                col("std_stock_splits").cast("double"),
                lit(company_name).alias("company_name"),
                lit(stock_code).alias("stock_code"),
                col("row_count").cast("int"),
                # Add year_number as an example for yearly-specific data, if needed
                # lit(year(col("Date"))).alias("year_number"), # Example, needs proper date in group by
                lit(datetime.now().isoformat()).alias("processed_timestamp")
            )

        yearly_df = yearly_df.coalesce(1)
        yearly_count = yearly_df.count()
        logger.info(f"✅ Yearly data processed for {company_name}: {yearly_count} records")

        # === 4. WRITE TO MONGODB ===
        # Write each aggregation level separately
        logger.info(f"💾 Writing daily data to MongoDB for {company_name}")
        write_spark_to_mongo(daily_df, DB_TRANSFORM, f"{collection_name}_daily", mode="overwrite")

        logger.info(f"💾 Writing monthly data to MongoDB for {company_name}")
        write_spark_to_mongo(monthly_df, DB_TRANSFORM, f"{collection_name}_monthly", mode="overwrite")

        logger.info(f"💾 Writing yearly data to MongoDB for {company_name}")
        write_spark_to_mongo(yearly_df, DB_TRANSFORM, f"{collection_name}_yearly", mode="overwrite")

        # === 5. COMBINED COLLECTION ===
        logger.info(f"🔄 Creating combined collection for {company_name}")

        # Ensure all dataframes have the same schema before union
        # Define a common schema that includes all possible columns from daily, monthly, yearly
        # and set nullable to true for optional columns or use null if not present
        common_schema = StructType([
            StructField("period_key", StringType(), True),
            StructField("agg_type", StringType(), True),
            StructField("Date", DateType(), True),
            StructField("Open", DoubleType(), True),
            StructField("High", DoubleType(), True),
            StructField("Low", DoubleType(), True),
            StructField("Close", DoubleType(), True),
            StructField("Volume", LongType(), True),
            StructField("Dividends", DoubleType(), True),
            StructField("Stock_Splits", DoubleType(), True),
            StructField("company_name", StringType(), True),
            StructField("stock_code", StringType(), True),
            StructField("processed_timestamp", StringType(), True),
            StructField("avg_open", DoubleType(), True),
            StructField("avg_high", DoubleType(), True),
            StructField("avg_low", DoubleType(), True),
            StructField("avg_close", DoubleType(), True),
            StructField("avg_volume", DoubleType(), True),
            StructField("sum_dividends", DoubleType(), True),
            StructField("sum_stock_splits", DoubleType(), True),
            StructField("max_open", DoubleType(), True),
            StructField("max_high", DoubleType(), True),
            StructField("max_low", DoubleType(), True),
            StructField("max_close", DoubleType(), True),
            StructField("max_volume", LongType(), True),
            StructField("max_dividends", DoubleType(), True),
            StructField("max_stock_splits", DoubleType(), True),
            StructField("min_open", DoubleType(), True),
            StructField("min_high", DoubleType(), True),
            StructField("min_low", DoubleType(), True),
            StructField("min_close", DoubleType(), True),
            StructField("min_volume", LongType(), True),
            StructField("min_dividends", DoubleType(), True),
            StructField("min_stock_splits", DoubleType(), True),
            StructField("std_open", DoubleType(), True),
            StructField("std_high", DoubleType(), True),
            StructField("std_low", DoubleType(), True),
            StructField("std_close", DoubleType(), True),
            StructField("std_volume", DoubleType(), True),
            StructField("std_dividends", DoubleType(), True),
            StructField("std_stock_splits", DoubleType(), True),
            StructField("row_count", IntegerType(), True)
        ])

        # Cast and select all common columns for daily_df
        daily_df_selected = daily_df.select(
            col("period_key"),
            col("agg_type"),
            col("Date"),
            col("Open"),
            col("High"),
            col("Low"),
            col("Close"),
            col("Volume"),
            col("Dividends"),
            col("Stock_Splits"),
            col("company_name"),
            col("stock_code"),
            col("processed_timestamp"),
            lit(None).cast(DoubleType()).alias("avg_open"),
            lit(None).cast(DoubleType()).alias("avg_high"),
            lit(None).cast(DoubleType()).alias("avg_low"),
            lit(None).cast(DoubleType()).alias("avg_close"),
            lit(None).cast(DoubleType()).alias("avg_volume"),
            lit(None).cast(DoubleType()).alias("sum_dividends"),
            lit(None).cast(DoubleType()).alias("sum_stock_splits"),
            lit(None).cast(DoubleType()).alias("max_open"),
            lit(None).cast(DoubleType()).alias("max_high"),
            lit(None).cast(DoubleType()).alias("max_low"),
            lit(None).cast(DoubleType()).alias("max_close"),
            lit(None).cast(LongType()).alias("max_volume"),
            lit(None).cast(DoubleType()).alias("max_dividends"),
            lit(None).cast(DoubleType()).alias("max_stock_splits"),
            lit(None).cast(DoubleType()).alias("min_open"),
            lit(None).cast(DoubleType()).alias("min_high"),
            lit(None).cast(DoubleType()).alias("min_low"),
            lit(None).cast(DoubleType()).alias("min_close"),
            lit(None).cast(LongType()).alias("min_volume"),
            lit(None).cast(DoubleType()).alias("min_dividends"),
            lit(None).cast(DoubleType()).alias("min_stock_splits"),
            lit(None).cast(DoubleType()).alias("std_open"),
            lit(None).cast(DoubleType()).alias("std_high"),
            lit(None).cast(DoubleType()).alias("std_low"),
            lit(None).cast(DoubleType()).alias("std_close"),
            lit(None).cast(DoubleType()).alias("std_volume"),
            lit(None).cast(DoubleType()).alias("std_dividends"),
            lit(None).cast(DoubleType()).alias("std_stock_splits"),
            lit(1).cast(IntegerType()).alias("row_count") # daily will always have 1 row_count per entry
        ).repartition(200).persist().withColumnRenamed("Open", "daily_open") # Rename to avoid clash later

        # Cast and select all common columns for monthly_df
        monthly_df_selected = monthly_df.select(
            col("period_key"),
            col("agg_type"),
            col("Date"),
            lit(None).cast(DoubleType()).alias("daily_open"), # No daily Open
            lit(None).cast(DoubleType()).alias("High"), # No daily High
            lit(None).cast(DoubleType()).alias("Low"), # No daily Low
            lit(None).cast(DoubleType()).alias("Close"), # No daily Close
            lit(None).cast(LongType()).alias("Volume"), # No daily Volume
            lit(None).cast(DoubleType()).alias("Dividends"), # No daily Dividends
            lit(None).cast(DoubleType()).alias("Stock_Splits"), # No daily Stock_Splits
            col("company_name"),
            col("stock_code"),
            col("processed_timestamp"),
            col("avg_open"),
            col("avg_high"),
            col("avg_low"),
            col("avg_close"),
            col("avg_volume"),
            col("sum_dividends"),
            col("sum_stock_splits"),
            col("max_open"),
            col("max_high"),
            col("max_low"),
            col("max_close"),
            col("max_volume"),
            col("max_dividends"),
            col("max_stock_splits"),
            col("min_open"),
            col("min_high"),
            col("min_low"),
            col("min_close"),
            col("min_volume"),
            col("min_dividends"),
            col("min_stock_splits"),
            col("std_open"),
            col("std_high"),
            col("std_low"),
            col("std_close"),
            col("std_volume"),
            col("std_dividends"),
            col("std_stock_splits"),
            col("row_count")
        ).repartition(200).persist()


        # Cast and select all common columns for yearly_df
        yearly_df_selected = yearly_df.select(
            col("period_key"),
            col("agg_type"),
            col("Date"),
            lit(None).cast(DoubleType()).alias("daily_open"), # No daily Open
            lit(None).cast(DoubleType()).alias("High"), # No daily High
            lit(None).cast(DoubleType()).alias("Low"), # No daily Low
            lit(None).cast(DoubleType()).alias("Close"), # No daily Close
            lit(None).cast(LongType()).alias("Volume"), # No daily Volume
            lit(None).cast(DoubleType()).alias("Dividends"), # No daily Dividends
            lit(None).cast(DoubleType()).alias("Stock_Splits"), # No daily Stock_Splits
            col("company_name"),
            col("stock_code"),
            # lit(None).cast(StringType()).alias("processed_timestamp"), # Yearly might not have individual timestamps
            lit(datetime.now().isoformat()).alias("processed_timestamp"),
            col("avg_open"),
            col("avg_high"),
            col("avg_low"),
            col("avg_close"),
            col("avg_volume"),
            col("sum_dividends"),
            col("sum_stock_splits"),
            col("max_open"),
            col("max_high"),
            col("max_low"),
            col("max_close"),
            col("max_volume"),
            col("max_dividends"),
            col("max_stock_splits"),
            col("min_open"),
            col("min_high"),
            col("min_low"),
            col("min_close"),
            col("min_volume"),
            col("min_dividends"),
            col("min_stock_splits"),
            col("std_open"),
            col("std_high"),
            col("std_low"),
            col("std_close"),
            col("std_volume"),
            col("std_dividends"),
            col("std_stock_splits"),
            col("row_count")
        ).repartition(200).persist()

        # Rename 'Open' to 'daily_open' in daily_df_selected to avoid unionByName conflicts.
        # Then, align schemas.
        # A simpler way to handle schema for union is to make sure all DFs have all columns and fill missing with nulls.
        # This approach ensures all columns are present and data types are consistent.
        # We also need to be careful with column names like 'Open' vs 'avg_open' etc.
        # Let's adjust the select statements for monthly and yearly to also include 'Open', 'High', etc., as nulls.

        # For monthly and yearly, we don't have individual daily Open/High/Low/Close/Volume/Dividends/Stock_Splits.
        # So we should represent them as null when unioning.
        # The daily_df has these, so we need to rename them to avoid conflict with avg/min/max/std columns.

        # Let's rebuild the union part with a more robust schema alignment.
        # Create a function to align schema of a DataFrame to a target schema
        def align_schema(df, target_schema):
            for field in target_schema.fields:
                if field.name not in df.columns:
                    df = df.withColumn(field.name, lit(None).cast(field.dataType))
            return df.select([field.name for field in target_schema.fields])


        # Ensure all columns in daily_df_selected exist in the final combined schema
        daily_df_aligned = daily_df.select(
            col("period_key"),
            col("agg_type"),
            col("Date"),
            col("Open").alias("Open"), # Keep original names for daily
            col("High").alias("High"),
            col("Low").alias("Low"),
            col("Close").alias("Close"),
            col("Volume").alias("Volume"),
            col("Dividends").alias("Dividends"),
            col("Stock_Splits").alias("Stock_Splits"),
            col("company_name"),
            col("stock_code"),
            col("processed_timestamp"),
            lit(None).cast(DoubleType()).alias("avg_open"),
            lit(None).cast(DoubleType()).alias("avg_high"),
            lit(None).cast(DoubleType()).alias("avg_low"),
            lit(None).cast(DoubleType()).alias("avg_close"),
            lit(None).cast(DoubleType()).alias("avg_volume"),
            lit(None).cast(DoubleType()).alias("sum_dividends"),
            lit(None).cast(DoubleType()).alias("sum_stock_splits"),
            lit(None).cast(DoubleType()).alias("max_open"),
            lit(None).cast(DoubleType()).alias("max_high"),
            lit(None).cast(DoubleType()).alias("max_low"),
            lit(None).cast(DoubleType()).alias("max_close"),
            lit(None).cast(LongType()).alias("max_volume"),
            lit(None).cast(DoubleType()).alias("max_dividends"),
            lit(None).cast(DoubleType()).alias("max_stock_splits"),
            lit(None).cast(DoubleType()).alias("min_open"),
            lit(None).cast(DoubleType()).alias("min_high"),
            lit(None).cast(DoubleType()).alias("min_low"),
            lit(None).cast(DoubleType()).alias("min_close"),
            lit(None).cast(LongType()).alias("min_volume"),
            lit(None).cast(DoubleType()).alias("min_dividends"),
            lit(None).cast(DoubleType()).alias("min_stock_splits"),
            lit(None).cast(DoubleType()).alias("std_open"),
            lit(None).cast(DoubleType()).alias("std_high"),
            lit(None).cast(DoubleType()).alias("std_low"),
            lit(None).cast(DoubleType()).alias("std_close"),
            lit(None).cast(DoubleType()).alias("std_volume"),
            lit(None).cast(DoubleType()).alias("std_dividends"),
            lit(None).cast(DoubleType()).alias("std_stock_splits"),
            lit(1).cast(IntegerType()).alias("row_count")
        )

        monthly_df_aligned = monthly_df.select(
            col("period_key"),
            col("agg_type"),
            col("Date"),
            lit(None).cast(DoubleType()).alias("Open"), # Daily values are null for monthly/yearly
            lit(None).cast(DoubleType()).alias("High"),
            lit(None).cast(DoubleType()).alias("Low"),
            lit(None).cast(DoubleType()).alias("Close"),
            lit(None).cast(LongType()).alias("Volume"),
            lit(None).cast(DoubleType()).alias("Dividends"),
            lit(None).cast(DoubleType()).alias("Stock_Splits"),
            col("company_name"),
            col("stock_code"),
            col("processed_timestamp"),
            col("avg_open"),
            col("avg_high"),
            col("avg_low"),
            col("avg_close"),
            col("avg_volume"),
            col("sum_dividends"),
            col("sum_stock_splits"),
            col("max_open"),
            col("max_high"),
            col("max_low"),
            col("max_close"),
            col("max_volume"),
            col("max_dividends"),
            col("max_stock_splits"),
            col("min_open"),
            col("min_high"),
            col("min_low"),
            col("min_close"),
            col("min_volume"),
            col("min_dividends"),
            col("min_stock_splits"),
            col("std_open"),
            col("std_high"),
            col("std_low"),
            col("std_close"),
            col("std_volume"),
            col("std_dividends"),
            col("std_stock_splits"),
            col("row_count")
        )

        yearly_df_aligned = yearly_df.select(
            col("period_key"),
            col("agg_type"),
            col("Date"),
            lit(None).cast(DoubleType()).alias("Open"), # Daily values are null for monthly/yearly
            lit(None).cast(DoubleType()).alias("High"),
            lit(None).cast(DoubleType()).alias("Low"),
            lit(None).cast(DoubleType()).alias("Close"),
            lit(None).cast(LongType()).alias("Volume"),
            lit(None).cast(DoubleType()).alias("Dividends"),
            lit(None).cast(DoubleType()).alias("Stock_Splits"),
            col("company_name"),
            col("stock_code"),
            col("processed_timestamp"),
            col("avg_open"),
            col("avg_high"),
            col("avg_low"),
            col("avg_close"),
            col("avg_volume"),
            col("sum_dividends"),
            col("sum_stock_splits"),
            col("max_open"),
            col("max_high"),
            col("max_low"),
            col("max_close"),
            col("max_volume"),
            col("max_dividends"),
            col("max_stock_splits"),
            col("min_open"),
            col("min_high"),
            col("min_low"),
            col("min_close"),
            col("min_volume"),
            col("min_dividends"),
            col("min_stock_splits"),
            col("std_open"),
            col("std_high"),
            col("std_low"),
            col("std_close"),
            col("std_volume"),
            col("std_dividends"),
            col("std_stock_splits"),
            col("row_count")
        )

        # Union all three dataframes
        final_df = daily_df_aligned.unionByName(monthly_df_aligned).unionByName(yearly_df_aligned)
        final_df = final_df.coalesce(4)

        # Write combined data
        write_spark_to_mongo(final_df, DB_TRANSFORM, collection_name, mode="overwrite")

        total_docs_transformed = daily_count + monthly_count + yearly_count
        logger.info(f"✅ Spark transformation completed for {company_name}:")
        logger.info(f"    - Daily: {daily_count} records")
        logger.info(f"    - Monthly: {monthly_count} records")
        logger.info(f"    - Yearly: {yearly_count} records")
        logger.info(f"    - Total: {total_docs_transformed} documents")

        # Clean up cache
        raw_df.unpersist()
        logger.info(f"🧹 Unpersisted cached dataframe for {company_name}")

        return total_docs_transformed

    except Exception as e:
        logger.error(f"❌ Error during Spark transformation for {company_name}: {e}")
        import traceback
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        return 0

def transform_data_spark(**kwargs):
    """
    Transforms stock data using Spark - processes one stock at a time.
    """
    logger.info("🔄 Starting Spark data transformation process")
    log_memory_usage("spark_transform_start")

    # Initialize Spark session
    spark = get_spark_session("YFinance_Transform")

    try:
        # 1. Connect to MongoDB to get collection list
        logger.info("📊 Membuat koneksi ke MongoDB Yfinance_Extract")
        client = get_mongo_client()
        db_extract = client[DB_EXTRACT]

        # 2. Read CSV file to get stock list
        daftar_saham_path = '/opt/airflow/data/Daftar_Saham.csv'
        if not os.path.exists(daftar_saham_path):
            logger.error(f"File {daftar_saham_path} tidak ditemukan!")
            raise FileNotFoundError(f"File {daftar_saham_path} tidak ditemukan!")

        logger.info("📋 Membaca daftar saham dari CSV")
        data_csv = pd.read_csv(daftar_saham_path)

        # 3. Process first 5 stocks
        MAX_SAHAM = 951
        logger.info(f"🎯 Memproses {MAX_SAHAM} saham pertama dari CSV")

        transformed_stocks_list = []
        total_transformed_docs = 0

        # 4. Transform per collection
        for idx, row in data_csv.iterrows():
            if idx >= MAX_SAHAM:
                logger.info(f"Membatasi transformasi hanya pada {MAX_SAHAM} saham pertama.")
                break

            kode_saham = row['Kode'] + '.JK'
            nama_perusahaan = row['Nama Perusahaan']
            collection_name = nama_perusahaan.replace(" ", "_")


            logger.info(f"🔄 Memproses transformasi Spark untuk {nama_perusahaan} ({kode_saham})")

            # Check if collection exists in DB Extract
            if collection_name not in db_extract.list_collection_names():
                logger.warning(f"⚠️ Collection {collection_name} tidak ditemukan di DB Extract. Melewati...")
                continue

            # Create stock_info object
            stock_info = {
                'company_name': nama_perusahaan,
                'collection_name': collection_name,
                'stock_code': kode_saham,
                'csv_index': idx
            }

            try:
                count = transform_single_stock_spark(spark, stock_info)
                if count > 0:
                    transformed_stocks_list.append(stock_info)
                    total_transformed_docs += count
                    logger.info(f"✅ Transformasi Spark {nama_perusahaan} selesai: {count} documents")

                # Log memory usage periodically
                if len(transformed_stocks_list) % 2 == 0:
                    log_memory_usage(f"spark_transform_progress_{len(transformed_stocks_list)}")

            except Exception as e:
                logger.error(f"❌ Failed Spark transformation for {nama_perusahaan}: {e}")
                continue

        # Save transformed stocks list
        transformed_stocks_file = f"{TEMP_DIR}/transformed_stocks.pkl"
        with open(transformed_stocks_file, "wb") as f:
            pickle.dump(transformed_stocks_list, f)
        logger.info(f"🗳️ List of transformed stocks saved to: {transformed_stocks_file}")

        logger.info(f"🎉 All Spark transformations completed: {len(transformed_stocks_list)} stocks, {total_transformed_docs} total documents")
        log_memory_usage("spark_transform_end")

        # Close connections
        client.close()
        spark.stop()

    except Exception as e:
        logger.error(f"🚨 Error di transform_data_spark: {e}")
        if 'spark' in locals():
            spark.stop()
        raise

def load_single_stock_spark(spark, stock_info):
    """
    Loads data for a single stock from DB transform to DB load using Spark.
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
    """
    Loads transformed data to final MongoDB using Spark.
    """
    logger.info("⬆️ Starting Spark load to final MongoDB")
    log_memory_usage("spark_load_start")

    # Initialize Spark session
    spark = get_spark_session("YFinance_Load")

    try:
        transformed_stocks_file = f"{TEMP_DIR}/transformed_stocks.pkl"

        if not os.path.exists(transformed_stocks_file):
            logger.error(f"File {transformed_stocks_file} not found! Transformation might have failed.")
            raise FileNotFoundError(f"File {transformed_stocks_file} not found!")

        with open(transformed_stocks_file, "rb") as f:
            transformed_stocks = pickle.load(f)

        total_stocks_loaded = 0
        total_docs_loaded = 0

        for stock_info in transformed_stocks:
            try:
                count = load_single_stock_spark(spark, stock_info)
                if count > 0:
                    total_stocks_loaded += 1
                    total_docs_loaded += count
            except Exception as e:
                logger.error(f"❌ Failed Spark load for {stock_info['company_name']}: {e}")

        logger.info(f"📦 Spark load total: {total_stocks_loaded} stocks loaded, {total_docs_loaded} documents")
        log_memory_usage("spark_load_end")

        # Clean up temporary files after load is complete
        processed_stocks_file = f"{TEMP_DIR}/processed_stocks.pkl"
        try:
            if os.path.exists(processed_stocks_file):
                os.remove(processed_stocks_file)
                logger.info(f"🧹 Temporary file {processed_stocks_file} cleaned up")
            if os.path.exists(transformed_stocks_file):
                os.remove(transformed_stocks_file)
                logger.info(f"🧹 Temporary file {transformed_stocks_file} cleaned up")
        except Exception as e:
            logger.warning(f"⚠️ Failed to clean up temporary files: {e}")

    except Exception as e:
        logger.error(f"🚨 Error in load_data_to_mongodb_spark: {e}")
        raise
    finally:
        # Always stop Spark session
        if 'spark' in locals():
            spark.stop()


# --- DAG Definition ---
default_args_transform_load = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='etl_yfinance_spark_transform_load_dag',
    description='DAG untuk Stock Data Transformation dan Load dengan Spark dan Agregasi',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args_transform_load,
    max_active_runs=1,
    tags=['saham', 'yfinance', 'mongodb', 'spark', 'transform', 'load', 'agregasi'],
) as dag_transform_load:

    start_transform_load = DummyOperator(task_id='start_spark_transform_load')

    # Sensor untuk menunggu DAG extract selesai - OPSIONAL
    # wait_for_extract = ExternalTaskSensor(
    #     task_id='wait_for_extract_dag',
    #     external_dag_id='etl_yfinance_extract_dag',
    #     external_task_id='extract_yfinance_data',
    #     timeout=7200,
    #     poke_interval=60,
    #     mode='poke'
    # )

    spark_transform_task = PythonOperator(
        task_id='spark_transform_extracted_data',
        python_callable=transform_data_spark,
        execution_timeout=timedelta(minutes=180),  # Increased timeout for Spark
    )

    spark_load_task = PythonOperator(
        task_id='spark_load_transformed_data_to_final_mongo',
        python_callable=load_data_to_mongodb_spark,
        execution_timeout=timedelta(minutes=90),  # Increased timeout for Spark
    )

    end_transform_load = DummyOperator(task_id='end_spark_transform_load')

    start_transform_load >> spark_transform_task >> spark_load_task >> end_transform_load