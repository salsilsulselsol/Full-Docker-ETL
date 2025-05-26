import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, expr, coalesce, udf, lower
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, MapType
from pymongo import MongoClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

# MongoDB Configuration - Service name 'mongo_db' will be used from docker-compose
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo_db:27017/") # Updated service name
SOURCE_DATABASE_NAME = "idx_financial_data_staging"
OUTPUT_DATABASE_NAME = "idx_financial_data_production"
SOURCE_COLLECTION_PREFIX = "reports_"
OUTPUT_COLLECTION_PREFIX = "processed_reports_"

def create_spark_session():
    try:
        spark_builder = (SparkSession.builder
                  .appName("IDXFinancialDataTransformation")
                  .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") 
                  .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
                  .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
                  .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")) 
                  .config("spark.default.parallelism", os.environ.get("SPARK_DEFAULT_PARALLELISM", "8")) 
                  .config("spark.mongodb.input.uri", MONGO_URI)
                  .config("spark.mongodb.output.uri", MONGO_URI)
                  .master(os.environ.get("SPARK_MASTER_URL", "local[*]")) 
                 )
        
        spark = spark_builder.getOrCreate()
        logger.info(f"Spark session created successfully. Spark version: {spark.version}")
        return spark
    
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}", exc_info=True)
        raise

def safe_get_key_case_insensitive(data_map, key_name_target):
    if data_map is None or key_name_target is None:
        return None
    key_name_target_lower = key_name_target.lower()
    for key_map, value_map in data_map.items():
        if key_map.lower() == key_name_target_lower:
            if value_map == "" or value_map is None: 
                return None
            return str(value_map) 
    return None

safe_get_key_udf = udf(safe_get_key_case_insensitive, StringType())

def process_financial_data(df_raw, source_collection_name_str):
    try:
        logger.info(f"Starting data processing for collection: {source_collection_name_str}")
        logger.info("Schema of raw DataFrame:")
        df_raw.printSchema()
        
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

        df_processed = df_raw.select(
            col("company_code"),
            col("year"),
            col("period"),
            safe_get_key_udf(col("data"), lit(financial_tags_map["company_name"])).alias("company_name"),
            safe_get_key_udf(col("data"), lit(financial_tags_map["sector"])).alias("sector"),
            safe_get_key_udf(col("data"), lit(financial_tags_map["subsector"])).alias("subsector"),
            
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["revenue"])).cast(DoubleType()), lit(0.0)).alias("revenue"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["cost_of_revenue"])).cast(DoubleType()), lit(0.0)).alias("cost_of_revenue"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["gross_profit"])).cast(DoubleType()), lit(0.0)).alias("gross_profit"),
            
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["operating_expenses"])).cast(DoubleType()), lit(0.0)).alias("operating_expenses"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["profit_loss_from_operating_activities"])).cast(DoubleType()), lit(0.0)).alias("operating_profit_loss"),
            
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["finance_income"])).cast(DoubleType()), lit(0.0)).alias("finance_income"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["finance_costs"])).cast(DoubleType()), lit(0.0)).alias("finance_costs"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["profit_loss_before_tax"])).cast(DoubleType()), lit(0.0)).alias("profit_loss_before_tax"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["income_tax_expense_benefit"])).cast(DoubleType()), lit(0.0)).alias("income_tax_expense"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["profit_loss"])).cast(DoubleType()), lit(0.0)).alias("net_profit_loss"),

            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["total_assets"])).cast(DoubleType()), lit(0.0)).alias("total_assets"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["total_liabilities"])).cast(DoubleType()), lit(0.0)).alias("total_liabilities"),
            coalesce(safe_get_key_udf(col("data"), lit(financial_tags_map["total_equity"])).cast(DoubleType()), lit(0.0)).alias("total_equity"),

            col("retrieved_at").cast(TimestampType())
        )

        df_processed = df_processed.withColumn(
            "gross_profit_margin_pct",
            when(col("revenue") != 0, (col("gross_profit") / col("revenue")) * 100).otherwise(None)
        ).withColumn(
            "operating_profit_margin_pct", 
            when(col("revenue") != 0, (col("operating_profit_loss") / col("revenue")) * 100).otherwise(None)
        ).withColumn(
            "net_profit_margin_pct",
            when(col("revenue") != 0, (col("net_profit_loss") / col("revenue")) * 100).otherwise(None)
        ).withColumn(
            "debt_to_equity_ratio",
            when(col("total_equity") != 0, col("total_liabilities") / col("total_equity")).otherwise(None)
        ).withColumn(
            "current_ratio", 
            lit(None).cast(DoubleType()) 
        )
        
        logger.info(f"Data processed successfully for {source_collection_name_str}.")
        logger.info(f"Schema of processed DataFrame for {source_collection_name_str}:")
        df_processed.printSchema()
        logger.info(f"Sample of processed data for {source_collection_name_str} (first 5 rows):")
        df_processed.show(5, truncate=False)
        return df_processed
    
    except Exception as e:
        logger.error(f"Error in data processing for {source_collection_name_str}: {str(e)}", exc_info=True)
        raise 

def list_source_collections_from_db(mongo_uri_str, db_name_str, prefix_str):
    try:
        client = MongoClient(mongo_uri_str, serverSelectionTimeoutMS=10000)
        db = client[db_name_str]
        collection_names = [name for name in db.list_collection_names() if name.startswith(prefix_str)]
        client.close()
        logger.info(f"Found source collections with prefix '{prefix_str}' in database '{db_name_str}': {collection_names}")
        return collection_names
    except Exception as e:
        logger.error(f"Error listing source collections from database '{db_name_str}': {e}", exc_info=True)
        return [] 

def process_single_mongodb_collection(spark_session, source_collection_full_name):
    try:
        source_read_options = {
            "uri": f"{MONGO_URI}/{SOURCE_DATABASE_NAME}.{source_collection_full_name}",
        }
        
        if source_collection_full_name.startswith(SOURCE_COLLECTION_PREFIX):
            output_collection_full_name = OUTPUT_COLLECTION_PREFIX + source_collection_full_name[len(SOURCE_COLLECTION_PREFIX):]
        else: 
            output_collection_full_name = f"processed_{source_collection_full_name}"

        output_write_options = {
            "uri": f"{MONGO_URI}/{OUTPUT_DATABASE_NAME}.{output_collection_full_name}",
            "replaceDocument": "true" 
        }
        
        logger.info(f"Starting processing for source collection: '{SOURCE_DATABASE_NAME}.{source_collection_full_name}'")
        
        df_raw_data = spark_session.read.format("mongodb") \
            .options(**source_read_options) \
            .load()
        
        if df_raw_data.rdd.isEmpty():
            logger.info(f"Source collection '{source_collection_full_name}' is empty or does not exist. Skipping.")
            return

        record_count = df_raw_data.count()
        logger.info(f"Read {record_count} records from '{source_collection_full_name}'.")
        if record_count > 0:
            logger.info("Sample of raw data (first 3 rows):")
            df_raw_data.show(3, truncate=False)
        
        df_processed_data = process_financial_data(df_raw_data, source_collection_full_name)
        
        if df_processed_data.rdd.isEmpty():
            logger.warning(f"No data after processing for '{source_collection_full_name}'. Skipping write to output.")
            return

        processed_record_count = df_processed_data.count()
        logger.info(f"Writing {processed_record_count} processed records to output collection: '{OUTPUT_DATABASE_NAME}.{output_collection_full_name}'")
        
        (df_processed_data.write.format("mongodb")
            .options(**output_write_options)
            .mode("overwrite") 
            .save())
        
        logger.info(f"✅ Successfully processed '{source_collection_full_name}' and saved to '{output_collection_full_name}'")
    
    except Exception as e:
        logger.error(f"❌ Error processing collection '{source_collection_full_name}': {str(e)}", exc_info=True)

def main():
    spark_session = None
    try:
        spark_session = create_spark_session()
        
        source_collections_list = list_source_collections_from_db(MONGO_URI, SOURCE_DATABASE_NAME, prefix_str=SOURCE_COLLECTION_PREFIX)
        
        if not source_collections_list:
            logger.warning(f"No source collections found in database '{SOURCE_DATABASE_NAME}' with prefix '{SOURCE_COLLECTION_PREFIX}'. Exiting.")
            return

        logger.info(f"Found {len(source_collections_list)} collections to process: {source_collections_list}")
        
        for collection_to_process in source_collections_list:
            try:
                process_single_mongodb_collection(spark_session, collection_to_process)
            except Exception as e_collection_process:
                logger.error(f"A failure occurred while processing collection '{collection_to_process}'. Error: {e_collection_process}", exc_info=True) 
        
        logger.info("🏁 All identified financial data collections have been processed.")
    
    except Exception as e_main_process:
        logger.error(f"A fatal error occurred in the main transformation process: {str(e_main_process)}", exc_info=True)
    
    finally:
        if spark_session:
            spark_session.stop()
            logger.info("Spark session has been closed.")

if __name__ == "__main__":
    logger.info("🚀 Starting IDX Financial Data Transformation Script...")
    main()