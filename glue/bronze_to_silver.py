import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, lit, upper, regexp_replace

# ==================== PARAMETERS ====================
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "INPUT_PATH",         # Do Airflow truyền
    "OUTPUT_PATH",        # Do Airflow truyền
    "batch_date",         # Airflow truyền (VD: '20220401' hoặc '2022-04-01')
    "fraud_mac_threshold"
])

INPUT_PATH  = args["INPUT_PATH"]
OUTPUT_PATH = args["OUTPUT_PATH"]
batch_date  = args["batch_date"]
FRAUD_MAC_THRESHOLD = int(args["fraud_mac_threshold"])

file_date = batch_date.replace("-", "")
year, month, day = file_date[0:4], file_date[4:6], file_date[6:8]

# ==================== SPARK SESSION ====================
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Input  : {INPUT_PATH}")
print(f"Output : {OUTPUT_PATH}")
print(f"Batch Date: {batch_date}")
print(f"Fraud MAC threshold: {FRAUD_MAC_THRESHOLD}")

# ==================== READ BRONZE ====================
df_raw = spark.read.json(INPUT_PATH)

# ==================== TRANSFORMATIONS ====================

# 1. Select, rename, cast
df = df_raw.select(
    F.col("_id").alias("event_id"),
    F.col("_source.Contract").alias("contract_id"),
    F.col("_source.Mac").alias("device_mac_raw"),
    F.col("_source.TotalDuration").cast("integer").alias("total_duration_seconds"),
    F.col("_source.AppName").alias("app_name"),
    F.lit(batch_date).alias("batch_date")
)

# 2. Add event_date
df = df.withColumn("event_date", to_date(lit(file_date), "yyyyMMdd"))

# 3. Drop null / empty on critical columns
df = df.filter(F.col("event_id").isNotNull()) \
       .filter(F.col("contract_id").isNotNull() & (F.trim(F.col("contract_id")) != "")) \
       .filter(F.col("device_mac_raw").isNotNull() & (F.trim(F.col("device_mac_raw")) != ""))

# 4. Filter valid total_duration_seconds (0 < x <= 86400)
df = df.filter((F.col("total_duration_seconds") > 0) & (F.col("total_duration_seconds") <= 86400))

# 5. Remove test app (BHD)
df = df.filter(F.col("app_name") != "BHD")

# 6. Clean contract_id: keep only valid format
df = df.filter(F.col("contract_id").rlike(r"^[A-Z]{2,5}\d+$"))

# 7. Validate and NORMALIZE MAC address (standardization)
MAC_PATTERN = r"^[0-9A-Fa-f]{12}$|^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$"
df = df.filter(F.col("device_mac_raw").rlike(MAC_PATTERN))

# Normalize MAC: remove colons, uppercase
df = df.withColumn(
    "device_mac",
    upper(regexp_replace(F.col("device_mac_raw"), ":", ""))
).drop("device_mac_raw")

# 8. Deduplicate AFTER standardization (important!)
df = df.dropDuplicates(["event_id"])

# 9. Flag fraud using threshold from parameters
mac_per_contract = df.groupBy("contract_id") \
                     .agg(F.countDistinct("device_mac").alias("mac_count"))

df = df.join(mac_per_contract, on="contract_id", how="left") \
       .withColumn("device_flag",
           F.when(F.col("mac_count") == 1,                    "normal")
            .when(F.col("mac_count") <= FRAUD_MAC_THRESHOLD,  "multi_device")
            .otherwise(                                       "fraud_suspect")
       ).drop("mac_count")

df.cache()
record_count = df.count()

# ==================== QUALITY REPORT ====================
fraud_suspect = df.filter(F.col("device_flag") == "fraud_suspect").count()
multi_device  = df.filter(F.col("device_flag") == "multi_device").count()
normal        = df.filter(F.col("device_flag") == "normal").count()

print("="*50)
print("SILVER TRANSFORMATION SUMMARY")
print("="*50)
print(f"Fraud threshold used: {FRAUD_MAC_THRESHOLD}")
print(f"Records after all cleans : {record_count:,}")
print(f"  - normal                : {normal:,}")
print(f"  - multi_device          : {multi_device:,}")
print(f"  - fraud_suspect         : {fraud_suspect:,}")
print(f"Event date range          : {df.agg(F.min('event_date').alias('min'), F.max('event_date').alias('max')).collect()[0]}")
print("="*50)

# ==================== WRITE SILVER ====================
df.write.mode("overwrite").parquet(OUTPUT_PATH)
df.unpersist()

print(f"Successfully written to {OUTPUT_PATH}")
job.commit()