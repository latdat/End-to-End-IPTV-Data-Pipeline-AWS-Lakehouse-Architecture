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

# 1. Select, rename, cast, lưu ý ép đúng kiểu với trên redshift tạo sau này.
df = df_raw.select(
    F.col("_id").alias("event_id"),
    F.col("_source.Contract").alias("contract_id"),
    F.col("_source.Mac").alias("device_mac_raw"),
    F.col("_source.TotalDuration").cast("long").alias("total_duration_seconds"),
    F.col("_source.AppName").alias("app_name"),
    F.lit(batch_date).alias("batch_date")
)

# 2. Add event_date
df = df.withColumn("event_date", to_date(lit(file_date), "yyyyMMdd"))

# 3. Track fraud reasons instead of dropping
MAC_PATTERN = r"^[0-9A-Fa-f]{12}$|^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$"

df = df.withColumn(
    "fraud_reasons",
    F.concat_ws(", ",
        F.when(F.col("event_id").isNull(), "Missing event_id"),
        F.when(F.col("contract_id").isNull() | (F.trim(F.col("contract_id")) == ""), "Missing contract_id"),
        F.when(F.col("device_mac_raw").isNull() | (F.trim(F.col("device_mac_raw")) == ""), "Missing device_mac"),
        F.when((F.col("total_duration_seconds").isNull()) | (F.col("total_duration_seconds") <= 0) | (F.col("total_duration_seconds") > 86400), "Invalid total_duration_seconds"),
        F.when(F.col("app_name") == "BHD", "Test app BHD"),
        F.when(F.col("contract_id").isNotNull() & (F.trim(F.col("contract_id")) != "") & (~F.col("contract_id").rlike(r"^[A-Z]{2,5}\d+$")), "Invalid contract_id format"),
        F.when(F.col("device_mac_raw").isNotNull() & (F.trim(F.col("device_mac_raw")) != "") & (~F.col("device_mac_raw").rlike(MAC_PATTERN)), "Invalid MAC format")
    )
)

# 4. Normalize MAC Address for ALL records (if regex matches, otherwise keep original)
df = df.withColumn(
    "device_mac",
    F.when(F.col("device_mac_raw").rlike(MAC_PATTERN), upper(regexp_replace(F.col("device_mac_raw"), ":", "")))
     .otherwise(F.col("device_mac_raw"))
).drop("device_mac_raw")

# 5. Deduplicate
df = df.dropDuplicates(["event_id"])

# 6. Flag fraud based on threshold using valid contract_ids only
mac_per_contract = df.filter(F.col("contract_id").isNotNull() & (F.trim(F.col("contract_id")) != "") & F.col("contract_id").rlike(r"^[A-Z]{2,5}\d+$")) \
                     .groupBy("contract_id") \
                     .agg(F.countDistinct("device_mac").alias("mac_count"))

# left join on = "contrac_id" sẽ bị đưa lên đầu làm cột số 1 dẫn đến event_id bị đẩy qua cột thứ hai => dữ liệu bị đảo chỗ
df = df.join(mac_per_contract, on="contract_id", how="left") \
       .withColumn("device_flag",
           F.when(F.col("mac_count") == 1,                    "normal")
            .when(F.col("mac_count") <= FRAUD_MAC_THRESHOLD,  "multi_device")
            .otherwise(                                       "fraud_suspect")
       ).drop("mac_count")

# Update fraud_reasons if fraud_suspect
df = df.withColumn(
    "fraud_reasons",
    F.when(
        F.col("device_flag") == "fraud_suspect",
        F.when(F.col("fraud_reasons") != "", F.concat_ws(", ", F.col("fraud_reasons"), F.lit("Too many devices")))
         .otherwise("Too many devices")
    ).otherwise(F.col("fraud_reasons"))
)

# 7. Create boolean is_fraudulent
df = df.withColumn("is_fraudulent", F.col("fraud_reasons") != "")

df.cache()
record_count = df.count()

# ==================== QUALITY REPORT ====================
fraud_suspect = df.filter(F.col("device_flag") == "fraud_suspect").count()
multi_device  = df.filter(F.col("device_flag") == "multi_device").count()
normal        = df.filter((F.col("device_flag") == "normal") | F.col("device_flag").isNull()).count()

fraudulent_count = df.filter(F.col("is_fraudulent") == True).count()
valid_count = df.filter(F.col("is_fraudulent") == False).count()

print("="*50)
print("SILVER TRANSFORMATION SUMMARY")
print("="*50)
print(f"Fraud threshold used: {FRAUD_MAC_THRESHOLD}")
print(f"Total Records             : {record_count:,}")
print(f"  - Valid records         : {valid_count:,}")
print(f"  - Fraudulent/Invalid    : {fraudulent_count:,}")
print(f"    (includes {fraud_suspect:,} fraud_suspect, {multi_device:,} multi_device, {normal:,} normal)")
print(f"Event date range          : {df.agg(F.min('event_date').alias('min'), F.max('event_date').alias('max')).collect()[0]}")
print("="*50)

# ==================== WRITE SILVER ====================
final_columns = [
    "event_id",
    "contract_id",
    "total_duration_seconds",
    "app_name",
    "batch_date", #string - Được dùng làm Partition Key (khóa phân vùng) khi lưu file Parquet xuống S3 (year=2022/month=04/day=01
    "event_date", #date - Ngày thực tế diễn ra sự kiện xem
    "fraud_reasons",
    "device_mac",
    "device_flag",
    "is_fraudulent"
]
df = df.select(*final_columns)

df.write.mode("overwrite").parquet(OUTPUT_PATH)
df.unpersist()

print(f"Successfully written to {OUTPUT_PATH}")
job.commit()