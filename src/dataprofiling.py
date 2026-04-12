import os
import re
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_extract, trim, lower, length, countDistinct, when, isnull

os.environ['JAVA_HOME']   = r'C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH']        = os.environ['HADOOP_HOME'] + r'\bin;' + os.environ['PATH']

spark = SparkSession.builder \
    .appName("JSON_DataProfiling_PySpark") \
    .master("local[32]") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.default.parallelism", "64") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
start_time = time.time()

DATASET_FOLDER = r"D:\ProjectDE\IPTV_DE\dataset"

def load_all_files(folder_path: str) -> DataFrame:
    print("\n1. LOAD DU LIEU")
    df = spark.read.json(f"{folder_path}/*.json")
    df = df.withColumn("file_path", F.input_file_name()) \
           .withColumn("date_raw", regexp_extract(col("file_path"), r"(\d{8})", 1)) \
           .withColumn("date", F.to_date(col("date_raw"), "yyyyMMdd")) \
           .drop("file_path", "date_raw") 
    return df


def flatten_df(df: DataFrame) -> DataFrame:
    return df.select(
        col("_index"),
        col("_type"),
        col("_id"),
        col("_source.Contract").alias("Contract"),
        col("_source.Mac").alias("Mac"),
        col("_source.TotalDuration").alias("TotalDuration"),
        col("_source.AppName").alias("AppName"),
        col("date")
    )


def summary_report(df: DataFrame):
    print("\n2. TONG QUAN DU LIEU")
    total = df.count()
    print(f"  Tong so dong : {total:,}")
    print(f"  Tong so cot  : {len(df.columns)}")
    df.printSchema()
    print(f"  {'Cot':<20} {'Null':>10} {'Null%':>8} {'Distinct':>10}")
    print("  " + "-"*50)
    for c in df.columns:
        null_count     = df.filter(isnull(col(c)) | (trim(col(c).cast("string")) == "")).count()
        distinct_count = df.select(countDistinct(col(c))).collect()[0][0]
        pct = null_count / total * 100 if total > 0 else 0
        print(f"  {c:<20} {null_count:>10,} {pct:>7.2f}% {distinct_count:>10,}")


def records_per_day(df: DataFrame):
    print("\n--- So record theo ngay ---")
    df.groupBy("date").count().orderBy("date").show(35, truncate=False)


def check_constant_fields(df: DataFrame):
    print("\n3. CONSTANT FIELDS (_index, _score)")
    for field in ["_index", "_score"]:
        if field not in df.columns:
            print(f"  [WARN] '{field}' khong ton tai")
            continue
        vals = [r[0] for r in df.select(field).distinct().collect()]
        if len(vals) == 1:
            print(f"  [OK] '{field}' = {vals[0]} -> CO THE DROP")
        else:
            print(f"  [WARN] '{field}' co {len(vals)} gia tri: {vals} -> KHONG drop")


def check_id_format(df: DataFrame):
    print("\n4. DINH DANG _id")
    df.withColumn("id_length", length(col("_id"))) \
      .select(F.min("id_length").alias("min_len"),
              F.max("id_length").alias("max_len"),
              F.countDistinct("id_length").alias("distinct_len_count")) \
      .show()
    invalid_id = df.filter(~col("_id").rlike(r"^[A-Za-z0-9_\-]+$")).count()
    if invalid_id > 0:
        print(f"  [WARN] {invalid_id:,} _id chua ky tu khong hop le")
    else:
        print("  [OK] Tat ca _id hop le")


def check_id_duplicates(df: DataFrame):
    print("\n--- _id duplicate ---")
    dup_count = df.count() - df.select("_id").distinct().count()
    if dup_count > 0:
        print(f"  [WARN] {dup_count:,} _id bi duplicate")
        df.groupBy("_id").count().filter(col("count") > 1).orderBy(col("count").desc()).show(10)
    else:
        print("  [OK] Khong co _id duplicate")


def drop_missing_rows(df: DataFrame) -> DataFrame:
    print("\n5. MISSING VALUES")
    before   = df.count()
    key_cols = ["_id", "Contract", "Mac", "TotalDuration", "AppName"]
    df_clean = df
    for c in key_cols:
        df_clean = df_clean.filter(col(c).isNotNull() & (trim(col(c).cast("string")) != ""))
    after = df_clean.count()
    print(f"  Truoc: {before:,}  |  Sau: {after:,}  |  Loai bo: {before - after:,}")
    return df_clean


def trim_and_check_encoding(df: DataFrame) -> DataFrame:
    print("\n6. ENCODING & TRIMMING")
    for c in ["Contract", "Mac", "AppName", "_index", "_type"]:
        if c in df.columns:
            df = df.withColumn(c, trim(col(c)))
    for c in ["Contract", "Mac", "AppName"]:
        if c in df.columns:
            n = df.filter(col(c).rlike("[^\x00-\x7F]")).count()
            tag = "[WARN]" if n > 0 else "[OK]"
            print(f"  {tag} '{c}' non-ASCII: {n:,}")
    return df


def validate_contract_format(df: DataFrame):
    print("\n7. CONTRACT -- Dinh dang")
    total   = df.count()
    invalid = df.filter(~col("Contract").rlike(r"^[A-Z]{2,4}\d+$")).count()
    print(f"  Hop le: {total - invalid:,} / {total:,}")
    if invalid > 0:
        df.filter(~col("Contract").rlike(r"^[A-Z]{2,6}\d+$")).select("Contract").distinct().show(10)


def analyze_contract_prefix(df: DataFrame):
    print("\n--- Prefix ma vung Contract ---")
    for n in [2, 3, 4, 5, 6]:
        print(f"  Top prefix {n} ky tu:")
        df.withColumn("prefix", regexp_extract(col("Contract"), r"^([A-Z]{" + str(n) + r"})", 1)) \
          .groupBy("prefix").count().orderBy(col("count").desc()).show(10)


def contract_diversity_ratio(df: DataFrame):
    print("\n--- Do da dang Contract ---")
    total  = df.count()
    unique = df.select("Contract").distinct().count()
    ratio  = unique / total if total > 0 else 0
    print(f"  Unique / Total = {unique:,} / {total:,} = {ratio:.4f}")
    print("  -> Gan nhu khoa duy nhat" if ratio > 0.95 else "  -> Xuat hien nhieu lan")


def check_contract_mac_relation(df: DataFrame):
    print("\n--- Quan he Contract <-> Mac ---")
    agg = df.groupBy("Contract").agg(countDistinct("Mac").alias("mac_count"))
    agg.select(F.min("mac_count").alias("min"),
               F.max("mac_count").alias("max"),
               F.avg("mac_count").alias("avg")).show()
    agg.orderBy(col("mac_count").desc()).show(5)
    print(f"  Contract co > 1 Mac: {agg.filter(col('mac_count') > 1).count():,}")


def validate_mac_format(df: DataFrame):
    print("\n8. MAC ADDRESS -- Dinh dang")
    p1 = r"^[0-9A-Fa-f]{12}$"
    p2 = r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$"
    total   = df.count()
    valid   = df.filter(col("Mac").rlike(p1) | col("Mac").rlike(p2)).count()
    print(f"  Hop le: {valid:,} / {total:,}")
    if total - valid > 0:
        df.filter(~col("Mac").rlike(p1) & ~col("Mac").rlike(p2)).select("Mac").distinct().show(10)


def check_mac_contract_relation(df: DataFrame):
    print("\n--- Quan he Mac <-> Contract ---")
    agg = df.groupBy("Mac").agg(countDistinct("Contract").alias("contract_count"))
    agg.select(F.min("contract_count").alias("min"),
               F.max("contract_count").alias("max"),
               F.avg("contract_count").alias("avg")).show()
    print(f"  Mac co > 1 Contract: {agg.filter(col('contract_count') > 1).count():,}")


def validate_duration_type(df: DataFrame):
    print("\n9. TOTAL DURATION -- Kieu du lieu")
    dtype = dict(df.dtypes).get("TotalDuration", "unknown")
    print(f"  Kieu: {dtype}")
    if dtype not in ("int", "bigint", "long"):
        n = df.filter(~col("TotalDuration").cast("string").rlike(r"^\d+$")).count()
        print(f"  [WARN] Khong phai integer | Gia tri loi: {n:,}")
    else:
        print("  [OK] Integer")


def validate_duration_range(df: DataFrame):
    print("\n--- Range TotalDuration ---")
    total   = df.count()
    invalid = df.filter((col("TotalDuration") <= 0) | (col("TotalDuration") > 86400)).count()
    print(f"  Hop le (0 < x <= 86400): {total - invalid:,} / {total:,}")
    if invalid > 0:
        df.filter((col("TotalDuration") <= 0) | (col("TotalDuration") > 86400)) \
          .select("TotalDuration").describe().show()


def duration_distribution(df: DataFrame):
    print("\n--- Phan phoi TotalDuration ---")
    df.select("TotalDuration").describe().show()
    q = df.stat.approxQuantile("TotalDuration", [0.25, 0.5, 0.75, 0.95], 0.01)
    for label, val in zip(["25%", "50%", "75%", "95%"], q):
        print(f"  P{label}: {val:>10,.0f}s  (~{val/60:.1f} phut)")
    print("\n--- TotalDuration trung binh theo ngay ---")
    df.groupBy("date").agg(F.avg("TotalDuration").alias("avg_duration"),
                           F.count("*").alias("record_count")) \
      .orderBy("date").show(35, truncate=False)


def check_type_appname_consistency(df: DataFrame):
    print("\n10. _type vs AppName")
    df.groupBy("_type", "AppName").count().orderBy("_type").show()
    df_check = df.withColumn("type_lower", lower(col("_type"))) \
                 .withColumn("app_lower",  lower(col("AppName")))
    mismatch = df_check.filter(col("type_lower") != col("app_lower")).count()
    if mismatch == 0:
        print("  [OK] Tuong dong 1-1 -> CO THE DROP 1 cot")
    else:
        print(f"  [WARN] {mismatch:,} dong khong khop -> GIU lai ca 2")


def enumerate_appname_values(df: DataFrame):
    print("\n--- AppName (normalized) ---")
    df.withColumn("AppName_normalized", F.upper(trim(col("AppName")))) \
      .groupBy("AppName_normalized").count().orderBy(col("count").desc()).show()


if __name__ == "__main__":
    df_raw  = load_all_files(DATASET_FOLDER)
    df_flat = flatten_df(df_raw)

    summary_report(df_flat)
    records_per_day(df_flat)

    check_constant_fields(df_flat)
    check_id_format(df_flat)
    check_id_duplicates(df_flat)

    df_clean = drop_missing_rows(df_flat)
    df_clean = trim_and_check_encoding(df_clean)
    df_clean.cache()
    df_clean.count()
    print("\n  [OK] df_clean cached")

    validate_contract_format(df_clean)
    analyze_contract_prefix(df_clean)
    contract_diversity_ratio(df_clean)
    check_contract_mac_relation(df_clean)

    validate_mac_format(df_clean)
    check_mac_contract_relation(df_clean)

    validate_duration_type(df_clean)
    validate_duration_range(df_clean)
    duration_distribution(df_clean)

    check_type_appname_consistency(df_clean)
    enumerate_appname_values(df_clean)

    print(f"\nTong thoi gian: {time.time() - start_time:.4f}s")
    spark.stop()