import os
import re
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, regexp_extract, trim, lower, length, countDistinct, when, isnull
from dataprofiling import load_all_files, flatten_df, summary_report, records_per_day, check_constant_fields, check_id_format, check_id_duplicates, drop_missing_rows, trim_and_check_encoding, validate_contract_format, analyze_contract_prefix, contract_diversity_ratio, check_contract_mac_relation, validate_mac_format, check_mac_contract_relation, validate_duration_type, validate_duration_range, duration_distribution, check_type_appname_consistency, enumerate_appname_values, analyze_mac_prefix

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

    analyze_mac_prefix(df_clean)

    print(f"\nTong thoi gian: {time.time() - start_time:.4f}s")
    spark.stop()