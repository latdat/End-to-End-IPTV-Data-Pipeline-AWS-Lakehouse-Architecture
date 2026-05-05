import os
import time
import logging
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

BUCKET           = os.environ["S3_BUCKET"]
GLUE_JOB_NAME    = "bronze_to_silver_etl"
REDSHIFT_WG      = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_DB      = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_ROLE    = os.environ["REDSHIFT_IAM_ROLE"]
AWS_REGION       = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/iptv_dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/iptv_dbt")
DBT_BIN          = os.environ.get("DBT_BIN", "/home/airflow/.local/bin/dbt")

def _s3():
    return boto3.client("s3", region_name=AWS_REGION)

def _glue():
    return boto3.client("glue", region_name=AWS_REGION)

def _redshift():
    return boto3.client("redshift-data", region_name=AWS_REGION)

def _parse_date_parts(batch_date: str):
    """Nhận vào YYYYMMDD, trả về (year, month, day)"""
    return batch_date[:4], batch_date[4:6], batch_date[6:8]

def _to_sql_date(batch_date: str) -> str:
    """'20220401' → '2022-04-01'"""
    return f"{batch_date[:4]}-{batch_date[4:6]}-{batch_date[6:8]}"

# Nếu có file landing, di chuyển (copy & delete) sang thư mục bronze.
def check_and_prepare(**context):
    ti = context["ti"]
    
    # Bước 1: Airflow tự động lấy ngày
    batch_date = context["ds_nodash"]  
    
    if not batch_date:
        raise AirflowSkipException("Batch date is missing")
    
    year, month, day = _parse_date_parts(batch_date)
    landing_key = f"landing/{batch_date}.json"
    bronze_key  = f"bronze/year={year}/month={month}/day={day}/{batch_date}.json"
    
    s3 = _s3()

    # Bước 2: Check landing trước
    try:
        s3.head_object(Bucket=BUCKET, Key=landing_key)
        # Có file landing → copy đè vào bronze
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": landing_key},
            Key=bronze_key,
        )
        try:
            s3.delete_object(Bucket=BUCKET, Key=landing_key)
        except Exception as e:
            logger.error(f"Lỗi khi xóa landing file: {e}")
            raise RuntimeError(f"Xóa landing file thất bại, dừng pipeline để đảm bảo idempotency. Lỗi: {e}")
            
        logger.info(f"Moved {landing_key} → {bronze_key}")

    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise
        
        # Không có landing → check bronze
        try:
            s3.head_object(Bucket=BUCKET, Key=bronze_key)
            logger.info(f"Bronze đã có sẵn: {bronze_key}, tiếp tục xử lý.")
        except s3.exceptions.ClientError as e2:
            if e2.response["Error"]["Code"] == "404":
                raise AirflowSkipException(f"Không có data cho {batch_date} ở cả landing/ lẫn bronze/")
            raise

    # Bước 3: Push thông lưu `batch_date` lên XCom để các task tiếp theo sử dụng.
    ti.xcom_push(key="batch_date", value=batch_date)

def trigger_glue_job(**context):
    """
    Lấy `batch_date` từ XCom, thiết lập các tham số INPUT/OUTPUT S3 và truyền vào Glue.
    Push `run_id` của quá trình chạy Glue lên XCom.
    """
    ti = context["ti"]
    batch_date = ti.xcom_pull(task_ids="check_and_prepare", key="batch_date")
    if not batch_date:
        raise AirflowSkipException("No batch_date found")
    
    year, month, day = _parse_date_parts(batch_date)
    input_path = f"s3://{BUCKET}/bronze/year={year}/month={month}/day={day}/{batch_date}.json"
    output_path = f"s3://{BUCKET}/silver/year={year}/month={month}/day={day}/"
    
    glue = _glue()
    resp = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--batch_date": batch_date,
            "--INPUT_PATH": input_path,
            "--OUTPUT_PATH": output_path,
            "--fraud_mac_threshold": "10",
        },
    )
    run_id = resp["JobRunId"]
    logger.info(f"Glue job triggered | batch_date={batch_date} | run_id={run_id}")
    ti.xcom_push(key="glue_run_id", value=run_id)

# Chờ định kỳ đến khi Job thành công (SUCCEEDED). Nếu thất bại hoặc timeout sẽ raise Error.
def wait_glue_complete(**context):
    ti = context["ti"]
    run_id = ti.xcom_pull(key="glue_run_id", task_ids="trigger_glue_job")
    if not run_id:
        raise AirflowSkipException("No glue_run_id")
    
    glue = _glue()
    terminal_ok = {"SUCCEEDED"}
    terminal_fail = {"FAILED", "ERROR", "TIMEOUT", "STOPPED"}
    
    while True:
        resp = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
        state = resp["JobRun"]["JobRunState"]
        logger.info(f"Glue state: {state}")
        if state in terminal_ok:
            logger.info("Glue succeeded")
            return
        if state in terminal_fail:
            error = resp["JobRun"].get("ErrorMessage", "")
            raise RuntimeError(f"Glue failed: {error}")
        time.sleep(30)

def copy_to_redshift(**context):
    ti = context["ti"]
    batch_date = ti.xcom_pull(task_ids="check_and_prepare", key="batch_date")
    if not batch_date:
        raise AirflowSkipException("No batch_date")
    
    year, month, day = _parse_date_parts(batch_date)
    silver_path = f"s3://{BUCKET}/silver/year={year}/month={month}/day={day}/"
    
    # Xóa partition cũ của ngày hiện tại (đảm bảo tính Idempotent) và COPY dữ liệu Parquet từ Silver S3 vào bảng staging.
    rd = _redshift()
    resp = rd.batch_execute_statement(
        WorkgroupName=REDSHIFT_WG,
        Database=REDSHIFT_DB,
    Sqls=[
            "BEGIN;",
            f"DELETE FROM staging.viewing_history WHERE batch_date = '{_to_sql_date(batch_date)}';",
            f"COPY staging.viewing_history FROM '{silver_path}' IAM_ROLE '{REDSHIFT_ROLE}' FORMAT AS PARQUET;",
            "COMMIT;"
        ],
    )
    stmt_id = resp["Id"]
    logger.info(f"Redshift statement {stmt_id} submitted")
    while True:
        desc = rd.describe_statement(Id=stmt_id)
        status = desc["Status"]
        if status == "FINISHED":
            logger.info("COPY completed")
            return
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift failed: {desc.get('Error')}")
        time.sleep(10)

default_args = {
    "owner": "phucvinh",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="iptv_pipeline",
    default_args=default_args,
    description="IPTV Pipeline: landing → bronze → Glue → Redshift → dbt",
    schedule_interval=None, # Chạy thủ công hoặc qua API, không chạy theo lịch định sẵn
    start_date=datetime(2022, 4, 1),
    catchup=False, # docker exec -it airflow-airflow-scheduler-1 airflow dags backfill -s 2022-04-01 -e 2022-04-03 iptv_pipeline 
    max_active_runs=1,
    tags=["iptv", "etl"],
) as dag:
    
    check_data = PythonOperator(
        task_id="check_and_prepare",
        python_callable=check_and_prepare,
    )

    trigger_glue = PythonOperator(
        task_id="trigger_glue_job",
        python_callable=trigger_glue_job,
        trigger_rule="all_success", 
    )

    wait_glue = PythonOperator(
        task_id="wait_glue_complete",
        python_callable=wait_glue_complete,
        retries=0,
        execution_timeout=timedelta(hours=1),
        trigger_rule="all_success",
    )

    copy_redshift = PythonOperator(
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        execution_timeout=timedelta(hours=1),
        trigger_rule="all_success",
    )

    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} "
            '--vars \'{"batch_date": "\'$DBT_BATCH_DATE\'"}\''
        ),
        env={
            "DBT_BATCH_DATE": "{{ ti.xcom_pull(task_ids='check_and_prepare', key='batch_date') }}"
        },
    )

    dbt_test = BashOperator(
        task_id="test_dbt",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} "
            '--vars \'{"batch_date": "\'$DBT_BATCH_DATE\'"}\''
        ),
        env={
            "DBT_BATCH_DATE": "{{ ti.xcom_pull(task_ids='check_and_prepare', key='batch_date') }}"
        },
    )

    check_data >> trigger_glue >> wait_glue >> copy_redshift >> dbt_run >> dbt_test