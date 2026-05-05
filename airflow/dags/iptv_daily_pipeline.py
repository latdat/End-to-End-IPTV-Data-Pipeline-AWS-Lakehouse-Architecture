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

# Fail-fast: raise ngay khi import nếu thiếu biến môi trường bắt buộc
BUCKET           = os.environ["S3_BUCKET"]
GLUE_JOB_NAME    = "bronze_to_silver_etl"
REDSHIFT_WG      = os.environ["REDSHIFT_WORKGROUP"]
REDSHIFT_DB      = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_ROLE    = os.environ["REDSHIFT_IAM_ROLE"]
AWS_REGION       = os.environ.get("AWS_DEFAULT_REGION", "ap-southeast-1")
DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/iptv_dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/iptv_dbt")
DBT_BIN          = os.environ.get("DBT_BIN", "/home/airflow/.local/bin/dbt")


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _s3():
    return boto3.client("s3", region_name=AWS_REGION)

def _glue():
    return boto3.client("glue", region_name=AWS_REGION)

def _redshift():
    return boto3.client("redshift-data", region_name=AWS_REGION)

def _parse_date_parts(batch_date: str):
    """YYYYMMDD → (year, month, day)"""
    return batch_date[:4], batch_date[4:6], batch_date[6:8]

def _to_sql_date(batch_date: str) -> str:
    """'20220401' → '2022-04-01'"""
    return f"{batch_date[:4]}-{batch_date[4:6]}-{batch_date[6:8]}"


# ──────────────────────────────────────────────
# Task functions
# ──────────────────────────────────────────────

def check_and_prepare(**context):
    """
    Kiểm tra file data ở landing/ và bronze/.
    - Nếu có landing → move (copy + delete) sang bronze.
    - Nếu không có landing nhưng bronze đã tồn tại → tiếp tục.
    - Nếu cả hai đều không có → skip toàn bộ pipeline.
    Push batch_date lên XCom để các task sau sử dụng.
    """
    ti = context["ti"]
    batch_date = context["ds_nodash"]   # YYYYMMDD, luôn có giá trị khi Airflow inject
    year, month, day = _parse_date_parts(batch_date)

    landing_key = f"landing/{batch_date}.json"
    bronze_key  = f"bronze/year={year}/month={month}/day={day}/{batch_date}.json"

    s3 = _s3()

    try:
        s3.head_object(Bucket=BUCKET, Key=landing_key)

        # Có file landing → copy sang bronze
        s3.copy_object(
            Bucket=BUCKET,
            CopySource={"Bucket": BUCKET, "Key": landing_key},
            Key=bronze_key,
        )

        # Xóa landing — nếu fail thì dừng pipeline để đảm bảo idempotency,
        # tránh lần chạy sau ghi đè bronze bằng data cũ.
        try:
            s3.delete_object(Bucket=BUCKET, Key=landing_key)
        except Exception as e:
            logger.error(f"Xóa landing file thất bại: {e}")
            raise RuntimeError(
                f"Không thể xóa landing file sau khi copy sang bronze. "
                f"Dừng pipeline để bảo toàn idempotency. Lỗi: {e}"
            )

        logger.info(f"Moved {landing_key} → {bronze_key}")

    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

        # Không có landing → kiểm tra bronze
        try:
            s3.head_object(Bucket=BUCKET, Key=bronze_key)
            logger.info(f"Bronze đã có sẵn: {bronze_key}, tiếp tục xử lý.")
        except s3.exceptions.ClientError as e2:
            if e2.response["Error"]["Code"] == "404":
                raise AirflowSkipException(
                    f"Không có data cho {batch_date} ở cả landing/ lẫn bronze/"
                )
            raise

    ti.xcom_push(key="batch_date", value=batch_date)


def trigger_glue_job(**context):
    """
    Lấy batch_date từ XCom, tạo đường dẫn INPUT/OUTPUT S3,
    trigger Glue job và push run_id lên XCom.
    """
    ti = context["ti"]
    batch_date = ti.xcom_pull(task_ids="check_and_prepare", key="batch_date")
    if not batch_date:
        raise AirflowSkipException("No batch_date found")

    year, month, day = _parse_date_parts(batch_date)
    input_path  = f"s3://{BUCKET}/bronze/year={year}/month={month}/day={day}/{batch_date}.json"
    output_path = f"s3://{BUCKET}/silver/year={year}/month={month}/day={day}/"

    glue = _glue()
    resp = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--batch_date":           batch_date,
            "--INPUT_PATH":           input_path,
            "--OUTPUT_PATH":          output_path,
            "--fraud_mac_threshold":  "10",
        },
    )
    run_id = resp["JobRunId"]
    logger.info(f"Glue job triggered | batch_date={batch_date} | run_id={run_id}")
    ti.xcom_push(key="glue_run_id", value=run_id)


def wait_glue_complete(**context):
    """
    Poll Glue job mỗi 30 giây đến khi SUCCEEDED.
    Fail nếu job ở trạng thái lỗi.
    Timeout được kiểm soát bởi execution_timeout của operator (1 giờ).
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="trigger_glue_job", key="glue_run_id")
    if not run_id:
        raise AirflowSkipException("No glue_run_id")

    glue = _glue()
    terminal_ok   = {"SUCCEEDED"}
    terminal_fail = {"FAILED", "ERROR", "TIMEOUT", "STOPPED"}

    while True:
        resp  = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
        state = resp["JobRun"]["JobRunState"]
        logger.info(f"Glue state: {state}")

        if state in terminal_ok:
            logger.info("Glue job succeeded.")
            return
        if state in terminal_fail:
            error = resp["JobRun"].get("ErrorMessage", "")
            raise RuntimeError(f"Glue job failed: {error}")

        time.sleep(30)


def copy_to_redshift(**context):
    """
    COPY dữ liệu Parquet từ Silver S3 vào staging.viewing_history.
    DELETE partition hiện tại trước để đảm bảo idempotency.
    batch_execute_statement tự wrap trong implicit transaction — không cần BEGIN/COMMIT thủ công.
    """
    ti = context["ti"]
    batch_date = ti.xcom_pull(task_ids="check_and_prepare", key="batch_date")
    if not batch_date:
        raise AirflowSkipException("No batch_date")

    year, month, day = _parse_date_parts(batch_date)
    silver_path   = f"s3://{BUCKET}/silver/year={year}/month={month}/day={day}/"
    sql_date      = _to_sql_date(batch_date)  # 'YYYY-MM-DD' cho cột kiểu DATE

    rd = _redshift()
    resp = rd.batch_execute_statement(
        WorkgroupName=REDSHIFT_WG,
        Database=REDSHIFT_DB,
        # batch_execute_statement đảm bảo atomicity qua implicit transaction.
        # Không thêm BEGIN/COMMIT tường minh vì sẽ conflict với transaction của API.
        Sqls=[
            f"DELETE FROM staging.viewing_history WHERE batch_date = '{sql_date}';",
            f"COPY staging.viewing_history FROM '{silver_path}' IAM_ROLE '{REDSHIFT_ROLE}' FORMAT AS PARQUET;",
        ],
    )
    stmt_id = resp["Id"]
    logger.info(f"Redshift batch statement submitted | id={stmt_id}")

    while True:
        desc   = rd.describe_statement(Id=stmt_id)
        status = desc["Status"]
        if status == "FINISHED":
            logger.info("Redshift COPY completed.")
            return
        if status in ("FAILED", "ABORTED"):
            raise RuntimeError(f"Redshift statement failed: {desc.get('Error')}")
        time.sleep(10)


# ──────────────────────────────────────────────
# DAG definition
# ──────────────────────────────────────────────

default_args = {
    "owner":          "phucvinh",
    "retries":        1,
    "retry_delay":    timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="iptv_pipeline",
    default_args=default_args,
    description="IPTV Pipeline: landing → bronze → Glue → Redshift → dbt",
    schedule_interval='@daily', 
    start_date=datetime(2022, 4, 1),
    catchup=False,
    # Trường hợp 1 (Backfill dữ liệu quá khứ): Giữ schedule_interval='@daily' và catchup=False
    # docker exec -it airflow-airflow-scheduler-1 airflow dags backfill -s 2022-04-01 -e 2022-04-03 iptv_pipeline
    
    # Trường hợp 2 (Chạy tự động theo lịch): Thiết lập schedule_interval='0 2 * * *' (chạy lúc 2h sáng mỗi ngày)
    # docker exec -it airflow-airflow-scheduler-1 airflow dags trigger iptv_pipeline -e 2022-04-01  # Trigger thủ công cho ngày cụ thể

    # Trường hợp 3 (Trigger thủ công/Qua API): Dùng khi muốn chạy ngay lập tức, bỏ qua lịch trình.
    # docker exec -it airflow-airflow-scheduler-1 airflow dags trigger iptv_pipeline
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
    )

    wait_glue = PythonOperator(
        task_id="wait_glue_complete",
        python_callable=wait_glue_complete,
        retries=0,                            # Glue tự retry bên trong; không retry lại từ đầu
        execution_timeout=timedelta(hours=1), # Tránh worker bị block vô thời hạn
    )

    copy_redshift = PythonOperator(
        task_id="copy_to_redshift",
        python_callable=copy_to_redshift,
        execution_timeout=timedelta(hours=1),
    )

    # append_env=True: giữ lại toàn bộ env hiện tại (PATH, HOME, ...) và chỉ thêm/override
    # DBT_BATCH_DATE. Không dùng append_env thì dbt sẽ thiếu PATH và fail.
    _dbt_env = {
        "DBT_BATCH_DATE": "{{ ti.xcom_pull(task_ids='check_and_prepare', key='batch_date') }}"
    }
    _dbt_vars = """--vars '{"batch_date": "{{ ti.xcom_pull(task_ids='check_and_prepare', key='batch_date') }}"}'"""

    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} run --profiles-dir {DBT_PROFILES_DIR} {_dbt_vars}"
        ),
        env=_dbt_env,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="test_dbt",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} test --profiles-dir {DBT_PROFILES_DIR} {_dbt_vars}"
        ),
        env=_dbt_env,
        append_env=True,
    )

    check_data >> trigger_glue >> wait_glue >> copy_redshift >> dbt_run >> dbt_test