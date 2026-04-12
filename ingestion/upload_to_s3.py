import boto3
import os
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

BUCKET      = os.getenv("S3_BUCKET_NAME", "iptv-latdat-bucket")
LOCAL_DIR   = Path("dataset")
TARGET_FILE = "20220404.json"        


def parse_partition(filename: str) -> dict:
    """20220404.json → {year:'2022', month:'04', day:'04'}"""
    stem = Path(filename).stem
    return {
        "year":  stem[0:4],
        "month": stem[4:6],
        "day":   stem[6:8],
    }


def build_s3_key(filename: str) -> str:
        #p = parse_partition(filename)
        #return f"bronze/year={p['year']}/month={p['month']}/day={p['day']}/{filename}"
        return f"landing/{filename}"


def upload_file(local_path: Path, s3_key: str):
    s3 = boto3.client("s3")
    file_size = local_path.stat().st_size

    config = TransferConfig(
        multipart_threshold = 50 * 1024 * 1024, 
        multipart_chunksize = 50 * 1024 * 1024,
        max_concurrency     = 10,
    )

    print(f"\nUploading : {local_path.name}")
    print(f"  → s3://{BUCKET}/{s3_key}")
    print(f"  Size     : {file_size / 1024**3:.2f} GB")
    print(f"  Chunks   : ~{file_size // (50 * 1024 * 1024) + 1} parts x 50MB")

    try:
        s3.upload_file(
            Filename  = str(local_path),
            Bucket    = BUCKET,
            Key       = s3_key,
            Config    = config,
            ExtraArgs = {"ContentType": "application/json"},
            Callback  = ProgressCallback(file_size),
        )
        print(f"\n  Upload hoan thanh!")
        print(f"  S3 URI : s3://{BUCKET}/{s3_key}")

    except boto3.exceptions.S3UploadFailedError as e:
        print(f"\n  FAIL: Upload that bai — {e}")
        raise
    except Exception as e:
        print(f"\n  FAIL: Loi khong xac dinh — {e}")
        raise


class ProgressCallback:
    def __init__(self, total):
        self._total    = total
        self._uploaded = 0

    def __call__(self, bytes_amount):
        self._uploaded += bytes_amount
        pct = self._uploaded / self._total * 100
        bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
        print(f"\r  [{bar}] {pct:.1f}%", end="", flush=True)


if __name__ == "__main__":
    local_path = LOCAL_DIR / TARGET_FILE

    if not local_path.exists():
        print(f"ERROR: Khong tim thay {local_path}")
        exit(1)

    s3_key = build_s3_key(TARGET_FILE)
    upload_file(local_path, s3_key)