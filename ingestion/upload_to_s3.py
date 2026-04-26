import boto3
import os
import argparse
from boto3.s3.transfer import TransferConfig
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta

load_dotenv()

BUCKET    = os.getenv("S3_BUCKET_NAME", "iptv-latdat-bucket")
LOCAL_DIR = Path(__file__).parent.parent / "dataset"
#__file__ là path của file script đang chạy (ingestion/upload_to_s3.py), .parent.parent lên 2 cấp về IPTV_DE/, rồi trỏ vào dataset/


def parse_partition(filename: str) -> dict:
    """20220404.json → {year:'2022', month:'04', day:'04'}"""
    stem = Path(filename).stem
    return {
        "year":  stem[0:4],
        "month": stem[4:6],
        "day":   stem[6:8],
    }


def build_s3_key(filename: str) -> str:
    return f"landing/{filename}"


def get_files_in_range(local_dir: Path, start_date: datetime, end_date: datetime) -> list[Path]:
    """Lấy danh sách file .json trong khoảng [start_date, end_date]."""
    all_files = sorted(local_dir.glob("*.json"))
    result = []

    for f in all_files:
        try:
            file_date = datetime.strptime(f.stem, "%Y%m%d")
            if start_date <= file_date <= end_date:
                result.append(f)
        except ValueError:
            # Bỏ qua file không đúng định dạng ngày
            continue

    return result


def get_last_n_days_files(local_dir: Path, n: int) -> list[Path]:
    """Lấy n file mới nhất (theo tên ngày) trong thư mục."""
    all_files = sorted(local_dir.glob("*.json"))
    dated_files = []

    for f in all_files:
        try:
            datetime.strptime(f.stem, "%Y%m%d")
            dated_files.append(f)
        except ValueError:
            continue

    return dated_files[-n:] if n <= len(dated_files) else dated_files


class ProgressCallback:
    def __init__(self, total):
        self._total    = total
        self._uploaded = 0

    def __call__(self, bytes_amount):
        self._uploaded += bytes_amount
        pct = self._uploaded / self._total * 100
        bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
        print(f"\r  [{bar}] {pct:.1f}%", end="", flush=True)


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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Upload file JSON từ dataset lên S3",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Ví dụ sử dụng:
  # Upload 1 file cụ thể
  python ingestion/upload_to_s3.py --file 20220404.json

  # Upload 10 ngày cuối cùng trong thư mục
  python ingestion/upload_to_s3.py --last 10

  # Upload theo khoảng ngày
  python ingestion/upload_to_s3.py --from 20220405 --to 20220415

  # Upload từ ngày 20 đến hôm nay
  python ingestion/upload_to_s3.py --from 20220420

  # Xem trước file sẽ upload (không upload thật)
  python ingestion/upload_to_s3.py --last 10 --dry-run
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--file", type=str,
        metavar="FILENAME",
        help="Upload 1 file cụ thể (vd: 20220404.json)"
    )
    group.add_argument(
        "--last", type=int,
        metavar="N",
        help="Upload N ngày cuối cùng trong thư mục"
    )
    group.add_argument(
        "--from", dest="date_from", type=str,
        metavar="YYYYMMDD",
        help="Upload từ ngày (dùng kèm --to)"
    )

    parser.add_argument(
        "--to", type=str,
        metavar="YYYYMMDD",
        help="Upload đến ngày (dùng kèm --from, mặc định = hôm nay)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Chỉ liệt kê file, không upload thật"
    )

    return parser.parse_args()


def resolve_files(args) -> list[Path]:
    """Xác định danh sách file cần upload dựa trên tham số."""

    # --- Chế độ 1: file đơn ---
    if args.file:
        path = LOCAL_DIR / args.file
        if not path.exists():
            print(f"ERROR: Không tìm thấy {path}")
            exit(1)
        return [path]

    # --- Chế độ 2: N ngày cuối ---
    if args.last:
        files = get_last_n_days_files(LOCAL_DIR, args.last)
        if not files:
            print(f"ERROR: Không tìm thấy file nào trong {LOCAL_DIR}")
            exit(1)
        return files

    # --- Chế độ 3: khoảng ngày ---
    if args.date_from:
        try:
            start = datetime.strptime(args.date_from, "%Y%m%d")
        except ValueError:
            print(f"ERROR: --from không đúng định dạng YYYYMMDD")
            exit(1)

        end = datetime.today()
        if args.to:
            try:
                end = datetime.strptime(args.to, "%Y%m%d")
            except ValueError:
                print(f"ERROR: --to không đúng định dạng YYYYMMDD")
                exit(1)

        if start > end:
            print(f"ERROR: --from ({args.date_from}) phải nhỏ hơn --to")
            exit(1)

        files = get_files_in_range(LOCAL_DIR, start, end)
        if not files:
            print(f"ERROR: Không có file nào trong khoảng {args.date_from} → {args.to or 'hôm nay'}")
            exit(1)
        return files

    return []


if __name__ == "__main__":
    args = parse_args()
    files = resolve_files(args)

    # Hiển thị danh sách sẽ upload
    print(f"\n{'[DRY-RUN] ' if args.dry_run else ''}Danh sách file sẽ upload ({len(files)} file):")
    for f in files:
        print(f"  {f.name}  →  s3://{BUCKET}/{build_s3_key(f.name)}")

    if args.dry_run:
        print("\n[DRY-RUN] Không có file nào được upload thật.")
        exit(0)

    # Xác nhận trước khi upload nhiều file
    if len(files) > 1:
        confirm = input(f"\nBắt đầu upload {len(files)} file? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Đã hủy.")
            exit(0)

    # Upload từng file
    success, failed = 0, []
    for i, local_path in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}]", end="")
        try:
            upload_file(local_path, build_s3_key(local_path.name))
            success += 1
        except Exception:
            failed.append(local_path.name)

    # Tổng kết
    print(f"\n{'='*40}")
    print(f"Tổng kết : {success}/{len(files)} file upload thành công")
    if failed:
        print(f"Thất bại  : {', '.join(failed)}")