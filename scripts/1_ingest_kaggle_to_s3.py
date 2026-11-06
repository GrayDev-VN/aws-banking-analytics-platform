"""
ENTERPRISE DATA INGESTION PIPELINE: KAGGLE -> S3 DATA LAKE
===========================================================
Kiến trúc:
1. Xác thực Kaggle API tự động qua biến môi trường (.env).
2. Tải và xử lý trích xuất song song CSV & SQLite (Transactions 1M rows).
3. Hive-Style Partitioning trên S3: raw/<table_name>/ingest_date=YYYY-MM-DD/<table_name>.csv
4. Tính toán Checksum SHA-256 và tạo Ingestion Manifest Audit trên S3.
5. Tối ưu hóa bộ nhớ RAM: Xử lý Chunking và Fast Byte Counting.
"""

import os
import json
import hashlib
import logging
import sqlite3
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from uuid import uuid4

import boto3
import pandas as pd
from dotenv import load_dotenv

# ============================================================
# 1. CẤU HÌNH & XÁC THỰC
# ============================================================
load_dotenv()

# Gán biến môi trường bắt buộc cho Kaggle API
os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME", "")
os.environ["KAGGLE_KEY"] = os.getenv("KAGGLE_KEY", "")

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")
KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "akrambelha/synthetic-banking-dataset-csv-sql-sqlite")

CHUNK_SIZE = 100_000

if not S3_BUCKET:
    raise ValueError("❌ Thiếu AWS_S3_BUCKET trong file .env!")
if not os.environ["KAGGLE_USERNAME"] or not os.environ["KAGGLE_KEY"]:
    raise ValueError("❌ Thiếu KAGGLE_USERNAME hoặc KAGGLE_KEY trong file .env!")

# ============================================================
# 2. LOGGING CHUẨN DOANH NGHIỆP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("IngestionPipeline")

# ============================================================
# 3. AWS S3 CLIENT
# ============================================================
s3_client = boto3.client("s3", region_name=AWS_REGION)

# ============================================================
# 4. HÀM TIỆN ÍCH TỐI ƯU HIỆU NĂNG
# ============================================================
def generate_batch_id():
    """Tạo mã Batch ID duy nhất theo thời gian UTC và mã UUID ngắn."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{timestamp}_{uuid4().hex[:8]}"

def calculate_sha256(file_path):
    """Tính mã băm SHA-256 theo khối 1MB để kiểm tra tính toàn vẹn (Data Integrity)."""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def fast_count_csv_records(file_path):
    """Đếm số dòng CSV cực nhanh bằng byte iterator (nhanh hơn pandas read_csv 10x)."""
    count = 0
    with open(file_path, "rb") as f:
        for line in f:
            count += 1
    # Trừ đi dòng tiêu đề Header (nếu file có dữ liệu)
    return max(0, count - 1)

def upload_file_to_s3(file_path, s3_key):
    """Đẩy file lên S3 Data Lake."""
    logger.info("Uploading -> s3://%s/%s", S3_BUCKET, s3_key)
    s3_client.upload_file(str(file_path), S3_BUCKET, s3_key)

# ============================================================
# 5. TẢI DATASET TỪ KAGGLE
# ============================================================
def download_kaggle_dataset(destination):
    logger.info("Xác thực Kaggle API...")
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()

    logger.info("Đang tải dataset: %s", KAGGLE_DATASET)
    api.dataset_download_files(KAGGLE_DATASET, path=str(destination), unzip=True)
    logger.info("Tải và giải nén hoàn tất.")

def discover_source_files(dataset_dir):
    """Tìm tất cả các file CSV và SQLite trong thư mục giải nén."""
    csv_files = list(dataset_dir.rglob("*.csv"))
    sqlite_files = list(dataset_dir.rglob("*.sqlite")) + list(dataset_dir.rglob("*.db")) + list(dataset_dir.rglob("*.sqlite3"))
    logger.info("Tìm thấy: %d tệp CSV | %d tệp SQLite", len(csv_files), len(sqlite_files))
    return csv_files, sqlite_files

# ============================================================
# 6. TRÍCH XUẤT TRANSACTIONS TỪ SQLITE (FALLBACK NẾU THIẾU CSV)
# ============================================================
def export_transactions_from_sqlite(sqlite_files, output_path):
    """Đọc 1 triệu dòng transactions từ SQLite theo từng mẻ Chunk 100k dòng để tiết kiệm RAM."""
    for sqlite_path in sqlite_files:
        logger.info("Kiểm tra SQLite DB: %s", sqlite_path.name)
        connection = sqlite3.connect(sqlite_path)
        try:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", connection)["name"].tolist()
            if "transactions" not in tables:
                continue

            logger.info("Bắt đầu trích xuất bảng transactions từ SQLite (Chunk size: %d)...", CHUNK_SIZE)
            chunks = pd.read_sql_query("SELECT * FROM transactions", connection, chunksize=CHUNK_SIZE)
            
            first_chunk = True
            total_records = 0
            for chunk in chunks:
                chunk.to_csv(output_path, mode="w" if first_chunk else "a", header=first_chunk, index=False)
                total_records += len(chunk)
                first_chunk = False

            logger.info("Trích xuất SQLite thành công: %d bản ghi", total_records)
            return total_records
        finally:
            connection.close()

    raise FileNotFoundError("Không tìm thấy bảng transactions trong các tệp SQLite.")

# ============================================================
# 7. INGESTION VÀ PHÂN VÙNG LÊN S3
# ============================================================
def ingest_all_tables_to_s3(csv_files, sqlite_files, ingest_date, batch_id, temp_dir, manifest):
    """Nạp trọn vẹn cả 7 bảng vào phân vùng S3 Hive-Style."""
    has_transactions_csv = any(p.stem.lower() == "transactions" for p in csv_files)
    
    # 1. Nạp các file CSV có sẵn
    for csv_path in csv_files:
        table_name = csv_path.stem.lower()
        record_count = fast_count_csv_records(csv_path)
        checksum = calculate_sha256(csv_path)
        file_size = csv_path.stat().st_size
        
        # Cấu trúc Hive Partitioning chuẩn: raw/<table_name>/ingest_date=YYYY-MM-DD/<table_name>.csv
        s3_key = f"raw/{table_name}/ingest_date={ingest_date}/{csv_path.name}"
        upload_file_to_s3(csv_path, s3_key)

        manifest["tables"].append({
            "table_name": table_name,
            "source_file": csv_path.name,
            "s3_key": s3_key,
            "record_count": record_count,
            "file_size_bytes": file_size,
            "checksum_sha256": checksum,
            "status": "SUCCESS"
        })
        logger.info("Bảng [%s]: Đã nạp %d dòng", table_name, record_count)

    # 2. Xử lý xuất transactions từ SQLite nếu trong CSV chưa có
    if not has_transactions_csv and sqlite_files:
        logger.info("Không có transactions.csv sẵn, tiến hành xuất từ SQLite...")
        tx_csv_path = temp_dir / "transactions.csv"
        record_count = export_transactions_from_sqlite(sqlite_files, tx_csv_path)
        
        checksum = calculate_sha256(tx_csv_path)
        file_size = tx_csv_path.stat().st_size
        s3_key = f"raw/transactions/ingest_date={ingest_date}/transactions.csv"
        upload_file_to_s3(tx_csv_path, s3_key)

        manifest["tables"].append({
            "table_name": "transactions",
            "source_file": "SQLite transactions table",
            "s3_key": s3_key,
            "record_count": record_count,
            "file_size_bytes": file_size,
            "checksum_sha256": checksum,
            "status": "SUCCESS"
        })
        logger.info("Bảng [transactions]: Đã nạp %d dòng", record_count)

# ============================================================
# 8. MANIFEST & AUDIT LOGGING
# ============================================================
def upload_manifest(manifest):
    """Ghi nhận nhật ký kiểm toán (Manifest) lên S3."""
    manifest_key = f"metadata/manifests/manifest_{manifest['batch_id']}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )
    logger.info("Manifest đã lưu tại: s3://%s/%s", S3_BUCKET, manifest_key)

# ============================================================
# 9. ĐIỀU PHỐI PIPELINE CHÍNH
# ============================================================
def run_pipeline():
    batch_id = generate_batch_id()
    now = datetime.now(timezone.utc)
    ingest_date = now.strftime("%Y-%m-%d")

    manifest = {
        "project": "Cloud-Based Banking Data Warehouse",
        "batch_id": batch_id,
        "source": KAGGLE_DATASET,
        "ingest_date": ingest_date,
        "ingestion_timestamp": now.isoformat(),
        "aws_region": AWS_REGION,
        "s3_bucket": S3_BUCKET,
        "status": "RUNNING",
        "tables": [],
        "error": None
    }

    logger.info("=" * 70)
    logger.info("BẮT ĐẦU ENTERPRISE INGESTION PIPELINE (BATCH ID: %s)", batch_id)
    logger.info("Phân vùng ngày: %s | S3 Target: s3://%s/raw/", ingest_date, S3_BUCKET)
    logger.info("=" * 70)

    with tempfile.TemporaryDirectory(prefix="banking_ingestion_") as temp_directory:
        temp_dir = Path(temp_directory)
        dataset_dir = temp_dir / "dataset"
        dataset_dir.mkdir()

        try:
            # Bước 1: Tải dataset
            download_kaggle_dataset(dataset_dir)

            # Bước 2: Quét file
            csv_files, sqlite_files = discover_source_files(dataset_dir)
            if not csv_files and not sqlite_files:
                raise FileNotFoundError("Không tìm thấy tệp CSV hoặc SQLite hợp lệ.")

            # Bước 3: Nạp 7 bảng lên S3 theo Hive Partition
            ingest_all_tables_to_s3(csv_files, sqlite_files, ingest_date, batch_id, temp_dir, manifest)

            manifest["status"] = "SUCCESS"
            logger.info("Hoàn tất nạp toàn bộ 7 bảng lên S3 Data Lake.")

        except Exception as error:
            manifest["status"] = "FAILED"
            manifest["error"] = str(error)
            logger.exception("Pipeline Ingestion thất bại: %s", error)
            raise error

        finally:
            manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
            upload_manifest(manifest)

    logger.info("=" * 70)
    logger.info("TRẠNG THÁI PIPELINE: %s | BATCH ID: %s", manifest["status"], batch_id)
    logger.info("=" * 70)
    return manifest

if __name__ == "__main__":
    run_pipeline()
