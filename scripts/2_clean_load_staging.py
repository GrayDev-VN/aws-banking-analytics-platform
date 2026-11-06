import os
import io
import sys
import time
from datetime import datetime, timezone
import pandas as pd
import psycopg2
import boto3
from dotenv import load_dotenv

# Đảm bảo in tiếng Việt trên console Windows không bị lỗi font
if __name__ == "__main__" and sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# Cấu hình AWS S3 & RDS PostgreSQL
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
BUCKET_NAME = os.getenv("AWS_S3_BUCKET")
INGEST_DATE = os.getenv("INGEST_DATE")


def get_latest_s3_ingest_date(s3_client, bucket_name: str) -> str:
    """Tự động tìm kiếm partition ingest_date mới nhất trên S3"""
    try:
        resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix="raw/branches/", Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        dates = []
        for p in prefixes:
            if "ingest_date=" in p:
                d_str = p.split("ingest_date=")[-1].strip("/")
                dates.append(d_str)
        if dates:
            return sorted(dates)[-1]
    except Exception as e:
        print(f"⚠️ Không thể quét partition S3 tự động: {e}")
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

DB_HOST = os.getenv("DB_HOST") or os.getenv("RDS_HOST")
DB_PORT = os.getenv("DB_PORT") or os.getenv("RDS_PORT", "5432")
DB_NAME = os.getenv("DB_NAME") or os.getenv("RDS_DATABASE", "banking_dwh")
DB_USER = os.getenv("DB_USER") or os.getenv("RDS_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("RDS_PASSWORD")

TABLE_CONFIGS = [
    {
        "table_name": "branches",
        "stg_table": "staging.stg_branches",
        "columns": ["branch_id", "branch_name", "manager_name"],
        "dtype": {"branch_id": str, "branch_name": str, "manager_name": str}
    },
    {
        "table_name": "merchants",
        "stg_table": "staging.stg_merchants",
        "columns": ["merchant_id", "merchant_name", "city"],
        "dtype": {"merchant_id": str, "merchant_name": str, "city": str}
    },
    {
        "table_name": "customers",
        "stg_table": "staging.stg_customers",
        "columns": ["customer_id", "first_name", "last_name", "email", "city", "credit_score", "created_at"],
        "dtype": {"customer_id": str, "first_name": str, "last_name": str, "email": str, "city": str, "credit_score": "Int64"},
        "date_cols": ["created_at"]
    },
    {
        "table_name": "accounts",
        "stg_table": "staging.stg_accounts",
        "columns": ["account_id", "customer_id", "account_type", "balance_usd", "open_date"],
        "dtype": {"account_id": str, "customer_id": str, "account_type": str, "balance_usd": float},
        "date_cols": ["open_date"]
    },
    {
        "table_name": "cards",
        "stg_table": "staging.stg_cards",
        "columns": ["card_id", "account_id", "card_type", "expiration_date"],
        "dtype": {"card_id": str, "account_id": str, "card_type": str},
        "date_cols": ["expiration_date"]
    },
    {
        "table_name": "loans",
        "stg_table": "staging.stg_loans",
        "columns": ["loan_id", "customer_id", "loan_amount", "interest_rate", "start_date"],
        "dtype": {"loan_id": str, "customer_id": str, "loan_amount": float, "interest_rate": float},
        "date_cols": ["start_date"]
    },
    {
        "table_name": "transactions",
        "stg_table": "staging.stg_transactions",
        "columns": ["transaction_id", "account_id", "merchant_id", "amount_usd", "transaction_date"],
        "dtype": {"transaction_id": str, "account_id": str, "merchant_id": str, "amount_usd": float},
        "datetime_cols": ["transaction_date"],
        "chunksize": 250000  # Chia 1 triệu dòng thành các mảng 250k dòng để không bị tràn RAM
    }
]

def get_db_connection():
    """Tạo kết nối tới PostgreSQL RDS với mã hóa SSL"""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode="require"
    )

def parse_datetime_column(values: pd.Series, dayfirst: bool = True) -> pd.Series:
    """Ưu tiên ISO để không đảo tháng/ngày; fallback cho định dạng hỗn hợp.

    Ngày mơ hồ như 03/04/2023 mặc định hiểu là 3 tháng 4.
    Cấu hình dayfirst=False nếu nguồn sử dụng tháng/ngày.
    """
    parsed = pd.to_datetime(values, format="ISO8601", errors="coerce")
    fallback = parsed.isna() & values.notna()
    if fallback.any():
        parsed.loc[fallback] = pd.to_datetime(
            values.loc[fallback], format="mixed", errors="coerce", dayfirst=dayfirst
        )
    return parsed


def clean_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Làm sạch và chuẩn hóa dữ liệu trước khi nạp"""
    # 1. Loại bỏ khoảng trắng thừa đầu/cuối của các cột chuỗi
    for col in df.select_dtypes(include=["object", "string"]).columns:
        # Giữ giá trị thiếu là NULL, không chuyển thành chuỗi "None"/"nan".
        df[col] = df[col].astype("string").str.strip()

    # 2. Chuẩn hóa cột ngày tháng dạng YYYY-MM-DD
    if "date_cols" in config:
        for col in config["date_cols"]:
            if col in df.columns:
                df[col] = parse_datetime_column(
                    df[col], dayfirst=config.get("dayfirst", True)
                ).dt.strftime("%Y-%m-%d")

    # 3. Chuẩn hóa cột thời gian dạng YYYY-MM-DD HH:MM:SS
    if "datetime_cols" in config:
        for col in config["datetime_cols"]:
            if col in df.columns:
                df[col] = parse_datetime_column(
                    df[col], dayfirst=config.get("dayfirst", True)
                ).dt.strftime("%Y-%m-%d %H:%M:%S")

    return df

def copy_df_to_postgres(cursor, df: pd.DataFrame, table_name: str, columns: list):
    """Nạp DataFrame vào Postgres bằng giao thức COPY siêu tốc qua RAM"""
    output = io.StringIO()
    df.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N", columns=columns)
    output.seek(0)
    
    cols_str = ", ".join(columns)
    sql = f"COPY {table_name} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
    cursor.copy_expert(sql, output)

def run_staging_pipeline():
    start_total = time.time()
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    
    ingest_date = INGEST_DATE or get_latest_s3_ingest_date(s3_client, BUCKET_NAME)
    
    print("=" * 80)
    print("🚀 BẮT ĐẦU ETL PIPELINE: S3 DATA LAKE --> RDS POSTGRESQL STAGING")
    print(f"📅 Mẻ Ingestion Date: {ingest_date}")
    print(f"🗄️ Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 80)

    conn = get_db_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        for config in TABLE_CONFIGS:
            table_name = config["table_name"]
            stg_table = config["stg_table"]
            columns = config["columns"]
            s3_key = f"raw/{table_name}/ingest_date={ingest_date}/{table_name}.csv"

            print(f"\n📦 [1/3] Đang xử lý bảng: {table_name.upper()}...")
            start_table = time.time()

            # 1. Truncate bảng staging trước khi nạp mẻ mới
            cursor.execute(f"TRUNCATE TABLE {stg_table};")

            # 2. Đọc file từ S3
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            csv_body = response["Body"]

            total_rows_loaded = 0

            # 3. Nạp dữ liệu (xử lý chunk nếu là bảng transactions)
            if "chunksize" in config:
                print(f"   ⏳ Đang đọc và nạp theo Chunk ({config['chunksize']:,} dòng/chunk)...")
                for chunk in pd.read_csv(csv_body, dtype=config.get("dtype"), chunksize=config["chunksize"]):
                    chunk = clean_dataframe(chunk, config)
                    copy_df_to_postgres(cursor, chunk, stg_table, columns)
                    total_rows_loaded += len(chunk)
                    print(f"      -> Đã nạp tích lũy: {total_rows_loaded:,} dòng...")
            else:
                df = pd.read_csv(csv_body, dtype=config.get("dtype"))
                df = clean_dataframe(df, config)
                copy_df_to_postgres(cursor, df, stg_table, columns)
                total_rows_loaded = len(df)

            conn.commit()
            elapsed_table = time.time() - start_table
            print(f"   ✅ Hoàn tất [{stg_table}]: {total_rows_loaded:,} dòng trong {elapsed_table:.2f} giây.")

        # 4. Kiểm tra đối soát số lượng bản ghi sau khi nạp
        print("\n" + "=" * 80)
        print("🔍 KIỂM TRA SỐ LƯỢNG BẢN GHI TRONG CÁC BẢNG STAGING:")
        print("=" * 80)
        for config in TABLE_CONFIGS:
            stg_table = config["stg_table"]
            cursor.execute(f"SELECT COUNT(*) FROM {stg_table};")
            count = cursor.fetchone()[0]
            print(f" - {stg_table:28}: {count:,} dòng")

        print("=" * 80)
        print(f"🎉 HOÀN TẤT TOÀN BỘ STAGING PIPELINE TRONG {time.time() - start_total:.2f} GIÂY!")
        print("=" * 80)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ LỖI TRONG QUÁ TRÌNH THỰC HIỆN PIPELINE: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    run_staging_pipeline()
