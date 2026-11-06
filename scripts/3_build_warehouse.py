"""
WAREHOUSE DIMENSIONAL TRANSFORMATION PIPELINE
=============================================
Script thực thi các chuyển đổi dữ liệu từ tầng `staging` sang tầng `warehouse` (Star Schema).
Thứ tự thực hiện:
1. Nạp và làm sạch 5 Dimension Tables (sql/03_insert_dimensions.sql).
2. Nạp và làm sạch 2 Fact Tables có liên kết khóa Surrogate (sql/04_insert_facts.sql).
3. Đối soát số lượng bản ghi sau khi nạp.
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv

if __name__ == "__main__" and sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

DB_HOST = os.getenv("DB_HOST") or os.getenv("RDS_HOST")
DB_PORT = os.getenv("DB_PORT") or os.getenv("RDS_PORT", "5432")
DB_NAME = os.getenv("DB_NAME") or os.getenv("RDS_DATABASE", "banking_dwh")
DB_USER = os.getenv("DB_USER") or os.getenv("RDS_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("RDS_PASSWORD")

def run_sql_file(cursor, file_path):
    print(f"📄 Đang thực thi: {file_path}...")
    with open(file_path, "r", encoding="utf-8") as f:
        sql = f.read()
    cursor.execute(sql)

def build_warehouse():
    start_total = time.time()
    print("=" * 80)
    print("🏗️ BẮT ĐẦU DIMENSIONAL TRANSFORMATION: STAGING --> WAREHOUSE STAR SCHEMA")
    print(f"🗄️ Database: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 80)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        sslmode="require"
    )
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        # Bước 0: Làm sạch bảng Fact và Dim cũ trước khi nạp mẻ mới (trừ dim_date)
        cursor.execute("""
            TRUNCATE TABLE 
                warehouse.fact_transactions, 
                warehouse.fact_loans, 
                warehouse.dim_customer, 
                warehouse.dim_account, 
                warehouse.dim_card, 
                warehouse.dim_merchant, 
                warehouse.dim_branch 
            CASCADE;
        """)
        conn.commit()

        # Bước 1: Nạp các bảng Dimension
        start_step = time.time()
        print("\n📦 [1/2] Đang nạp và làm sạch 5 Dimension Tables...")
        run_sql_file(cursor, "sql/03_insert_dimensions.sql")
        conn.commit()
        print(f"   ✅ Hoàn tất Dimension Tables trong {time.time() - start_step:.2f} giây.")

        # Bước 2: Nạp các bảng Fact
        start_step = time.time()
        print("\n📦 [2/2] Đang nạp và đối soát 2 Fact Tables...")
        run_sql_file(cursor, "sql/04_insert_facts.sql")
        conn.commit()
        print(f"   ✅ Hoàn tất Fact Tables trong {time.time() - start_step:.2f} giây.")

        # Bước 3: Đối soát số lượng bản ghi trong toàn bộ Warehouse
        print("\n" + "=" * 80)
        print("🔍 KIỂM TRA SỐ LƯỢNG BẢN GHI TRONG CÁC BẢNG WAREHOUSE:")
        print("=" * 80)
        tables = [
            "warehouse.dim_date",
            "warehouse.dim_customer",
            "warehouse.dim_account",
            "warehouse.dim_card",
            "warehouse.dim_merchant",
            "warehouse.dim_branch",
            "warehouse.fact_transactions",
            "warehouse.fact_loans"
        ]
        for t in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {t};")
            count = cursor.fetchone()[0]
            print(f" - {t:30}: {count:,} dòng")

        print("=" * 80)
        print(f"🎉 HOÀN TẤT TOÀN BỘ WAREHOUSE PIPELINE TRONG {time.time() - start_total:.2f} GIÂY!")
        print("=" * 80)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ LỖI KHI XÂY DỰNG WAREHOUSE: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    build_warehouse()
