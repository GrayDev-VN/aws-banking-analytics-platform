"""
DATA MARTS BUILDER PIPELINE
===========================
Thực thi tạo và nạp dữ liệu cho 4 Data Marts chiến lược trong schema `mart` trên PostgreSQL RDS:
1. mart.customer_360
2. mart.daily_branch_performance
3. mart.merchant_analytics
4. mart.credit_risk_monitoring
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

def build_marts():
    start_total = time.time()
    print("=" * 80)
    print("📊 BẮT ĐẦU XÂY DỰNG DATA MARTS: WAREHOUSE --> SCHEMA MART")
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
        print("\n📄 Đang thực thi: sql/06_create_data_marts.sql...")
        with open("sql/06_create_data_marts.sql", "r", encoding="utf-8") as f:
            sql = f.read()
        
        cursor.execute(sql)
        conn.commit()

        print("\n" + "=" * 80)
        print("🔍 KIỂM TRA SỐ LƯỢNG BẢN GHI TRONG CÁC BẢNG DATA MART:")
        print("=" * 80)
        marts = [
            "mart.customer_360",
            "mart.daily_branch_performance",
            "mart.merchant_analytics",
            "mart.credit_risk_monitoring"
        ]
        for m in marts:
            cursor.execute(f"SELECT COUNT(*) FROM {m};")
            count = cursor.fetchone()[0]
            print(f" - {m:35}: {count:,} dòng")

        print("=" * 80)
        print(f"🎉 HOÀN TẤT XÂY DỰNG DATA MARTS TRONG {time.time() - start_total:.2f} GIÂY!")
        print("=" * 80)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ LỖI KHI TẠO DATA MARTS: {e}")
        raise e
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    build_marts()
