"""
================================================================================
LAB 7: AUTOMATED DATA QUALITY TESTING & VALIDATION FRAMEWORK
================================================================================
Mục đích:
  Kiểm định chất lượng toàn diện của Data Warehouse & Data Marts trước khi bàn giao
  cho hệ thống BI & Reporting. Hỗ trợ chạy cả bằng `pytest` và chạy trực tiếp CLI.

Các tiêu chuẩn kiểm tra (Dimensions of Data Quality):
  1. Table Completeness & Volume: Kiểm tra các bảng tồn tại và đủ số lượng bản ghi.
  2. Uniqueness & Primary Key: Đảm bảo tính duy nhất của Surrogate & Natural Keys.
  3. Referential Integrity: Kiểm tra 100% Khóa ngoại (FK) hợp lệ, không có orphan keys.
  4. Business Logic & Value Range: Kiểm tra miền giá trị tài chính (FICO, Lãi suất, Số tiền).
  5. Data Mart Reconciliation: Đối soát dữ liệu Marts có khớp 100% với Facts gốc hay không.
================================================================================
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import pytest

# Đảm bảo in tiếng Việt và Emoji trên console Windows không bị lỗi encoding
if __name__ == "__main__" and sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# Cấu hình kết nối RDS PostgreSQL
DB_HOST = os.getenv("DB_HOST") or os.getenv("RDS_HOST")
DB_PORT = os.getenv("DB_PORT") or os.getenv("RDS_PORT", "5432")
DB_NAME = os.getenv("DB_NAME") or os.getenv("RDS_DATABASE", "banking_dwh")
DB_USER = os.getenv("DB_USER") or os.getenv("RDS_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("RDS_PASSWORD")


@pytest.fixture(scope="module")
def db_conn():
    """Fixture kết nối database dùng chung cho pytest"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    yield conn
    conn.close()


# ==============================================================================
# NHÓM 1: KIỂM TRA SỰ TỒN TẠI VÀ SỐ LƯỢNG BẢN GHI (COMPLETENESS & VOLUME)
# ==============================================================================

def test_warehouse_tables_exist_and_not_empty(db_conn):
    """Kiểm tra tất cả các bảng DWH đều có dữ liệu và không bị rỗng"""
    tables = [
        ("warehouse", "dim_date", 4000),
        ("warehouse", "dim_customer", 50000),
        ("warehouse", "dim_account", 75000),
        ("warehouse", "dim_card", 100000),
        ("warehouse", "dim_merchant", 5000),
        ("warehouse", "dim_branch", 500),
        ("warehouse", "fact_transactions", 1000000),
        ("warehouse", "fact_loans", 30000),
    ]
    with db_conn.cursor() as cur:
        for schema, table, min_rows in tables:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
            count = cur.fetchone()[0]
            assert count >= min_rows, f"Bảng {schema}.{table} chỉ có {count} dòng, ít hơn mức kỳ vọng {min_rows} dòng!"


def test_data_marts_exist_and_not_empty(db_conn):
    """Kiểm tra các bảng Data Marts đã được tạo và chứa dữ liệu phân tích"""
    marts = [
        ("mart", "customer_360", 50000),
        ("mart", "daily_branch_performance", 200000),
        ("mart", "merchant_analytics", 5000),
        ("mart", "credit_risk_monitoring", 4),
    ]
    with db_conn.cursor() as cur:
        for schema, table, min_rows in marts:
            cur.execute(f"SELECT COUNT(*) FROM {schema}.{table};")
            count = cur.fetchone()[0]
            assert count >= min_rows, f"Data Mart {schema}.{table} chỉ có {count} dòng, ít hơn mức kỳ vọng {min_rows} dòng!"


# ==============================================================================
# NHÓM 2: KIỂM TRA TÍNH DUY NHẤT VÀ KHÔNG ĐƯỢC NULL (UNIQUENESS & NOT NULL)
# ==============================================================================

def test_primary_keys_uniqueness_and_not_null(db_conn):
    """Kiểm tra Surrogate Keys trong các bảng Dimension & Fact là duy nhất và NOT NULL"""
    pk_checks = [
        ("warehouse.dim_customer", "customer_key"),
        ("warehouse.dim_account", "account_key"),
        ("warehouse.dim_card", "card_key"),
        ("warehouse.dim_merchant", "merchant_key"),
        ("warehouse.dim_branch", "branch_key"),
        ("warehouse.dim_date", "date_key"),
        ("warehouse.fact_transactions", "transaction_key"),
        ("warehouse.fact_loans", "loan_key"),
    ]
    with db_conn.cursor() as cur:
        for table, pk_col in pk_checks:
            # Check 1: NOT NULL
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {pk_col} IS NULL;")
            null_count = cur.fetchone()[0]
            assert null_count == 0, f"Phát hiện {null_count} giá trị NULL trong cột khóa chính {table}.{pk_col}!"

            # Check 2: Uniqueness
            cur.execute(f"""
                SELECT {pk_col}, COUNT(*) 
                FROM {table} 
                GROUP BY {pk_col} 
                HAVING COUNT(*) > 1;
            """)
            duplicates = cur.fetchall()
            assert len(duplicates) == 0, f"Phát hiện trùng lặp khóa chính trong {table}.{pk_col}: {duplicates[:5]}!"


# ==============================================================================
# NHÓM 3: KIỂM TRA TÍNH TOÀN VẸN THAM CHIẾU (REFERENTIAL INTEGRITY - NO ORPHANS)
# ==============================================================================

def test_fact_transactions_referential_integrity(db_conn):
    """Đảm bảo 100% Khóa ngoại trong fact_transactions đều trỏ đến Dimension hợp lệ"""
    fk_checks = [
        ("customer_key", "warehouse.dim_customer", "customer_key"),
        ("account_key", "warehouse.dim_account", "account_key"),
        ("card_key", "warehouse.dim_card", "card_key"),
        ("merchant_key", "warehouse.dim_merchant", "merchant_key"),
        ("branch_key", "warehouse.dim_branch", "branch_key"),
        ("date_key", "warehouse.dim_date", "date_key"),
    ]
    with db_conn.cursor() as cur:
        for fk_col, dim_table, dim_pk in fk_checks:
            query = f"""
                SELECT COUNT(*) 
                FROM warehouse.fact_transactions f
                LEFT JOIN {dim_table} d ON f.{fk_col} = d.{dim_pk}
                WHERE d.{dim_pk} IS NULL;
            """
            cur.execute(query)
            orphan_count = cur.fetchone()[0]
            assert orphan_count == 0, f"Phát hiện {orphan_count} orphan FKs trong fact_transactions.{fk_col} không tìm thấy trong {dim_table}!"


def test_fact_loans_referential_integrity(db_conn):
    """Đảm bảo 100% Khóa ngoại trong fact_loans đều trỏ đến Dimension hợp lệ"""
    fk_checks = [
        ("customer_key", "warehouse.dim_customer", "customer_key"),
        ("branch_key", "warehouse.dim_branch", "branch_key"),
        ("start_date_key", "warehouse.dim_date", "date_key"),
    ]
    with db_conn.cursor() as cur:
        for fk_col, dim_table, dim_pk in fk_checks:
            query = f"""
                SELECT COUNT(*) 
                FROM warehouse.fact_loans f
                LEFT JOIN {dim_table} d ON f.{fk_col} = d.{dim_pk}
                WHERE d.{dim_pk} IS NULL;
            """
            cur.execute(query)
            orphan_count = cur.fetchone()[0]
            assert orphan_count == 0, f"Phát hiện {orphan_count} orphan FKs trong fact_loans.{fk_col} không tìm thấy trong {dim_table}!"


# ==============================================================================
# NHÓM 4: KIỂM TRA RÀNG BUỘC NGHIỆP VỤ & MIỀN GIÁ TRỊ (BUSINESS RANGE VALIDATION)
# ==============================================================================

def test_financial_measures_business_ranges(db_conn):
    """Kiểm tra các chỉ số tài chính tuân thủ quy tắc nghiệp vụ ngân hàng"""
    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Số tiền giao dịch phải lớn hơn 0
        cur.execute("SELECT COUNT(*) AS bad_count FROM warehouse.fact_transactions WHERE amount_usd <= 0;")
        res = cur.fetchone()
        assert res["bad_count"] == 0, f"Có {res['bad_count']} giao dịch có amount_usd <= 0!"

        # 2. Lãi suất vay phải trong khoảng 0% - 40% và số tiền vay > 0
        cur.execute("""
            SELECT COUNT(*) AS bad_count 
            FROM warehouse.fact_loans 
            WHERE loan_amount <= 0 OR interest_rate < 0 OR interest_rate > 40;
        """)
        res = cur.fetchone()
        assert res["bad_count"] == 0, f"Có {res['bad_count']} hợp đồng vay vi phạm miền giá trị lãi suất/khoản vay!"

        # 3. Điểm tín dụng FICO Score phải trong khoảng 300 - 850 (đối với khách hàng thực)
        cur.execute("""
            SELECT COUNT(*) AS bad_count 
            FROM warehouse.dim_customer 
            WHERE customer_id != 'UNKNOWN' AND (credit_score < 300 OR credit_score > 850);
        """)
        res = cur.fetchone()
        assert res["bad_count"] == 0, f"Có {res['bad_count']} khách hàng có FICO Score ngoài dải [300, 850]!"


# ==============================================================================
# NHÓM 5: ĐỐI SOÁT DỮ LIỆU DATA MARTS (DATA MART RECONCILIATION)
# ==============================================================================

def test_data_marts_reconciliation_with_facts(db_conn):
    """Đối soát tổng số lượng bản ghi giữa Marts và Fact gốc"""
    with db_conn.cursor(cursor_factory=RealDictCursor) as cur:
        # 1. Đối soát Customer 360 với dim_customer
        cur.execute("SELECT COUNT(*) AS dim_count FROM warehouse.dim_customer WHERE customer_id != 'UNKNOWN';")
        dim_cust = cur.fetchone()["dim_count"]
        cur.execute("SELECT COUNT(*) AS mart_count FROM mart.customer_360;")
        mart_cust = cur.fetchone()["mart_count"]
        assert dim_cust == mart_cust, f"Customer 360 ({mart_cust}) không khớp số lượng với dim_customer ({dim_cust})!"

        # 2. Đối soát Merchant Analytics với Fact Transactions
        cur.execute("SELECT COUNT(*) AS fact_count FROM warehouse.fact_transactions;")
        fact_tx_count = cur.fetchone()["fact_count"]
        cur.execute("SELECT SUM(total_transactions) AS mart_tx_count FROM mart.merchant_analytics;")
        mart_tx_count = cur.fetchone()["mart_tx_count"]
        assert fact_tx_count == mart_tx_count, f"Tổng giao dịch ở Merchant Analytics ({mart_tx_count}) không khớp với Fact ({fact_tx_count})!"

        # 3. Đối soát tổng số hợp đồng vay ở Credit Risk với Fact Loans
        cur.execute("SELECT COUNT(*) AS fact_loans FROM warehouse.fact_loans;")
        fact_loans = cur.fetchone()["fact_loans"]
        cur.execute("SELECT SUM(total_loans_originated) AS mart_loans FROM mart.credit_risk_monitoring;")
        mart_loans = cur.fetchone()["mart_loans"]
        assert fact_loans == mart_loans, f"Tổng số khoản vay ở Credit Risk Mart ({mart_loans}) không khớp với Fact Loans ({fact_loans})!"


# ==============================================================================
# CLI RUNNER KHI CHẠY TRỰC TIẾP `python tests/test_data_quality.py`
# ==============================================================================

def run_all_checks_cli():
    print("=" * 80)
    print("🔍 BẮT ĐẦU CHẠY KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY SUITE)")
    print(f"🗄️ Database Target: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print("=" * 80)

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except Exception as e:
        print(f"❌ Kết nối Database thất bại: {e}")
        sys.exit(1)

    tests = [
        ("Table Completeness & Volume", test_warehouse_tables_exist_and_not_empty),
        ("Data Marts Completeness", test_data_marts_exist_and_not_empty),
        ("Primary Keys Uniqueness & Not-Null", test_primary_keys_uniqueness_and_not_null),
        ("Fact Transactions Referential Integrity", test_fact_transactions_referential_integrity),
        ("Fact Loans Referential Integrity", test_fact_loans_referential_integrity),
        ("Financial Business Range Validation", test_financial_measures_business_ranges),
        ("Data Mart Reconciliation", test_data_marts_reconciliation_with_facts),
    ]

    passed = 0
    failed = 0

    for name, func in tests:
        print(f"👉 Đang kiểm tra: {name}...", end=" ")
        try:
            func(conn)
            print("✅ PASSED")
            passed += 1
        except AssertionError as ae:
            conn.rollback()
            print(f"❌ FAILED\n   Lỗi: {ae}")
            failed += 1
        except Exception as ex:
            conn.rollback()
            print(f"💥 ERROR\n   Lỗi ngoại lệ: {ex}")
            failed += 1

    conn.close()
    print("=" * 80)
    if failed == 0:
        print(f"🎉 TẤT CẢ {passed}/{len(tests)} BỘ KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU ĐÃ ĐẠT CHUẨN (100% PASSED)!")
    else:
        print(f"⚠️ KẾT QUẢ: {passed} PASSED, {failed} FAILED. VUI LÒNG KIỂM TRA LẠI DỮ LIỆU DWH!")
    print("=" * 80)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    run_all_checks_cli()
