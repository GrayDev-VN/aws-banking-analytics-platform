"""
================================================================================
MASTER END-TO-END PIPELINE RUNNER (LAB 9)
================================================================================
Mục đích:
  Điều phối toàn bộ chu trình ETL tự động từ S3 Data Lake đến Data Marts và
  bộ kiểm định chất lượng dữ liệu, tích hợp hệ thống cảnh báo đa kênh Amazon SNS.

Chu trình thực thi:
  1. STAGE 1: S3 Data Lake ➔ Staging Schema (2_clean_load_staging.py)
  2. STAGE 2: Staging ➔ Warehouse Star Schema (3_build_warehouse.py)
  3. STAGE 3: Warehouse ➔ Strategic Data Marts (4_build_marts.py)
  4. STAGE 4: Automated Data Quality Assurance Suite (test_data_quality.py)
  5. NOTIFICATION: Tự động gửi thông báo qua Amazon SNS (Success / Quality Alert / Failure)
================================================================================
"""

import os
import sys
import time
import traceback
import importlib
from datetime import datetime, timezone
from dotenv import load_dotenv

# Đảm bảo thư mục gốc dự án nằm trong PYTHONPATH
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Đảm bảo in tiếng Việt trên console Windows không bị lỗi
if __name__ == "__main__" and sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

# Import các module pipeline
run_staging_pipeline = importlib.import_module("scripts.2_clean_load_staging").run_staging_pipeline
build_warehouse = importlib.import_module("scripts.3_build_warehouse").build_warehouse
build_marts = importlib.import_module("scripts.4_build_marts").build_marts
from tests.test_data_quality import run_all_checks_cli

def send_sns_notification(alert_type: str, stage: str = None, error: str = None, stack_trace: str = None, duration: float = 0, failed_tests: list = None):
    """Gọi Lambda Handler để phát thông báo qua Amazon SNS"""
    # Không tạo AWS client khi chỉ import runner để kiểm thử.
    alert_module = importlib.import_module("lambda.alert_on_failure.lambda_function")
    event = {
        "alert_type": alert_type,
        "stage": stage,
        "error": error,
        "stack_trace": stack_trace,
        "duration_seconds": duration,
        "failed_tests": failed_tests or [],
        "ingest_date": datetime.now(timezone.utc).strftime("%Y-%m-%d")
    }
    alert_module.lambda_handler(event, None)


def execute_full_pipeline():
    start_time = time.time()
    current_stage = "INITIALIZATION"

    print("=" * 80)
    print("🚀 BẮT ĐẦU CHU TRÌNH ĐIỀU PHỐI END-TO-END PIPELINE (APEX BANK DWH)")
    print(f"⏰ Thời gian bắt đầu: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)

    try:
        # GIAI ĐOẠN 1: NẠP STAGING TỪ S3
        current_stage = "1. S3_TO_STAGING (clean_load_staging)"
        print(f"\n▶️ BẮT ĐẦU GIAI ĐOẠN: {current_stage}")
        run_staging_pipeline()

        # GIAI ĐOẠN 2: XÂY DỰNG DATA WAREHOUSE (STAR SCHEMA)
        current_stage = "2. STAGING_TO_WAREHOUSE (build_warehouse)"
        print(f"\n▶️ BẮT ĐẦU GIAI ĐOẠN: {current_stage}")
        build_warehouse()

        # GIAI ĐOẠN 3: XÂY DỰNG TẦNG DATA MARTS
        current_stage = "3. WAREHOUSE_TO_DATA_MARTS (build_marts)"
        print(f"\n▶️ BẮT ĐẦU GIAI ĐOẠN: {current_stage}")
        build_marts()

        # GIAI ĐOẠN 4: KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU TỰ ĐỘNG
        current_stage = "4. DATA_QUALITY_ASSURANCE (test_data_quality)"
        print(f"\n▶️ BẮT ĐẦU GIAI ĐOẠN: {current_stage}")
        try:
            run_all_checks_cli()
        except SystemExit as se:
            if se.code != 0:
                print("⚠️ Phát hiện bài test kiểm định chất lượng dữ liệu không đạt chuẩn!")
                send_sns_notification(
                    alert_type="DATA_QUALITY_ALERT",
                    stage="Data Quality Suite",
                    failed_tests=[{"name": "DWH Quality Checks", "reason": "One or more assertions failed"}]
                )
                raise Exception("Data Quality Suite failed assertion checks.")

        total_duration = time.time() - start_time
        print("\n" + "=" * 80)
        print(f"🎉 TOÀN BỘ END-TO-END PIPELINE ĐÃ HOÀN TẤT THÀNH CÔNG TRONG {total_duration:.2f} GIÂY!")
        print("=" * 80)

        # Gửi thông báo thành công qua Amazon SNS
        send_sns_notification(
            alert_type="SUCCESS",
            duration=total_duration
        )

    except Exception as e:
        total_duration = time.time() - start_time
        err_msg = str(e)
        st_trace = traceback.format_exc()

        print("\n" + "=" * 80)
        print(f"🚨 PIPELINE GẶP SỰ CỐ TẠI GIAI ĐOẠN: {current_stage}")
        print(f"❌ Chi tiết lỗi: {err_msg}")
        print("=" * 80)

        # Gửi thông báo khẩn cấp (P1 Critical Alert) qua Amazon SNS
        send_sns_notification(
            alert_type="FAILURE",
            stage=current_stage,
            error=err_msg,
            stack_trace=st_trace,
            duration=total_duration
        )
        sys.exit(1)


if __name__ == "__main__":
    execute_full_pipeline()
