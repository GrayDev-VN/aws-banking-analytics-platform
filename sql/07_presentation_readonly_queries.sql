-- Minh chứng báo cáo. Chạy trên database đã có staging/warehouse/mart.
-- Chưa chạy trên DB trong phiên review. Chạy cả file hoặc từng SELECT riêng.
BEGIN TRANSACTION READ ONLY;

-- Q01. Quy mô thực tế: không lấy số trong comment làm kết quả chạy.
SELECT 'staging.transactions' AS layer_table, COUNT(*) AS rows FROM staging.stg_transactions
UNION ALL SELECT 'warehouse.transactions', COUNT(*) FROM warehouse.fact_transactions
UNION ALL SELECT 'warehouse.customers', COUNT(*) FROM warehouse.dim_customer
UNION ALL SELECT 'warehouse.loans', COUNT(*) FROM warehouse.fact_loans
UNION ALL SELECT 'mart.customer_360', COUNT(*) FROM mart.customer_360
UNION ALL SELECT 'mart.merchant_analytics', COUNT(*) FROM mart.merchant_analytics;

-- Q02. Truy vết một giao dịch. Card là mapping suy ra, branch có thể UNKNOWN.
SELECT t.transaction_id, t.amount_usd, t.transaction_date,
       a.account_id, c.customer_id, c.full_name,
       cr.card_id, m.merchant_id, b.branch_id, d.full_date
FROM warehouse.fact_transactions t
LEFT JOIN warehouse.dim_account a ON a.account_key = t.account_key
LEFT JOIN warehouse.dim_customer c ON c.customer_key = t.customer_key
LEFT JOIN warehouse.dim_card cr ON cr.card_key = t.card_key
LEFT JOIN warehouse.dim_merchant m ON m.merchant_key = t.merchant_key
LEFT JOIN warehouse.dim_branch b ON b.branch_key = t.branch_key
LEFT JOIN warehouse.dim_date d ON d.date_key = t.date_key
ORDER BY t.transaction_key
LIMIT 5;

-- Q03. Không có orphan chưa chứng minh mapping đầy đủ: đo UNKNOWN riêng.
SELECT COUNT(*) AS total_transactions,
       COUNT(*) FILTER (WHERE c.customer_id = 'UNKNOWN') AS unknown_customer,
       COUNT(*) FILTER (WHERE a.account_id = 'UNKNOWN') AS unknown_account,
       COUNT(*) FILTER (WHERE cr.card_id = 'UNKNOWN') AS unknown_card,
       COUNT(*) FILTER (WHERE m.merchant_id = 'UNKNOWN') AS unknown_merchant,
       COUNT(*) FILTER (WHERE b.branch_id = 'UNKNOWN') AS unknown_branch,
       ROUND(100.0 * COUNT(*) FILTER (WHERE b.branch_id = 'UNKNOWN')
             / NULLIF(COUNT(*), 0), 2) AS unknown_branch_pct
FROM warehouse.fact_transactions t
LEFT JOIN warehouse.dim_customer c ON c.customer_key = t.customer_key
LEFT JOIN warehouse.dim_account a ON a.account_key = t.account_key
LEFT JOIN warehouse.dim_card cr ON cr.card_key = t.card_key
LEFT JOIN warehouse.dim_merchant m ON m.merchant_key = t.merchant_key
LEFT JOIN warehouse.dim_branch b ON b.branch_key = t.branch_key;

-- Q04. Đối soát count VÀ tiền. Phải so cùng phạm vi, gồm cả UNKNOWN merchant.
WITH f AS (
    SELECT COUNT(*) AS n, COALESCE(SUM(amount_usd), 0) AS amount
    FROM warehouse.fact_transactions
), m AS (
    SELECT COALESCE(SUM(total_transactions), 0) AS n,
           COALESCE(SUM(total_sales_volume_usd), 0) AS amount
    FROM mart.merchant_analytics
)
SELECT f.n AS fact_count, m.n AS mart_count, f.n - m.n AS count_difference,
       f.amount AS fact_amount, m.amount AS mart_amount,
       f.amount - m.amount AS amount_difference
FROM f CROSS JOIN m;

-- Q05. Khoảng thời gian minh họa; đổi năm cho phù hợp dữ liệu thật.
-- Không bọc cột lọc trong TO_CHAR; optimizer có thể dùng index timestamp nếu có lợi.
SELECT DATE_TRUNC('month', transaction_date)::date AS month,
       COUNT(*) AS transaction_count, SUM(amount_usd) AS volume_usd,
       ROUND(AVG(amount_usd), 2) AS average_amount_usd
FROM warehouse.fact_transactions
WHERE transaction_date >= TIMESTAMP '2023-01-01'
  AND transaction_date < TIMESTAMP '2024-01-01'
GROUP BY 1
ORDER BY 1;

-- Q06. Top 3 merchant mỗi tháng. ROW_NUMBER cho tối đa 3 dòng/tháng;
-- dùng DENSE_RANK theo volume nếu muốn lấy cả các merchant đồng hạng.
WITH monthly AS (
    SELECT DATE_TRUNC('month', t.transaction_date)::date AS month,
           m.merchant_id, m.merchant_name, SUM(t.amount_usd) AS volume_usd
    FROM warehouse.fact_transactions t
    JOIN warehouse.dim_merchant m ON m.merchant_key = t.merchant_key
    WHERE m.merchant_id <> 'UNKNOWN'
    GROUP BY 1, 2, 3
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY month ORDER BY volume_usd DESC, merchant_id
    ) AS position
    FROM monthly
)
SELECT * FROM ranked WHERE position <= 3 ORDER BY month, position;

-- Q07. Chẩn đoán branch mart: nếu nhiều branch nhận cùng tổng hệ thống/ngày,
-- query có thể trả dòng nghi vấn. Đây là chẩn đoán, không thay thế kiểm tra source JOIN.
WITH daily_fact AS (
    SELECT transaction_date::date AS full_date, SUM(amount_usd) AS system_volume
    FROM warehouse.fact_transactions
    GROUP BY 1
)
SELECT m.full_date, COUNT(*) AS branch_rows,
       MIN(m.daily_transaction_volume_usd) AS min_branch_volume,
       MAX(m.daily_transaction_volume_usd) AS max_branch_volume,
       SUM(m.daily_transaction_volume_usd) AS sum_branch_volume,
       f.system_volume
FROM mart.daily_branch_performance m
JOIN daily_fact f ON f.full_date = m.full_date
GROUP BY m.full_date, f.system_volume
HAVING COUNT(*) > 1
   AND MIN(m.daily_transaction_volume_usd) = f.system_volume
   AND MAX(m.daily_transaction_volume_usd) = f.system_volume
ORDER BY m.full_date DESC
LIMIT 10;

-- Q08. Số dòng bị điều kiện validation loại (chưa tính duplicate/lookup).
-- amount > 0 là quy tắc code hiện tại, không phải mọi hệ thống ngân hàng.
SELECT COUNT(*) AS raw_staging_rows,
       COUNT(*) FILTER (
           WHERE transaction_id IS NOT NULL AND account_id IS NOT NULL
             AND merchant_id IS NOT NULL AND amount_usd > 0
             AND transaction_date IS NOT NULL
       ) AS valid_by_current_filter,
       COUNT(*) - COUNT(*) FILTER (
           WHERE transaction_id IS NOT NULL AND account_id IS NOT NULL
             AND merchant_id IS NOT NULL AND amount_usd > 0
             AND transaction_date IS NOT NULL
       ) AS excluded_by_current_filter
FROM staging.stg_transactions;

-- Q09. Duplicate staging account có thể gây fan-out khi lookup customer.
SELECT account_id, COUNT(*) AS rows,
       COUNT(DISTINCT customer_id) AS distinct_customers
FROM staging.stg_accounts
GROUP BY account_id HAVING COUNT(*) > 1
ORDER BY rows DESC, account_id
LIMIT 20;

-- Q10. Ngày không có trong calendar: tạo YYYYMMDD fallback chưa giải quyết FK.
SELECT t.transaction_date::date AS missing_date, COUNT(*) AS rows
FROM staging.stg_transactions t
LEFT JOIN warehouse.dim_date d ON d.full_date = t.transaction_date::date
WHERE t.transaction_date IS NOT NULL AND d.date_key IS NULL
GROUP BY 1 ORDER BY 1;

-- Q11. Trung bình có trọng số từ mart, so với trung bình trực tiếp từ fact.
SELECT
    (SELECT ROUND(SUM(total_sales_volume_usd)
                  / NULLIF(SUM(total_transactions), 0), 2)
     FROM mart.merchant_analytics) AS weighted_average_from_mart,
    (SELECT ROUND(AVG(amount_usd), 2)
     FROM warehouse.fact_transactions) AS average_from_fact;

-- Q12. Kiểm quan hệ customer-account, bổ sung cho kiểm FK đơn thuần.
-- Chỉ so khi staging có đúng 1 dòng cho mỗi account và nguồn customer có mặt.
WITH unique_accounts AS (
    SELECT account_id, MIN(customer_id) AS customer_id
    FROM staging.stg_accounts
    GROUP BY account_id HAVING COUNT(*) = 1
)
SELECT COUNT(*) AS mismatched_customer_account_transactions
FROM warehouse.fact_transactions t
JOIN warehouse.dim_account a ON a.account_key = t.account_key
JOIN unique_accounts sa ON sa.account_id = a.account_id
JOIN warehouse.dim_customer expected ON expected.customer_id = sa.customer_id
WHERE t.customer_key IS DISTINCT FROM expected.customer_key;

-- Muốn đo plan, chạy EXPLAIN (ANALYZE, BUFFERS) trước một SELECT cụ thể.
-- ANALYZE thực thi SELECT thật; thời gian/I/O phụ thuộc DB và cache hiện tại.
ROLLBACK;
