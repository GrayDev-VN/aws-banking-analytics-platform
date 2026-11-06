"""
AWS Lambda Handler: Build Warehouse
===================================
Serverless function triggered after Clean & Load Staging to execute
Dimensional Transformations (Dimensions & Fact loading) on RDS PostgreSQL.
"""

import os
import json
import logging
import psycopg2

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_HOST = os.getenv("DB_HOST") or os.getenv("RDS_HOST")
DB_PORT = os.getenv("DB_PORT") or os.getenv("RDS_PORT", "5432")
DB_NAME = os.getenv("DB_NAME") or os.getenv("RDS_DATABASE", "banking_dwh")
DB_USER = os.getenv("DB_USER") or os.getenv("RDS_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD") or os.getenv("RDS_PASSWORD")

SQL_INSERT_DIMENSIONS = """
-- 1.0 External / Guest Customer
INSERT INTO warehouse.dim_customer (
    customer_id, first_name, last_name, full_name, email, city, credit_score, created_at
) VALUES (
    'UNKNOWN', 'External', 'Customer', 'External / Guest Customer', 'external-guest@bank.internal', 'Unknown City', -1, '1900-01-01'
) ON CONFLICT (customer_id) DO NOTHING;z

-- 2.0 External Account
INSERT INTO warehouse.dim_account (
    account_id, account_type, balance_usd, open_date
) VALUES (
    'UNKNOWN', 'EXTERNAL_ACCOUNT', 0.00, '1900-01-01'
) ON CONFLICT (account_id) DO NOTHING;

-- 3.0 External Card
INSERT INTO warehouse.dim_card (
    card_id, card_type, expiration_date
) VALUES (
    'UNKNOWN', 'EXTERNAL_CARD', '1900-01-01'
) ON CONFLICT (card_id) DO NOTHING;

-- 4.0 External Merchant
INSERT INTO warehouse.dim_merchant (
    merchant_id, merchant_name, city
) VALUES (
    'UNKNOWN', 'Unregistered / External Merchant', 'Unknown City'
) ON CONFLICT (merchant_id) DO NOTHING;

-- 5.0 General Branch
INSERT INTO warehouse.dim_branch (
    branch_id, branch_name, manager_name
) VALUES (
    'UNKNOWN', 'General / Online Branch', 'Unassigned'
) ON CONFLICT (branch_id) DO NOTHING;

-- 1. DIM_CUSTOMER (50,000)
WITH validated_customers AS (
    SELECT 
        TRIM(customer_id) AS customer_id,
        INITCAP(NULLIF(TRIM(first_name), '')) AS first_name,
        INITCAP(NULLIF(TRIM(last_name), '')) AS last_name,
        CASE 
            WHEN LOWER(TRIM(email)) ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' 
            THEN LOWER(TRIM(email))
            ELSE 'invalid-email@bank.internal'
        END AS email,
        INITCAP(NULLIF(TRIM(city), '')) AS city,
        CASE 
            WHEN credit_score BETWEEN 300 AND 850 THEN credit_score
            ELSE -1
        END AS credit_score,
        COALESCE(created_at, CURRENT_DATE) AS created_at
    FROM staging.stg_customers
    WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ''
)
INSERT INTO warehouse.dim_customer (
    customer_id, first_name, last_name, full_name, email, city, credit_score, created_at
)
SELECT 
    customer_id,
    COALESCE(first_name, 'Unknown') AS first_name,
    COALESCE(last_name, 'Customer') AS last_name,
    CONCAT(COALESCE(first_name, 'Unknown'), ' ', COALESCE(last_name, 'Customer')) AS full_name,
    email,
    COALESCE(city, 'Unknown City') AS city,
    credit_score,
    created_at
FROM validated_customers
ON CONFLICT (customer_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    full_name = EXCLUDED.full_name,
    email = EXCLUDED.email,
    city = EXCLUDED.city,
    credit_score = EXCLUDED.credit_score;

-- 2. DIM_ACCOUNT (75,000)
WITH validated_accounts AS (
    SELECT 
        TRIM(account_id) AS account_id,
        CASE 
            WHEN UPPER(TRIM(account_type)) IN ('CHECKING', 'SAVINGS', 'INVESTMENT', 'CREDIT') 
            THEN UPPER(TRIM(account_type))
            ELSE 'OTHER'
        END AS account_type,
        COALESCE(balance_usd, 0.00) AS balance_usd,
        COALESCE(open_date, CURRENT_DATE) AS open_date
    FROM staging.stg_accounts
    WHERE account_id IS NOT NULL AND TRIM(account_id) != ''
)
INSERT INTO warehouse.dim_account (
    account_id, account_type, balance_usd, open_date
)
SELECT 
    account_id,
    account_type,
    balance_usd,
    open_date
FROM validated_accounts
ON CONFLICT (account_id) DO UPDATE SET
    account_type = EXCLUDED.account_type,
    balance_usd = EXCLUDED.balance_usd,
    open_date = EXCLUDED.open_date;

-- 3. DIM_CARD (100,000)
WITH validated_cards AS (
    SELECT 
        TRIM(card_id) AS card_id,
        CASE 
            WHEN UPPER(TRIM(card_type)) IN ('DEBIT', 'CREDIT', 'PREPAID') 
            THEN UPPER(TRIM(card_type))
            ELSE 'STANDARD'
        END AS card_type,
        expiration_date
    FROM staging.stg_cards
    WHERE card_id IS NOT NULL AND TRIM(card_id) != ''
)
INSERT INTO warehouse.dim_card (
    card_id, card_type, expiration_date
)
SELECT 
    card_id,
    card_type,
    expiration_date
FROM validated_cards
ON CONFLICT (card_id) DO UPDATE SET
    card_type = EXCLUDED.card_type,
    expiration_date = EXCLUDED.expiration_date;

-- 4. DIM_MERCHANT (5,000)
WITH validated_merchants AS (
    SELECT 
        TRIM(merchant_id) AS merchant_id,
        COALESCE(NULLIF(TRIM(merchant_name), ''), 'Unknown Merchant') AS merchant_name,
        COALESCE(INITCAP(NULLIF(TRIM(city), '')), 'Unknown City') AS city
    FROM staging.stg_merchants
    WHERE merchant_id IS NOT NULL AND TRIM(merchant_id) != ''
)
INSERT INTO warehouse.dim_merchant (
    merchant_id, merchant_name, city
)
SELECT 
    merchant_id,
    merchant_name,
    city
FROM validated_merchants
ON CONFLICT (merchant_id) DO UPDATE SET
    merchant_name = EXCLUDED.merchant_name,
    city = EXCLUDED.city;

-- 5. DIM_BRANCH (500)
WITH validated_branches AS (
    SELECT 
        TRIM(branch_id) AS branch_id,
        COALESCE(NULLIF(TRIM(branch_name), ''), 'General Branch') AS branch_name,
        COALESCE(INITCAP(NULLIF(TRIM(manager_name), '')), 'Unassigned') AS manager_name
    FROM staging.stg_branches
    WHERE branch_id IS NOT NULL AND TRIM(branch_id) != ''
)
INSERT INTO warehouse.dim_branch (
    branch_id, branch_name, manager_name
)
SELECT 
    branch_id,
    branch_name,
    manager_name
FROM validated_branches
ON CONFLICT (branch_id) DO UPDATE SET
    branch_name = EXCLUDED.branch_name,
    manager_name = EXCLUDED.manager_name;
"""

SQL_INSERT_FACTS = """
-- 1. FACT_TRANSACTIONS (1 triệu dòng)
WITH valid_transactions AS (
    SELECT 
        t.transaction_id,
        t.account_id,
        t.merchant_id,
        t.amount_usd,
        t.transaction_date
    FROM staging.stg_transactions t
    WHERE t.transaction_id IS NOT NULL 
      AND t.account_id IS NOT NULL
      AND t.merchant_id IS NOT NULL
      AND t.amount_usd > 0
      AND t.transaction_date IS NOT NULL
),
unknown_keys AS (
    SELECT 
        (SELECT customer_key FROM warehouse.dim_customer WHERE customer_id = 'UNKNOWN') AS unk_cust_key,
        (SELECT account_key FROM warehouse.dim_account WHERE account_id = 'UNKNOWN') AS unk_acc_key,
        (SELECT card_key FROM warehouse.dim_card WHERE card_id = 'UNKNOWN') AS unk_card_key,
        (SELECT merchant_key FROM warehouse.dim_merchant WHERE merchant_id = 'UNKNOWN') AS unk_merch_key,
        (SELECT branch_key FROM warehouse.dim_branch WHERE branch_id = 'UNKNOWN') AS unk_branch_key
),
enriched_transactions AS (
    SELECT 
        vt.transaction_id,
        COALESCE(c.customer_key, uk.unk_cust_key) AS customer_key,
        COALESCE(a.account_key, uk.unk_acc_key) AS account_key,
        COALESCE(crd.card_key, uk.unk_card_key) AS card_key,
        COALESCE(m.merchant_key, uk.unk_merch_key) AS merchant_key,
        uk.unk_branch_key AS branch_key,
        COALESCE(d.date_key, TO_CHAR(vt.transaction_date, 'YYYYMMDD')::INT) AS date_key,
        vt.amount_usd,
        vt.transaction_date
    FROM valid_transactions vt
    CROSS JOIN unknown_keys uk
    LEFT JOIN staging.stg_accounts sa ON vt.account_id = sa.account_id
    LEFT JOIN warehouse.dim_customer c ON sa.customer_id = c.customer_id
    LEFT JOIN warehouse.dim_account a ON vt.account_id = a.account_id
    LEFT JOIN (
        SELECT DISTINCT ON (account_id) account_id, card_id
        FROM staging.stg_cards
        ORDER BY account_id, expiration_date DESC
    ) sc ON vt.account_id = sc.account_id
    LEFT JOIN warehouse.dim_card crd ON sc.card_id = crd.card_id
    LEFT JOIN warehouse.dim_merchant m ON vt.merchant_id = m.merchant_id
    LEFT JOIN warehouse.dim_date d ON vt.transaction_date::date = d.full_date
)
INSERT INTO warehouse.fact_transactions (
    transaction_id,
    customer_key,
    account_key,
    card_key,
    merchant_key,
    branch_key,
    date_key,
    amount_usd,
    transaction_date
)
SELECT 
    transaction_id,
    customer_key,
    account_key,
    card_key,
    merchant_key,
    branch_key,
    date_key,
    amount_usd,
    transaction_date
FROM enriched_transactions
ON CONFLICT (transaction_id) DO NOTHING;

-- 2. FACT_LOANS (30,000 dòng)
WITH valid_loans AS (
    SELECT 
        l.loan_id,
        l.customer_id,
        l.loan_amount,
        l.interest_rate,
        l.start_date
    FROM staging.stg_loans l
    WHERE l.loan_id IS NOT NULL 
      AND l.customer_id IS NOT NULL
      AND l.loan_amount > 0
      AND l.interest_rate >= 0
      AND l.start_date IS NOT NULL
),
unknown_keys AS (
    SELECT 
        (SELECT customer_key FROM warehouse.dim_customer WHERE customer_id = 'UNKNOWN') AS unk_cust_key,
        (SELECT branch_key FROM warehouse.dim_branch WHERE branch_id = 'UNKNOWN') AS unk_branch_key
),
enriched_loans AS (
    SELECT 
        vl.loan_id,
        COALESCE(c.customer_key, uk.unk_cust_key) AS customer_key,
        uk.unk_branch_key AS branch_key,
        COALESCE(d.date_key, TO_CHAR(vl.start_date, 'YYYYMMDD')::INT) AS start_date_key,
        vl.loan_amount,
        vl.interest_rate,
        vl.start_date
    FROM valid_loans vl
    CROSS JOIN unknown_keys uk
    LEFT JOIN warehouse.dim_customer c ON vl.customer_id = c.customer_id
    LEFT JOIN warehouse.dim_date d ON vl.start_date = d.full_date
)
INSERT INTO warehouse.fact_loans (
    loan_id,
    customer_key,
    branch_key,
    start_date_key,
    loan_amount,
    interest_rate,
    start_date
)
SELECT 
    loan_id,
    customer_key,
    branch_key,
    start_date_key,
    loan_amount,
    interest_rate,
    start_date
FROM enriched_loans
ON CONFLICT (loan_id) DO NOTHING;
"""

def lambda_handler(event, context):
    logger.info("Bắt đầu Warehouse Dimensional Transformation...")
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
        logger.info("Executing Insert Dimensions...")
        cursor.execute(SQL_INSERT_DIMENSIONS)
        conn.commit()

        logger.info("Executing Insert Facts...")
        cursor.execute(SQL_INSERT_FACTS)
        conn.commit()

        counts = {}
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
            counts[t] = cursor.fetchone()[0]

        logger.info(f"Warehouse build completed successfully: {counts}")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Warehouse Dimensional Transformation completed successfully",
                "warehouse_row_counts": counts
            })
        }
    except Exception as e:
        conn.rollback()
        logger.error(f"Warehouse build failed: {e}", exc_info=True)
        raise e
    finally:
        cursor.close()
        conn.close()
