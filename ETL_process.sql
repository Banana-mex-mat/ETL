CREATE SCHEMA test; 

-- SELECT * FROM test.ft_balance_f;

-- UPDATE test.ft_balance_f
-- SET "BALANCE_OUT" = 111111.11
-- WHERE "ACCOUNT_RK" = 24656;

-- SELECT * FROM test.ft_balance_f 
-- WHERE "BALANCE_OUT" = 111111.11;

INSERT INTO ds.ft_balance_f (
    "ON_DATE"
	, "ACCOUNT_RK"
	, "CURRENCY_RK"
	, "BALANCE_OUT"
)
SELECT 
    to_date(fb."ON_DATE", 'DD.MM.YYYY') AS "ON_DATE"
	, fb."ACCOUNT_RK"
	, fb."CURRENCY_RK"
	, fb."BALANCE_OUT"
FROM test.ft_balance_f fb
WHERE fb."ACCOUNT_RK" IS NOT NULL
    AND fb."CURRENCY_RK" IS NOT NULL;

INSERT INTO ds.ft_posting_f (
    "OPER_DATE"
	, "CREDIT_ACCOUNT_RK"
	, "DEBET_ACCOUNT_RK"
	, "CREDIT_AMOUNT"
	, "DEBET_AMOUNT"
)
SELECT 
    to_date(fb."OPER_DATE", 'DD-MM-YYYY') AS "ON_DATE"
	, fb."CREDIT_ACCOUNT_RK"
	, fb."DEBET_ACCOUNT_RK"
	, fb."CREDIT_AMOUNT"
	, fb."DEBET_AMOUNT"
FROM test.ft_posting_f fb
WHERE fb."CREDIT_ACCOUNT_RK" IS NOT NULL
  AND fb."DEBET_ACCOUNT_RK" IS NOT NULL;

INSERT INTO ds.md_account_d (
    "DATA_ACTUAL_DATE"
	, "DATA_ACTUAL_END_DATE"
	, "ACCOUNT_RK"
	, "ACCOUNT_NUMBER"
	, "CHAR_TYPE"
	, "CURRENCY_RK"
	, "CURRENCY_CODE"
)
SELECT 
    fb."DATA_ACTUAL_DATE"::date,  
    fb."DATA_ACTUAL_END_DATE"::date,
    fb."ACCOUNT_RK"::int,  
    fb."ACCOUNT_NUMBER"::varchar(20),  
    fb."CHAR_TYPE"::varchar(1),
    fb."CURRENCY_RK"::int,  
    fb."CURRENCY_CODE"::int 
FROM test.md_account_d fb
WHERE fb."DATA_ACTUAL_DATE" IS NOT NULL
  AND fb."DATA_ACTUAL_END_DATE" IS NOT NULL
  AND fb."ACCOUNT_RK" IS NOT NULL
  AND fb."ACCOUNT_NUMBER" IS NOT NULL
  AND fb."CHAR_TYPE" IS NOT NULL
  AND fb."CURRENCY_RK" IS NOT NULL
  AND fb."CURRENCY_CODE" IS NOT NULL;

INSERT INTO ds.md_currency_d (
    "CURRENCY_RK"
	, "DATA_ACTUAL_DATE"
	, "DATA_ACTUAL_END_DATE"
	, "CURRENCY_CODE"
	, "CODE_ISO_CHAR"
)
SELECT 
    fb."CURRENCY_RK"
	, fb."DATA_ACTUAL_DATE"::date
	, fb."DATA_ACTUAL_END_DATE"::date
	, fb."CURRENCY_CODE"::varchar(3)
	, fb."CODE_ISO_CHAR"::varchar(3)
FROM test.md_currency_d fb
WHERE fb."CURRENCY_RK" IS NOT NULL
  AND fb."DATA_ACTUAL_DATE" IS NOT NULL;

INSERT INTO ds.md_exchange_rate_d (
    "DATA_ACTUAL_DATE"
	, "DATA_ACTUAL_END_DATE"
	, "CURRENCY_RK"
	, "REDUCED_COURCE"
	, "CODE_ISO_NUM"
)
SELECT 
    fb."DATA_ACTUAL_DATE"::date
	, fb."DATA_ACTUAL_END_DATE"::date
	, fb."CURRENCY_RK" 
	, fb."REDUCED_COURCE"::double precision, 
	 fb."CODE_ISO_NUM"::varchar(3)
FROM test.md_exchange_rate_d fb
WHERE fb."DATA_ACTUAL_DATE" IS NOT NULL
  AND fb."CURRENCY_RK" IS NOT NULL;

--проверка на дубликаты
SELECT "DATA_ACTUAL_DATE", "CURRENCY_RK", COUNT(*)
FROM test.md_exchange_rate_d
GROUP BY "DATA_ACTUAL_DATE", "CURRENCY_RK"
HAVING COUNT(*) > 1;

--удаляем дубликаты
WITH duplicates AS (
    SELECT ctid, 
           ROW_NUMBER() OVER (PARTITION BY "DATA_ACTUAL_DATE", "CURRENCY_RK" ORDER BY ctid) AS rn
    FROM test.md_exchange_rate_d
)
DELETE FROM test.md_exchange_rate_d
WHERE ctid IN (
    SELECT ctid 
    FROM duplicates 
    WHERE rn > 1
);

INSERT INTO ds.md_ledger_account_s (
    "CHAPTER"
	, "CHAPTER_NAME"
	, "SECTION_NUMBER"
	, "SECTION_NAME"
	, "SUBSECTION_NAME"
	, "LEDGER1_ACCOUNT"
	, "LEDGER1_ACCOUNT_NAME"
	, "LEDGER_ACCOUNT"
	, "LEDGER_ACCOUNT_NAME"
	, "CHARACTERISTIC"
	, "START_DATE"
	, "END_DATE"
)
SELECT 
    fb."CHAPTER"::char(1)
	, fb."CHAPTER_NAME"::varchar(16)
	, fb."SECTION_NUMBER"::int 
	, fb."SECTION_NAME"::varchar(22)
	, fb."SUBSECTION_NAME"::varchar(21)
	, fb."LEDGER1_ACCOUNT"::int 
	, fb."LEDGER1_ACCOUNT_NAME"::varchar(47) 
	, fb."LEDGER_ACCOUNT"::int 
	, fb."LEDGER_ACCOUNT_NAME"::varchar(153)
	, fb."CHARACTERISTIC"::char(1)
	, fb."START_DATE"::date
	, fb."END_DATE"::date
FROM test.md_ledger_account_s fb
WHERE fb."LEDGER_ACCOUNT" IS NOT NULL
  AND fb."START_DATE" IS NOT NULL;

