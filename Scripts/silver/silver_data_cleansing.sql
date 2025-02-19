-- Cleansing for crm_cst_info
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE
	WHEN cst_marital_status = 'M' THEN 'Married'
    WHEN cst_marital_status = 'S' THEN 'Single'
    WHEN cst_marital_status IS NULL THEN 'n/a'
    ELSE cst_marital_status
END AS cst_marital_status,
CASE
	WHEN cst_gndr = 'M' THEN 'Male'
    WHEN cst_gndr = 'F' THEN 'Female'
    WHEN cst_gndr IS NULL THEN 'N/A'
    ELSE cst_gndr
END AS cst_gndr,
cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t WHERE flag_last = 1;

-- Cleansing for crm_prd_info
SELECT
prd_id,
prd_key,
prd_nm, 
CASE
	WHEN prd_cost IS NULL THEN '0'
	ELSE prd_cost
END AS prd_cost,
CASE
	WHEN prd_line IS NULL THEN 'N/A'
	WHEN prd_line = 'M' THEN 'Mountain'
	WHEN prd_line = 'R' THEN 'Road'
	WHEN prd_line = 'S' THEN 'Shorts'
	WHEN prd_line = 'T' THEN 'Touring'
END AS prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
