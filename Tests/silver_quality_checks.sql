-- Detect quality issues within the bronze data for the silver layer

---------------------------------------
-- crm_cst_info
---------------------------------------

-- PRIMARY KEY
-- Check duplicate and Null values
SELECT 
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- CST_FIRSTNAME, CST_LASTNAME
-- Check for leading / trailing spaces.
SELECT cst_firstname, cst_lastname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname)

-- CST_MARITAL_STATUS, CST_GNDR
-- Check low cardinality values and standardize them
SELECT DISTINCT
cst_marital_status
FROM bronze.crm_cust_info

---------------------------------------
-- crm_prd_info
---------------------------------------

-- PRIMARY KEY
-- Check duplicate and Null values
-- Expected to have no data.
-- Actual Result: Multiple primary Key instances and Null values.
SELECT 
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

-- PRD_KEY
-- Check the format of all product keys are consistant
-- Predicted Result : No data
-- Actual Result : No data
SELECT
prd_key
FROM bronze.crm_prd_info
WHERE prd_key NOT LIKE '[A-Z][A-Z]-[A-Z][A-Z]-[A-Z][A-Z]-[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]%'

-- PRD_NM
-- Check the format of the product names, ensure that no whitespaces are there or null values
-- Predicted Result : No data
-- Actual Result : No data
SELECT 
prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm) OR prd_nm IS NULL

-- PRD_COST
-- Check that the cost is not null and not negative
-- Predicted Result : No data
-- Actual Result : Null values
SELECT
prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- PRD_LINE
-- Check the product line's distinct values and normalise the column
-- Predicted Result : 4 values
-- Result : 5 values
SELECT DISTINCT
prd_line
FROM bronze.crm_prd_info

-- PRD_START_DT, PRD_END_DT
-- Check that the start dates are always < the end dates. Check that the products end/start dates for historical date line up with each other.
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt
------
SELECT
*
FROM bronze.crm_prd_info
WHERE prd_key = 'CL-CA-CA-1098'
