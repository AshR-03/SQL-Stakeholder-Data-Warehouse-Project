/*
---------------------------------------
This script tests the quality of the bronze data for each table in the schema.
- Tests for Nulls, Incomplete values, Whitespace, incorrect counts/lengths, Duplicate keys, Patterns and more.
- Used to build and test the silver layer cleansing script.

Usage
- run one query at a time to test a specific unit case.
---------------------------------------
*/

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

---------------------------------------
-- crm_sales_details
---------------------------------------
SELECT * FROM bronze.crm_sales_details

-- SLS_ORD_NUM, SLS_PRD_KEY, SLS_CUST_ID
-- Check that they are joinable between tables as shown in the Data Integration Diagram.
SELECT * FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

SELECT * FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- SLS_ORDER_DT, SLS_SHIP_DT, SLS_DUE_DT
-- Check if the order date is less than the shipping or arrival date.
-- Check if the dates are in range and also correct length.
-- Predicted result : No data
-- Actual result : Columns with 0 and length < 8 in the sls_order_dt
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt 
OR LEN(sls_order_dt) != 8 OR sls_order_dt > '20500101' OR sls_order_dt < '19000101'

-- SLS_SALES, SLS_QUANITITY, SLS_PRICE
-- Check whether there are negatives in any of the columns, and also where price * quantity does not equal sales
SELECT * FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL or sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 or sls_quantity <= 0 OR sls_price <= 0


---------------------------------------
-- erp_cust_az12
---------------------------------------

SELECT * FROM bronze.erp_cust_az12
SELECT * FROM silver.crm_cust_info

-- CID
-- Check that the CID matches with the cst_key from silver.crm_cust_info so tables can be merged later on.
-- Predicted result : No data
-- Actual result : All cids with NASA substring.
SELECT cid FROM bronze.erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- BDATE
-- Check for extreme ranges of birth dates.
-- Predicted result : No data
-- Actual result : Birthdates out of extreme ranges.
SELECT * FROM bronze.erp_cust_az12
WHERE bdate < '1920-01-01' OR bdate > '2025-02-19'

-- GEN
-- Check low cardinality column for null / Incorrect values
-- Predicted result : Male, Female, N/A. 
-- Actual result: NULL, "", M, F included.
SELECT DISTINCT gen FROM bronze.erp_cust_az12;


---------------------------------------
-- erp_loc_a101
---------------------------------------

SELECT * FROM bronze.erp_loc_a101

-- CID
-- Must match with the cst_key from silver.crm_cust_info
-- Expected value : No data
-- Actual value : All data, invalid ID format.
SELECT cid FROM bronze.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- CNTRY
-- Low cardinality, check for abreviations, nulls and nothing values.
-- Predicted result : Only 1 name per country.
-- Actual result : Multiple names per country.
SELECT DISTINCT cntry FROM bronze.erp_loc_a101

-- Check correct Distinct values
SELECT DISTINCT cntry FROM (SELECT
REPLACE(cid, '-', '') AS cid,
CASE
WHEN cntry = 'DE' THEN 'Germany'
WHEN cntry = 'USA' OR cntry = 'US' THEN 'United States'
WHEN cntry IS NULL OR cntry = '' THEN 'N/A'
ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101) AS tbl
