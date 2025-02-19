/* 
------------------------------------
Data Cleaning and transformations for the Silver Layer
- This script uses TRUNCATE and INSERT to insert the transformed date into the silver tables.
- Transformations include data normalisation, standardisation, removing whitespace, removing NULL, data enrichment.
 
Example Usage
- EXEC silver.load_silver
------------------------------------
*/

-- Store the load bronze script in a procedure
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
		)
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



		-- Cleansing and Inserting for crm_prd_info
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		CASE
			WHEN prd_cost IS NULL THEN '0'
			ELSE prd_cost
		END AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN  NULL THEN 'N/A'
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
		END AS prd_line,
		CAST (prd_start_dt AS DATE) AS prd_start_dt,
		CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info



		-- Cleansing for crm_sales_details
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales != sls_quantity * ABS(sls_price) OR sls_sales IS NULL OR sls_sales <= 0 THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price != sls_sales / sls_quantity OR sls_price IS NULL OR sls_price <= 0 THEN sls_sales / sls_quantity
		ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details


		-- Cleansing and inserting into erp_cust_az_12
		INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
		)
		SELECT 
		REPLACE(cid, 'NAS', '') AS cid,
		CASE WHEN
			bdate < '1930-01-01' OR bdate > GETDATE() THEN NULL
		ELSE bdate
		END AS bdate,
		CASE
			WHEN gen IS NULL THEN 'N/A'
			WHEN gen = 'F' THEN 'Female'
			WHEN gen = '' THEN 'N/A'
			WHEN gen = 'M' THEN 'Male'
			ELSE gen
		END AS gen
		FROM bronze.erp_cust_az12


		-- Cleansing and inserting into erp_loc_101
		INSERT INTO silver.erp_loc_a101 (
		cid,
		cntry
		)
		SELECT
		REPLACE(cid, '-', '') AS cid,
		CASE
		WHEN cntry = 'DE' THEN 'Germany'
		WHEN cntry = 'USA' OR cntry = 'US' THEN 'United States'
		WHEN cntry IS NULL OR cntry = '' THEN 'N/A'
		ELSE cntry
		END AS cntry
		FROM bronze.erp_loc_a101


		-- Inserting into erp_loc_101

		INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT * FROM bronze.erp_px_cat_g1v2
	END TRY
	BEGIN CATCH 
		PRINT '--------------------------------------------------------'
		PRINT 'There was an error when loading the silver layer';
		PRINT ERROR_MESSAGE();
		PRINT ERROR_NUMBER();
		PRINT '--------------------------------------------------------'
	END CATCH
END
