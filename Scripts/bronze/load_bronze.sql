/*
----------------------------------------------------------
This script uses TRUNCATE and BULK INSERT to insert the CSV raw data into the bronze layer of the Data Warehouse.
Uses the "as-is" model and does not apply any data transformations.
Logs Individual Truncate and Insert queries and logs the total time taken to load the bronze layer.

Parameters:
 - None.

Stored Procedure Usage:
   EXEC bronze.load_bronze;
----------------------------------------------------------
*/

----------------------------
-- BULK IMPORT CRM DATA
----------------------------

-- Store the load bronze script in a procedure
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		DECLARE @StartTime DATE, @EndTime DATE, @TableStartTime DATE, @TableEndTime DATE;

		SET @StartTime = GETDATE();
		SET @TableStartTime = GETDATE();

		PRINT '>> Truncating crm_cust_info.'

		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Bulk Inserting into crm_cust_info.'

		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\Ashley\Documents\GitHubRepos\SQL Data Warehouse Project\Datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @TableEndTime = GETDATE();

		PRINT '>> Time To Complete crm_cust_info Trucates + Insertions: ' + CAST(DATEDIFF(second, @TableStartTime, @TableEndTime) AS NVARCHAR(50));

		--------------------------------------------------------------------------------

		SET @TableStartTime = GETDATE();

		PRINT '>> Truncating crm_prd_info.'

		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Bulk Inserting into crm_prd_info.'

		BULK INSERT bronze.crm_prd_info 
		FROM 'C:\Users\Ashley\Documents\GitHubRepos\SQL Data Warehouse Project\Datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @TableEndTime = GETDATE();

		PRINT '>> Time To Complete crm_prd_info Trucates + Insertions: ' + CAST(DATEDIFF(second, @TableStartTime, @TableEndTime) AS NVARCHAR(50));

		--------------------------------------------------------------------------------

		SET @TableStartTime = GETDATE();

		PRINT '>> Truncating crm_sales_details.'

		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Bulk Inserting into crm_sales_details.'

		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\Ashley\Documents\GitHubRepos\SQL Data Warehouse Project\Datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @TableEndTime = GETDATE();

		PRINT '>> Time To Complete crm_sales_details Trucates + Insertions: ' + CAST(DATEDIFF(second, @TableStartTime, @TableEndTime) AS NVARCHAR(50));

		SET @TableStartTime = GETDATE();

		----------------------------
		-- BULK IMPORT ERP DATA
		----------------------------

		PRINT '>> Truncating erp_cust_az12.'

		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Bulk Inserting into erp_cust_az12.'

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Ashley\Documents\GitHubRepos\SQL Data Warehouse Project\Datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @TableEndTime = GETDATE();

		PRINT '>> Time To Complete erp_cust_az12 Trucates + Insertions: ' + CAST(DATEDIFF(second, @TableStartTime, @TableEndTime) AS NVARCHAR(50));

		--------------------------------------------------------------------------------

		SET @TableStartTime = GETDATE();

		PRINT '>> Truncating erp_loc_a101.'

		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Bulk Inserting into erp_loc_a101.'

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Ashley\Documents\GitHubRepos\SQL Data Warehouse Project\Datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @TableEndTime = GETDATE();

		PRINT '>> Time To Complete erp_loc_a101 Trucates + Insertions: ' + CAST(DATEDIFF(second, @TableStartTime, @TableEndTime) AS NVARCHAR(50));

		--------------------------------------------------------------------------------

		SET @TableStartTime = GETDATE();

		PRINT '>> Truncating erp_px_cat_g1v2.'

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Bulk Inserting into erp_px_cat_g1v2.'

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Ashley\Documents\GitHubRepos\SQL Data Warehouse Project\Datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @TableEndTime = GETDATE();

		PRINT '>> Time To Complete erp_px_cat_g1v2 Trucates + Insertions: ' + CAST(DATEDIFF(second, @TableStartTime, @TableEndTime) AS NVARCHAR(50));

		--------------------------------------------------------------------------------

		SET @EndTime = GETDATE();

		-- Show the final amount of time to complete the whole truncate and insertion process to the bronze layer.
		PRINT '>> Time To Complete CRM and ERP Trucates + Insertions: ' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR(50));
	END TRY
	BEGIN CATCH
		PRINT '--------------------------------------------------------'
		PRINT 'There was an error when loading the bronze layer';
		PRINT ERROR_MESSAGE();
		PRINT ERROR_NUMBER();
		PRINT '--------------------------------------------------------'
	END CATCH
END
