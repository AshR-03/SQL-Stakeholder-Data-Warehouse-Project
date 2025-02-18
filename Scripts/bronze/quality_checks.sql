/*
----------------------------------------------------------------
QUALITY CHECKS for BRONZE LAYER data

- Run Each Query Individually to check data has moved from CSV files correctly
----------------------------------------------------------------
*/ 


-- Ensure Counts are correct
SELECT COUNT(*) FROM bronze.crm_cust_info; -- Should return 18493
SELECT COUNT(*) FROM bronze.crm_prd_info; -- Should return 397
SELECT COUNT(*) FROM bronze.crm_sales_details; -- Should return 60398
SELECT COUNT(*) FROM bronze.erp_cust_az12; -- Should return 18484
SELECT COUNT(*) FROM bronze.erp_loc_a101; -- Should return 18484
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2; -- Should return 37

-- Ensure data has been truncated and inserted into the correct column
SELECT * FROM bronze.crm_cust_info;
SELECT * FROM bronze.crm_prd_info;
SELECT * FROM bronze.crm_sales_details;
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM bronze.erp_px_cat_g1v2;
