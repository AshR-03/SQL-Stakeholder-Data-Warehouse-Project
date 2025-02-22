/*
------------------------------------------
This script was used to complete the quality checks for the dimentions and facts of the gold layer in the data warehouse.
------------------------------------------
*/

------------------------------------------
-- CUSTOMER DIMENTION VIEW CHECKS
------------------------------------------

SELECT * FROM gold.dim_customers

SELECT DISTINCT gender FROM gold.dim_customers

------------------------------------------
-- SALES FACT VIEW CHECKS
------------------------------------------

SELECT * FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c
ON f.customer_id = c.customer_id
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL
