/*
------------------------------------------------
DDL Script to create the gold layer Views.
-------------------------------------
This script will use the silver layer to create the gold layer views.

	- The gold layer contains the a final dimention and fact tables of the star schema,
	and creates analytic and report ready data for data analysts.

Usage:
	- Query the views for data analysis and reporting.
------------------------------------------------
*/


------------------------------------------------
-- CREATE CUSTOMER DIMENTION gold.dim_customers
------------------------------------------------

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	c.cst_id AS customer_id,
	c.cst_key AS customer_number,
	c.cst_firstname AS first_name,
	c.cst_lastname AS last_name,
	lo.cntry AS country,
	c.cst_marital_status AS marital_status,
	CASE
		WHEN c.cst_gndr != 'N/A' AND c.cst_gndr != ca.gen THEN c.cst_gndr
		WHEN c.cst_gndr = 'N/A' AND ca.gen IS NOT NULL THEN ca.gen
		ELSE c.cst_gndr -- Data integration with 2 table columns.
	END AS gender,
	ca.bdate AS birthdate,
	c.cst_create_date AS customer_creation_date
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 ca
ON c.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 lo
ON c.cst_key = lo.cid

------------------------------------------------
-- CREATE PRODUCT DIMENTION gold.dim_products
------------------------------------------------

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pri.prd_start_dt, pri.prd_key) AS product_key,
	pri.prd_id AS product_id,
	pri.prd_key AS product_number,
	pri.prd_nm AS product_name,
	pri.cat_id AS category_id,
	c.cat as category,
	c.subcat AS sub_category,
	c.maintenance,
	pri.prd_cost AS product_cost,
	pri.prd_line AS product_line,
	pri.prd_start_dt AS start_date
FROM silver.crm_prd_info pri
LEFT JOIN silver.erp_px_cat_g1v2 c
ON pri.cat_id = c.id
WHERE pri.prd_end_dt IS NULL

------------------------------------------------
-- CREATE SALES FACT gold.fact_sales
------------------------------------------------

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number,
	do.product_key AS product_key,
	dc.customer_key AS customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
ON sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products do
ON sd.sls_prd_key = do.product_number
