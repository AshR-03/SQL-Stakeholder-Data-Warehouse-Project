-- Gold views.

-- CUSTOMER VIEW
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
ca.bdate,
c.cst_create_date
FROM silver.crm_cust_info c
LEFT JOIN silver.erp_cust_az12 ca
ON c.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 lo
ON c.cst_key = lo.cid
