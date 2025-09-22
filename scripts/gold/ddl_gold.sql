/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/


--------------------------------------------
-- Create Dimension: gold.dim_customers
--------------------------------------------

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS firstname,
ci.cst_lastname AS lastname,
CASE 
    WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
    ELSE COALESCE(caz.gen, 'n/a')
END AS gender,
ci.cst_marital_status AS marital_status,
caz.bdate AS birthdate,
ela.cntry AS country,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 caz
ON ci.cst_key = caz.cid
LEFT JOIN silver.erp_local_a101 ela
ON ci.cst_key = ela.cid
GO

--------------------------------------------
-- Create Dimension: gold.dim_products
--------------------------------------------
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER (ORDER BY poi.prd_start_dt, poi.prd_key) AS product_key,
poi.prd_id AS product_id,
poi.prd_key AS product_number,
poi.prd_nm AS product_name,
poi.prd_line AS product_line,
poi.prd_cost AS cost,
pca.cat AS category,
pca.subcat AS subcategory,
pca.maintenance AS maintenance,
poi.prd_start_dt AS start_date
FROM silver.crm_prd_info poi
LEFT JOIN silver.erp_px_cat_g1v2 pca
ON poi.cat_id = pca.id
WHERE poi.prd_end_dt IS NULL;
GO

--------------------------------------------
-- Create Dimension: gold.fact_sales
--------------------------------------------

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
     DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number ,
gdc.customer_key AS customer_key,
gdp.product_key AS product_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers gdc
ON sd.sls_cust_id = gdc.customer_id
LEFT JOIN gold.dim_products gdp
ON sd.sls_prd_key = gdp.product_number
GO
