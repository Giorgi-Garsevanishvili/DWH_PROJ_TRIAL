/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after loading the Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT 
cst_id,
COUNT (*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


-- Check for Unwanted Spaces
-- Expectation: No Results

SELECT
cst_firstname,
cst_lastname,
cst_key
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 
OR cst_lastname != TRIM(cst_lastname) 
OR cst_key != TRIM(cst_key)


-- Data Standardization & Consistency

SELECT DISTINCT 
cst_marital_status,
cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL


-- Check for Unwanted Spaces
-- Expectation: No Results

SELECT
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM (prd_nm)


-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results

SELECT
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results

SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt




-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today

-- Data Standardization & Consistency

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results

-- Data Standardization & Consistency
