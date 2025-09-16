/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-='
		PRINT 'Loading Silver Layer'
		PRINT '-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-='

		SET @batch_start_time = GETDATE()
		PRINT '----------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '----------------------------------------'



		PRINT '------------------------------------------------------'
		SET @start_time = GETDATE()
		PRINT '>>> Trancating table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>>> Inserting Data Into: silver.crm_cust_info'
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
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE UPPER(TRIM(cst_marital_status))
				WHEN 'M' THEN 'Married'
				WHEN 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status,
			CASE UPPER(TRIM(cst_gndr)) 
				WHEN 'M' THEN 'Male'
				WHEN 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag = 1
	
		SET @end_time = GETDATE()
		PRINT 'Insert Completed: Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' Seconds'
		PRINT '------------------------------------------------------'

		PRINT '------------------------------------------------------'
		SET @batch_end_time = GETDATE()
		PRINT '-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-'
		PRINT 'Loading Silver Layer is Completed'
		PRINT '    - Total Duration Time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS VARCHAR) + ' Seconds'
		PRINT '-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-'
	END TRY
	BEGIN CATCH
		PRINT '-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-'
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS VARCHAR)
		PRINT 'Error Message' + CAST (ERROR_STATE() AS VARCHAR)
		PRINT '-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-'
	END CATCH
END
GO
EXEC silver.load_silver
