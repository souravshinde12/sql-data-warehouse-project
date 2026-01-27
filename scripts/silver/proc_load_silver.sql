/*
==================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
==================================================
Script Purpose : 
This stored procedure performs the ETL (Extract, Transform, Load) process to 
populate the 'silver' schema tables from 'bronze' schema

Actions Performed :
- Truncates silver tables
- Inserts transformed and cleaned data from Bronze into Silver tables

Parameters:
None
This stored procedure does not accept any parameter or return any values.

Usage Example:
EXEC Silver.load_silver
=====================================================
*/

CREATE OR ALTER PROCEDURE silver.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_layer_start_time DATETIME, @bronze_layer_end_time DATETIME
	BEGIN TRY
	SET @bronze_layer_start_time = GETDATE();
	PRINT('==========================================')
	PRINT('LOADING BRONZE LAYER')
	PRINT('==========================================')

	PRINT('------------------------------------------')
	PRINT('Loading CRM Tables')
	PRINT('------------------------------------------')

		SET @start_time = GETDATE();
		PRINT' >> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info;

		PRINT' >> Inserting Date Into : silver.crm_cust_info'
		BULK INSERT silver.crm_cust_info
		FROM 'E:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>------------------------';
	
		SET @start_time = GETDATE();
		PRINT' >> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		BULK INSERT silver.crm_prd_info
		FROM 'E:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>------------------------';
	
		SET @start_time = GETDATE();
		PRINT' >> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		BULK INSERT silver.crm_sales_details
		FROM 'E:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>------------------------';

	PRINT('------------------------------------------')
	PRINT('Loading ERP Tables')
	PRINT('------------------------------------------')

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_cust_az12;
		BULK INSERT silver.erp_cust_az12
		FROM 'E:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>------------------------';


		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_loc_a101;
		BULK INSERT silver.erp_loc_a101
		FROM 'E:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		BULK INSERT silver.erp_px_cat_g1v2
		FROM 'E:\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
	PRINT '================================'
	PRINT 'Bronze Layer Loading Completed'
	PRINT '================================'
	SET @bronze_layer_end_time = GETDATE();
	PRINT 'Total Time for Loading Bronze Layer: ' + CAST(DATEDIFF(second, @bronze_layer_start_time, @bronze_layer_end_time) AS NVARCHAR) + 'seconds'
	END TRY
BEGIN CATCH
	PRINT'===================================================='
	PRINT'ERROR OCCURURED DURING BRONZE LAYER'
	PRINT'Error Message' + ERROR_MESSAGE();
	PRINT'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT'====================================================' 
END CATCH
END 


EXEC silver.load_bronze 


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE()
	PRINT'===================================================='
	PRINT'Loading Silver Layer'
	PRINT'===================================================='

	PRINT'----------------------------------------------------'
	PRINT'Loading CRM Tables';
	PRINT'----------------------------------------------------'

	-- Table 1 Operations - crm_cust_info
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';

	-- Insert values in crm_cust_info
	INSERT INTO silver.crm_cust_info(
	cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_create_date)
	SELECT cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
		 WHEN UPPER(cst_marital_status)  = 'M' THEN 'Married'
		 ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(cst_gender) = 'F' THEN 'Female'
		 WHEN UPPER(cst_gender)  = 'M' THEN 'Male'
		 ELSE 'n/a'
	END cst_gender,
	cst_create_date
	from (
	select *, row_number() over(partition by cst_id ORDER BY cst_create_date DESC) as flag_last
	from bronze.crm_cust_info WHERE cst_id IS NOT NULL
	)t 
	where flag_last = 1
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>> ---------------'

---------- Check for table 1 - bronze.crm_prd_info
---------- Check nulls or duplicates in Primary Key
--Select cst_id, count(*) 
--from silver.crm_cust_info 
--group by cst_id 
--having count(*) > 1 OR cst_id IS NULL;

--Select * from bronze.crm_cust_info

--Check for unwanted Spaces
--Select cst_firstname from silver.crm_cust_info
--where cst_firstname != TRIM(cst_firstname)

--Select cst_lastname from silver.crm_cust_info
--where cst_lastname != TRIM(cst_lastname)

--Select cst_gender from silver.crm_cust_info
--where cst_gender != TRIM(cst_gender)

--Data Standarization  Consistency
--Select distinct cst_gender from bronze.crm_cust_info

--Select distinct cst_gender from silver.crm_cust_info

--SELECT * FROM silver.crm_cust_info

	PRINT'===================================================='
	PRINT'===================================================='
	PRINT'===================================================='

	-- Table 2 Operations - crm_prd_info
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inserting Data Into: silver.crm_prd_info';

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
	Select prd_id,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost, 0) AS prd_cost,
	CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	ELSE 'n/a'
	END AS prd_line,
	CAST (prd_start_dt AS DATE) AS prd_start_dt,
	CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)AS prd_end_dt
	from bronze.crm_prd_info
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>> ---------------';

------------- Check for table 2 - bronze.crm_prd_info
--select distinct id from bronze.erp_px_cat_g1v2;

--select sls_prd_key FROM bronze.crm_sales_details

---------- Check nulls or duplicates in Primary Key
--Select prd_id, count(*) from silver.crm_prd_info group by prd_id having count(*) > 1 or prd_id IS NULL

-------------Check for unwanted Spaces
--SELECT prd_nm FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm)

------------- Check for NULLs or Negative Costs
--Select prd_cost from silver.crm_prd_info WHERE prd_cost IS NULL

------------- Data Standararization and Consistency
-- Select distinct prd_line from silver.crm_prd_info

------------- Check for Invalid Date Orders
--Select * from silver.crm_prd_info
--WHERE prd_end_dt < prd_start_dt

--Select prd_id, prd_start_dt,
--LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
--FROM bronze.crm_prd_info WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

--select * from silver.crm_prd_info;

	PRINT'===================================================='
	PRINT'===================================================='
	PRINT'===================================================='

	-- Table 3 Operations - crm_sales_details
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inserting Data Into: silver.crm_sales_details';

	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price,
	dwh_create_date
	)

	select * from silver.crm_sales_details;
	SELECT 
	sls_ord_num ,
	sls_prd_key,
	sls_cust_id ,
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
		 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
		 THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price is NULL or sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
	END AS sls_price
	from bronze.crm_sales_details  
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>> ---------------'

--------- Chec for table 3 - crm_sales_details

----------Check if the order_num is having any problem
--select sls_ord_num from bronze.crm_sales_details
--where sls_ord_num != TRIM(sls_ord_num)

----------where sls_prd_key NOT IN (Select prd_key FROM silver.crm_prd_info)

--select sls_ord_num from bronze.crm_sales_details
--where sls_prd_key NOT IN (Select prd_key FROM silver.crm_prd_info)

--select sls_ord_num from bronze.crm_sales_details
--where sls_cust_id NOT IN (Select cst_id FROM silver.crm_cust_info)

--SELECT NULLIF(sls_order_dt, 0) sls_order_dt
--FROM bronze.crm_sales_details WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt	> 20500101 OR sls_order_dt < 19000101;

--SELECT NULLIF(sls_order_dt, 0) sls_order_dt
--FROM bronze.crm_sales_details WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

----------Check invalid order dates
--Select * from bronze.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_ord_num > sls_due_dt

------------ Check the Busines Rule = Quantity * Price
------------ Values mut not be zero and NULL

--Select sls_sales as old_sls_sales,
--sls_quantity, 
--sls_price as old_sls_price,
--CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	-- THEN sls_quantity * ABS(sls_price)
	-- ELSE sls_sales
--END AS sls_sales,
--CASE WHEN sls_price is NULL or sls_price <= 0
	-- THEN sls_sales / NULLIF(sls_quantity, 0)
--ELSE sls_price
--END AS sls_price
--from bronze.crm_sales_details
--where sls_sales != sls_quantity * sls_price
--OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
--OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0
--ORDER BY sls_sales, sls_quantity, sls_price;

	PRINT'===================================================='
	PRINT'===================================================='
	PRINT'===================================================='

	-- Table 4 Operations - erp_cust_az12
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inserting Data Into: silver.erp_cust_az12';

	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
	select 
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid
	END cid,
	CASE WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END bdate,
	CASE WHEN UPPER(TRIM(gen)) = 'M' OR UPPER(TRIM(gen)) = 'Male' THEN 'Male'
		WHEN UPPER(TRIM(gen)) = 'F' OR UPPER(TRIM(gen)) = 'Female' THEN 'Female'
		ELSE 'N/A'
	END gen
	from bronze.erp_cust_az12
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>> ---------------';

--------- Check for table 4 - erp_cust_az12
--------- Check the overall data
--select * from bronze.erp_cust_az12;

--------- Check data standarization
--select cid from bronze.erp_cust_az12 where cid LIKE 'NAS%'

--------- Check data consistency
--select distinct gen from bronze.erp_cust_az12;

	PRINT'===================================================='
	PRINT'===================================================='
	PRINT'===================================================='


	-- Table 5 Operations - erp_loc_a101
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inserting Data Into: silver.erp_loc_a101';


	INSERT INTO silver.erp_loc_a101
	(cid, cntry)
	select replace(cid, '-', '') cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(cntry)
	END as cntry 
	FROM bronze.erp_loc_a101 
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>> ---------------'

	select * from bronze.erp_px_cat_g1v2

	select * from silver.crm_prd_info

-------- Checks for table 5 - erp_loc_a101
-------- Replace and remove - from cid
--select replace(cid, '-', '') cid,
--cntry FROM bronze.erp_loc_a101 

--------- Data Standarization and Consistency
--Select CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
--	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
--	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
--	ELSE TRIM(cntry)
--END as cntry
--FROM bronze.erp_loc_a101


	PRINT'===================================================='
	PRINT'===================================================='
	PRINT'===================================================='

	-- Table 6 Operations - erp_px_cat_g1v2
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table : silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';


	Insert into silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
	select id, cat, subcat, maintenance from bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
	PRINT'>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + 'seconds';
	PRINT '>> ---------------'

	SET @batch_end_time = GETDATE();
	PRINT '====================================='
	PRINT'Loading Silver Layer is completed'
	PRINT'Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds'
	PRINT'======================================='


	-------Check for unwanted spaces
	--SELECT * FROM bronze.erp_px_cat_g1v2 WHERE 
	--cat != TRIM(cat)
	--OR subcat != TRIM(subcat)
	--OR maintenance != TRIM(maintenance)

	--------Check Data Standarisation and consistency
	--select Distinct cat from bronze.erp_px_cat_g1v2
	--select Distinct subcat from bronze.erp_px_cat_g1v2
	--select Distinct maintenance from bronze.erp_px_cat_g1v2


	END TRY
	BEGIN CATCH 
		PRINT'===================================================='
		PRINT'ERROR OCCURURED DURING BRONZE LAYER'
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'====================================================' 
	END CATCH
END


EXEC silver.load_silver;

