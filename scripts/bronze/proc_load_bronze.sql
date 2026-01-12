/*
===============================================================================
Stored Procedure : Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose :
This stored procedure loads data into 'bronze' schema from external CSV files.
It performs following actions :
- Truncates bronze tables before loading data
- Uses 'BULK INSERT' command to ,oad data from csv files to bronze tables.

Parameters:
None (This stored procedure does not accept any parameters or return any values.

Usage Examples:
EXEC bronze.load_bronze
=================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_layer_start_time DATETIME, @bronze_layer_end_time DATETIME
	BEGIN TRY
	  SET @bronze_layer_start_time = GETDATE();
	  PRINT('==========================================')
	  PRINT('LOADIN BRONZE LAYER')
	  PRINT('==========================================')

	  PRINT('------------------------------------------')
	  PRINT('Loading CRM Tables')
	  PRINT('------------------------------------------')

	  SET @start_time = GETDATE();
	  PRINT' >> Truncating Table: bronze.crm_cust_info'
	  TRUNCATE TABLE bronze.crm_cust_info;
	  PRINT' >> Inserting Date Into : bronze.crm_cust_info'
	  BULK INSERT bronze.crm_cust_info
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
		PRINT' >> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
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
		PRINT' >> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
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
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
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
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
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
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
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


EXEC bronze.load_bronze 
