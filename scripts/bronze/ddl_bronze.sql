/*
===========================================================================================
DDL Script: Create bronze table
===========================================================================================
This script creates tables in the bronze schema dropping  the tables if they already exists 
Run this script to reddefine the ddl structures of bronze layer tables tables 

*/


ALTER    PROCEDURE [bronze].[load_bronze] AS
BEGIN
 Declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
 BEGIN TRY
    set @batch_start_time = GETDATE();
	Print '======================================================================';
	print ' Loading bronze layer';
	Print '======================================================================';


	Print '======================================================================';
	print ' Loading crm tables';
	Print '======================================================================';

	set @start_time = GETDATE();
	print'>> truncating table bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	print '>> inserting data into  bronze.crm_prd_info';
	bulk insert bronze.crm_prd_info
	from 'C:\Users\DEgbe\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	with (
	 firstrow = 2,
	 fieldterminator = ',',
	 tablock
	);
	set @end_time = GETDATE();
	Print '>> load duration: '  +  cast(datediff(second, @start_time, @end_time) as nvarchar) +  'seconds';
    Print '================================================';


	set @start_time = GETDATE();
	print'>> truncating table bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	print '>> inserting data into  bronze.crm_sales_details';
	bulk insert bronze.crm_sales_details
	from 'C:\Users\DEgbe\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	with (
	 firstrow = 2,
	 fieldterminator = ',',
	 tablock
	);
	set @end_time = GETDATE();
	Print '>> load duration: '  +  cast(datediff(second, @start_time, @end_time) as nvarchar) +  'seconds';
	Print '================================================';




	Print '======================================================================';
	print ' Loading erp tables';
	Print '======================================================================';

	set @start_time = GETDATE();
	print'>> truncating table bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;

	print '>> inserting data into: erp_px_cat_g1v2 table'
	bulk insert bronze.erp_px_cat_g1v2
	from 'C:\Users\DEgbe\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	with (
	 firstrow = 2,
	 fieldterminator = ',',
	 tablock
	);
	set @end_time = GETDATE();
	Print '>> load duration: '  +  cast(datediff(second, @start_time, @end_time) as nvarchar) +  'seconds';


	set @start_time = GETDATE();
	print'>> truncating table bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	print '>> inserting data: into erp_cust_az12 table'
	bulk insert bronze.erp_cust_az12
	from 'C:\Users\DEgbe\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	with (
	 firstrow = 2,
	 fieldterminator = ',',
	 tablock
	);

	set @end_time = GETDATE();
	Print '>> load duration: '  +  cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
	Print '================================================';


	set @start_time = GETDATE();
	print'>> truncating table bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	print '>> inserting data into: bronze.erp_loc_a101 table'
	bulk insert bronze.erp_loc_a101
	from 'C:\Users\DEgbe\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	with (
	 firstrow = 2,
	 fieldterminator = ',',
	 tablock
	);
	set @end_time = GETDATE();
	Print '>> load duration: '  +  cast(datediff(second, @start_time, @end_time) as nvarchar) +   'seconds';
	Print '================================================';

	set @batch_end_time = GETDATE();
	Print '>> Total Batch load : '+  cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';
	Print '================================================';
 END TRY
 BEGIN CATCH
	PRINT '===================================================='
	PRINT 'ERROR OCCURED DURING LOADING OF THE BRONZE LAYER';
	PRINT 'error message' + ERROR_MESSAGE();
	print' error message' + cast (ERROR_MESSAGE() as nvarchar);

 END CATCH
END

GO


