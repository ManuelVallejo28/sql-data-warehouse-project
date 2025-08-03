/* 
=====================================================================================
Stored Procedure : Load Bronze Layer (Source-->Bronze)
=====================================================================================
Script Purpose:
  This stored procedure loads data into the 'bronze' schema from external CSV files 
  It performs the following actions:
    -Truncates the bronze tables before loading data.
    -Uses the 'Bulk Insert' command to load from CSV files to bronze tables.

Parameters:
    None,
    This stored procedure does not accept any parameters or return any values. 

Usage Example:
  EXEC bronze.load_bronze;
====================================================================================
*/


EXEC bronze.load_bronze
CREATE OR ALTER PROCEDURE bronze.load_bronze AS   --Se define un procedimiento que puede ser creado o modificado
BEGIN
	
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;  --Se utilizan para medir cuánto tarda cada carga.

	
	BEGIN TRY --Aquí se intentan ejecutar todas las cargas. Si ocurre un error, se salta al CATCH.
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================================';
		PRINT 'loading Bronze Layer';
		PRINT '=========================================================================';
		PRINT 'Loading CRM tables';
		PRINT '=========================================================================';
		
		SET @start_time = GETDATE(); --Se mide cuánto tiempo tardó esta carga en segundos.
		PRINT '>>Truncating the Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;  --we r making the table empty and next w start loading from scratch, Esto elimina todos los registros rápidamente sin dejar logs (más eficiente que DELETE).    
	
		PRINT '>>Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info --Esta instrucción carga el archivo CSV directamente en la tabla.
	
		FROM 'C:\Users\jvall\OneDrive\Escritorio\DataWareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (                                         ----how to handle our file
			FIRST_ROW = 2, --Ignora la primera fila(encabezado)
			FIELDTERMINATOR = ',', --Separador de columnas
			TABLOCK		--LOCKING THE entire table during loading it 
		);
		
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>============'
		--SELECT COUNT(*) FROM bronze.crm_cust_info





		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;  --we r making the table empty and next w start loading from scratch    

		PRINT '>>Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\jvall\OneDrive\Escritorio\DataWareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (                                         ----how to handle our file
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK		--LOCKING THE entire table during loading it 
	
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>============'
		--SELECT COUNT(*) FROM bronze.crm_prd_info





		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;  --we r making the table empty and next w start loading from scratch    

		PRINT '>>Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\jvall\OneDrive\Escritorio\DataWareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (                                         ----how to handle our file
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK		--LOCKING THE entire table during loading it 
	
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>============'
		--SELECT COUNT(*) FROM bronze.crm_sales_details





		SET @start_time = GETDATE();
		PRINT '>>Truncating the Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;  --we r making the table empty and next w start loading from scratch    

		PRINT '>>Inserting data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\jvall\OneDrive\Escritorio\DataWareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (                                         ----how to handle our file
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK		--LOCKING THE entire table during loading it 
	
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>============'
		--SELECT COUNT(*) FROM bronze.erp_cust_az12





		SET @end_time = GETDATE();
		PRINT '>>Truncating the Table: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;  --we r making the table empty and next w start loading from scratch    

		PRINT '>>Inserting data into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\jvall\OneDrive\Escritorio\DataWareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'WITH (                                        ----how to handle our file
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK		--LOCKING THE entire table during loading it 
	
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>============'
		--SELECT COUNT(*) FROM bronze.erp_loc_a101




		SET @end_time = GETDATE();
		PRINT '>>truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;  --we r making the table empty and next w start loading from scratch    

		PRINT '>>inserting Table: bronze.erp_px_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\jvall\OneDrive\Escritorio\DataWareHouse\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (                                         ----how to handle our file
			FIRST_ROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK		--LOCKING THE entire table during loading it 
	
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>============'
		

		SET @batch_end_time = GETDATE();
		PRINT'-----------'
		PRINT 'lOAding Bronze Layer is COmpleted';
		PRINT '   -Total Load Duration ' + CAST(DATEDIFF(SECOND, @batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT'-----------------'

	
	
	
	END TRY 
	BEGIN CATCH
		PRINT '==========='
		PRINT 'ERROR OCURRED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR message' + ERROR_MESSAGE();  --ERROR_MESSAGE() – descripción del error.
		PRINT 'ERROR message' + CAST(ERROR_NUMBER() AS NVARCHAR); --ERROR_NUMBER() – código de error.
		PRINT 'ERROR message' + CAST(ERROR_STATE() AS NVARCHAR); --ERROR_STATE() – estado del error.
		PRINT '==========='
		--TRACK ETL DURATION-->Helps to identify bottlenecks, optimize performance, monitor trends, detect issues
	END CATCH
END



/*Todo está dentro del BEGIN ... END del procedimiento.

Dentro de ese BEGIN, hay:

Un bloque BEGIN TRY ... END TRY.

Un bloque BEGIN CATCH ... END CATCH.

Dentro del bloque TRY, hay varios sub-bloques que:

Declaran el inicio del tiempo.

Truncan la tabla.

Insertan desde archivo.

Calculan la duración.



Nivel 0: CREATE PROCEDURE
Nivel 1: BEGIN (del procedimiento)
Nivel 2:   BEGIN TRY
Nivel 3:     Bloque para una tabla (SET, TRUNCATE, BULK INSERT)
Nivel 2:   END TRY
Nivel 2:   BEGIN CATCH
Nivel 3:     Captura de errores
Nivel 2:   END CATCH
Nivel 1: END (del procedimiento)


Ejemplo:
CREATE PROCEDURE ejemplo AS
BEGIN
    BEGIN TRY
        -- Código propenso a fallar
        TRUNCATE TABLE alguna_tabla;
        BULK INSERT alguna_tabla FROM 'archivo.csv' ...
    END TRY

    BEGIN CATCH
        PRINT 'Ocurrió un error: ' + ERROR_MESSAGE();
    END CATCH
END
