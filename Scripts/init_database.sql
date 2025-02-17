/*
---------------------------------------------------------
CREATE  THE DATABASE AND BRONZE, SILVER AND GOLD SCHEMAS
---------------------------------------------------------

- This script will create a new database named 'DataWarehouse'. Will first check if the database already exists, and if so will delete the database and recreate an empty one with 3 schemas.

WARNING:
	Running this script whilst the database already exists will destroy the database.
	All data will be deleted permanently.
*/

USE master;
GO

-- If the data Warehouse already exists then drop it and create a new one
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create the new data Warehouse
CREATE DATABASE DataWarehouse;
GO

-- Set Use to DataWarehouse
USE DataWarehouse;
GO

-- Create the Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
