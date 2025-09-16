/*
-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-
Create Database And Schemas
-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-

Script Purpose: 
	This Script creates a new database with name: 'DWH_Project' after checking if it already exist.
	If it exists, the database will be dropped and re-created. Also, the script creates schemas with names: 'bronze', 'silver', gold.


WARNING: 
	Running this script will drop the entire 'DWH_Project' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate database.

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH_Project')
 BEGIN
	DROP DATABASE DWH_Project;
 END
GO

-- Create Database

CREATE DATABASE DWH_Project;
GO

-- Create Schemas

USE DWH_Project;
GO

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


