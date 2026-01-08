PURPOSE - 
Create database named as datawarehouse.The scripts set up 3 schemas withon the database 'bronze', 'silver', 'gold'

USE Master
GO

CREATE DATABASE DataWarehouse;

USE DataWarehouse

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO

