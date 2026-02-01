-------------------Gold Layer-----------------------------
/* 
Script Purpose
   This script creates views for the Gold layer in data warehouse.
   The Gold layer represents final dimension and fact tables(Star Schema)
  
   Each view performs transformations and combines data from Silver layer 
   to produce a clean, enriched and business ready dataset
  
Usage 
   The views can be queried directly for analytics and reporting

*/
--==================================================================
--Create Dimension: gold.dim_customers
-- ==================================================================
Select * from silver.crm_cust_info;
select * from silver.erp_cust_az12;
select * from silver.erp_loc_a101

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
  DROP VIEW gold.dim_customers;
GO

CREATE OR ALTER VIEW gold.dim_customers AS
select 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
	ci.cst_id AS customer_id, 
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender --- CRM is the Master for Gender Info
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,	
	ca.bdate AS birthdate, 
	ci.cst_create_date AS create_date
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

------------------------------------------
  -------------Testing Purpose ----------
------------------------------------------
-- Eliminate the gender columns 
select DISTINCT
	ci.cst_gender,
	ca.gen,
	CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender --- CRM is the Master for Gender Info
	ELSE COALESCE(ca.gen, 'n/a')
	END AS new_gen
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

select 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
where prd_end_dt IS NULL  -- Filter our all historical data

--Check quality and uniqueness 
SELECT prd_key, COUNT(*) FROM (
select 
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.cat,
	pc.subcat,
	pc.maintenance,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
where prd_end_dt IS NULL  -- Filter our all historical data
)t GROUP BY prd_key HAVING COUNT(*) > 1

-- Give friendly names and make in correct order
-- Add surrogate key for the same


/*-- VIEW 2
========================================

-- Create Dimension: gold.dim_products
========================================
*/

IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
  DROP VIEW gold.dim_product;
GO

CREATE OR ALTER VIEW gold.dim_product AS
select 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS prod_key, 
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance AS maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
where prd_end_dt IS NULL  -- Filter our all historical data


------------------------------------------
  -------------Testing Purpose ----------
------------------------------------------
Select * from gold.dim_product;

select * from gold.dim_customers;

select * from silver.crm_sales_details 

/*-- VIEW 3
========================================
Create Fact Table : gold.fact_sales
========================================
*/

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
  DROP VIEW gold.fact_sales;
GO

CREATE OR ALTER VIEW gold.fact_sales as 
select 
	sd.sls_ord_num as order_number,
	pr.prod_key,
	cu.customer_id,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as ship_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_product pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
GO
