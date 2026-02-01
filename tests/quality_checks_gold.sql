-------------------Gold Layer Quality Checks-----------------------------
/* 
Script Purpose
   This script performs quality checks to validate the intregrity, consistecy
  and accuracy of Gold Layer. Tese checks ensure:
  - Uniqueness of surrogate keys in dimension tables.
  - Referential integrity between fact and dimension tables.
  - Validation of relationship in the data model for analytical purposes.

Usage 
  - Run these checks after data loading Silver Layer.
  - Investigate and resolve any discrepancies found during the checks. 
*/
--==================================================================
--Checks for : gold.dim_customers
-- ==================================================================
Select * from silver.crm_cust_info;
select * from silver.erp_cust_az12;
select * from silver.erp_loc_a101
  
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

/*
========================================
-- Checks for: gold.dim_products
========================================
*/

Select * from gold.dim_product;

select * from gold.dim_customers;

select * from silver.crm_sales_details 

/*
========================================
-- Checks for: gold.fact_sales
========================================
*/

Select * from gold.facte_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.cutoomer_key
WHERE c.customer_key IS NULL
