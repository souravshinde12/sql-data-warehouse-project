/*
=============================================================
Quality Checks
=============================================================
Script Purpose:
This script performs various quality cheks for data consistency, accuracy and standarization
across the 'silver' schema. It ibcludes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields
	- Data standardization and consistency
	- Invalid date ranges and orders
	- Data consistency between related fields

Usage Notes:
	- Run these checks after data loading Silver Layer
	- Investigate and resolve any desrepancies found during the checks.
============================================================
*/


-------- Check for table 1 - bronze.crm_prd_info
-------- Check nulls or duplicates in Primary Key
Select cst_id, count(*) 
from silver.crm_cust_info 
group by cst_id 
having count(*) > 1 OR cst_id IS NULL;

Select * from bronze.crm_cust_info

Check for unwanted Spaces
Select cst_firstname from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

Select cst_lastname from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname)

Select cst_gender from silver.crm_cust_info
where cst_gender != TRIM(cst_gender)

Data Standarization  Consistency
Select distinct cst_gender from bronze.crm_cust_info

Select distinct cst_gender from silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

--=========================================================--
--=========================================================--
--=========================================================--
  
----------- Check for table 2 - bronze.crm_prd_info
select distinct id from bronze.erp_px_cat_g1v2;

select sls_prd_key FROM bronze.crm_sales_details

-------- Check nulls or duplicates in Primary Key
Select prd_id, count(*) from silver.crm_prd_info group by prd_id having count(*) > 1 or prd_id IS NULL

-----------Check for unwanted Spaces
SELECT prd_nm FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm)

----------- Check for NULLs or Negative Costs
Select prd_cost from silver.crm_prd_info WHERE prd_cost IS NULL

----------- Data Standararization and Consistency
Select distinct prd_line from silver.crm_prd_info

----------- Check for Invalid Date Orders
Select * from silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

Select prd_id, prd_start_dt,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')

select * from silver.crm_prd_info;


--=========================================================--
--=========================================================--
--=========================================================--

------- Chec for table 3 - crm_sales_details

--------Check if the order_num is having any problem
select sls_ord_num from bronze.crm_sales_details
where sls_ord_num != TRIM(sls_ord_num)

--------where sls_prd_key NOT IN (Select prd_key FROM silver.crm_prd_info)

select sls_ord_num from bronze.crm_sales_details
where sls_prd_key NOT IN (Select prd_key FROM silver.crm_prd_info)

select sls_ord_num from bronze.crm_sales_details
where sls_cust_id NOT IN (Select cst_id FROM silver.crm_cust_info)

SELECT NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt	> 20500101 OR sls_order_dt < 19000101;

SELECT NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8

--------Check invalid order dates
Select * from bronze.crm_sales_details WHERE sls_order_dt > sls_ship_dt OR sls_ord_num > sls_due_dt

---------- Check the Busines Rule = Quantity * Price
---------- Values mut not be zero and NULL

Select sls_sales as old_sls_sales,
sls_quantity, 
sls_price as old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price is NULL or sls_price <= 0
	THEN sls_sales / NULLIF(sls_quantity, 0)
ELSE sls_price
END AS sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;


--=========================================================--
--=========================================================--
--=========================================================--

------- Check for table 4 - erp_cust_az12
------- Check the overall data
select * from bronze.erp_cust_az12;

------- Check data standarization
select cid from bronze.erp_cust_az12 where cid LIKE 'NAS%'

------- Check data consistency
select distinct gen from bronze.erp_cust_az12;


--=========================================================--
--=========================================================--
--=========================================================--

------ Checks for table 5 - erp_loc_a101
------ Replace and remove - from cid
select replace(cid, '-', '') cid,
cntry FROM bronze.erp_loc_a101 

------- Data Standarization and Consistency
Select CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	ELSE TRIM(cntry)
END as cntry
FROM bronze.erp_loc_a101


--=========================================================--
--=========================================================--
--=========================================================--
  
  ------ Check for table 6 - erp_px_cat_g1v2
	-----Check for unwanted spaces
	SELECT * FROM bronze.erp_px_cat_g1v2 WHERE 
	cat != TRIM(cat)
	OR subcat != TRIM(subcat)
	OR maintenance != TRIM(maintenance)

	------Check Data Standarisation and consistency
	select Distinct cat from bronze.erp_px_cat_g1v2
	select Distinct subcat from bronze.erp_px_cat_g1v2
	select Distinct maintenance from bronze.erp_px_cat_g1v2



