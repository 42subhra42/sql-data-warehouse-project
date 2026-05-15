/*
===================================================================================
Quality Checks
===================================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization across the 'silver' schema. It includes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fields.

Usage Notes:
	- Run these checks after data loading Silver Layer.
	- Investigate and resolve any discrepancies found during the checks.
===================================================================================
*/

-- Check for nulls or duplicate in Primary key
-- Expectation: No Result
select
cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null

-- Check for unwanted Spaces
-- Expectation: No Results
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

-- Data Standardization and Consistency
select distinct cst_gndr
from bronze.crm_cust_info

----------------------------------------------------------------

-- Check for nulls or duplicate in Primary key
-- Expectation: No Result
select
prd_id,
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null

-- Check for unwanted Spaces
-- Expectation: No Results
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

-- Data Standardization and Consistency
select distinct prd_line
from bronze.crm_prd_info

-- Check for Invalid Date Orders
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select *
from silver.crm_prd_info

alter table silver.crm_prd_info alter column prd_end_dt date

----------------------------------------------------------------

-- Check for Invalid Dates
select
nullif(sls_order_dt,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
or len(sls_order_dt) != 8
or sls_order_dt > 20500101
or sls_order_dt < 19000101

-- Check for Invalid Date Orders
select
*
from  bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

-- Check Data Consistency: Between Sales, Quantity, and Price
-- >> Sales = Quantity * Price
-- >> Values must not be null, zero or negative

select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price

select *
from silver.crm_sales_details

----------------------------------------------------------------

-- Identify Out-of-Range Dates
select distinct
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

-- Data Standardization and Consistency
select distinct gen
from bronze.erp_cust_az12

select *
from silver.erp_cust_az12

----------------------------------------------------------------

-- Data standardization and Consistency
select distinct cntry
from bronze.erp_loc_a101
order by cntry

select *
from silver.erp_loc_a101

----------------------------------------------------------------

-- Check for Unwanted Spaces 
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

-- Data Standardization and Consistency
select distinct
maintenance
from bronze.erp_px_cat_g1v2

select *
from silver.erp_px_cat_g1v2
