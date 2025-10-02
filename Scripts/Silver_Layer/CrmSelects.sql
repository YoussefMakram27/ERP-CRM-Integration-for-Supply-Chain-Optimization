/********************************************************************************************
 Script Name   : InsertingCRMDataInSilver.sql
 Description   : This script cleanse the CRM data and then insert it into silver layer tables.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-10-01
 Modified Date : <YYYY-MM-DD>  |  <Your Initials>  |  <Reason for modification>

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/

USE ERP_CRM;

---------------------------------------------------
-- Load CRM Customers
---------------------------------------------------

PRINT '=== Starting load for CRM Customers...';

SELECT 
	TRIM(customer_id) AS customer_id,
	customer_name,
	location,
	TRIM(industry_type) AS industry_type,
	account_manager,
	'Bronze.crm_customers.csv' AS source_file,
	SYSDATETIME() AS load_datetime
FROM (
	SELECT *,
		   ROW_NUMBER() OVER(
				PARTITION BY customer_id
				ORDER BY customer_id
		   ) flag
	FROM Bronze.crm_customers
) f 
WHERE flag = 1

PRINT '>>> Finished load for CRM Customers.';

GO

---------------------------------------------------
-- Load CRM Returns
---------------------------------------------------
PRINT '=== Starting load for CRM Returns...';

SELECT 
	  return_id,
      order_id,
      sku,
      returns_quantity,
      TRIM(return_reason) AS return_reason,
      CASE 
			WHEN TRY_CONVERT(datetime2, return_date, 23) IS NOT NULL
				THEN CONVERT(datetime2, return_date, 23)
			WHEN TRY_CONVERT(datetime2, return_date, 105) IS NOT NULL
				THEN CONVERT(datetime2, return_date, 105)
			WHEN TRY_CONVERT(datetime2, return_date, 101) IS NOT NULL
				THEN CONVERT(datetime2, return_date, 101)
			ELSE NULL
	  END AS return_date,
	  'Bronze.crm_returns.csv' AS source_file,
	  SYSDATETIME() AS load_datetime
FROM Bronze.crm_returns;

PRINT '>>> Finished load for CRM Returns.';

GO

---------------------------------------------------
-- Load CRM Sales Leads
---------------------------------------------------
PRINT '=== Starting load for CRM Sales Leads...';

SELECT 
	lead_id,
	customer_id,
	product_interest,
	expected_order_quantity,
	CASE 
		WHEN TRY_CONVERT(datetime2, expected_order_date, 23) IS NOT NULL
			THEN CONVERT(datetime2, expected_order_date, 23)
		WHEN TRY_CONVERT(datetime2, expected_order_date, 105) IS NOT NULL
			THEN CONVERT(datetime2, expected_order_date, 105)
		WHEN TRY_CONVERT(datetime2, expected_order_date, 101) IS NOT NULL
			THEN CONVERT(datetime2, expected_order_date, 101)
		ELSE NULL
	END AS expected_order_date,
	'Bronze.crm_sales_leads.csv' AS source_file,
	SYSDATETIME() AS load_datetime
FROM Bronze.crm_sales_leads;

PRINT '>>> Finished load for CRM Sales Leads.';

GO

---------------------------------------------------
-- Load CRM Sales Orders
---------------------------------------------------
PRINT '=== Starting load for CRM Sales Orders...';

SELECT 
	order_id,
	customer_id,
	sku,
	REPLACE(order_quantity, '-', '') AS order_quantity,
	CASE 
		WHEN TRY_CONVERT(datetime2, order_date, 23) IS NOT NULL
			THEN CONVERT(datetime2, order_date, 23)
		WHEN TRY_CONVERT(datetime2, order_date, 105) IS NOT NULL
			THEN CONVERT(datetime2, order_date, 105)
		WHEN TRY_CONVERT(datetime2, order_date, 101) IS NOT NULL
			THEN CONVERT(datetime2, order_date, 101)
		ELSE NULL
	END AS order_date,
	TRIM(order_status) AS order_status,
	'Bronze.crm_sales_orders' AS source_file,
	SYSDATETIME() AS load_datetime
FROM Bronze.crm_sales_orders;

PRINT '>>> Finished load for CRM Sales Orders.';

GO

PRINT '=== All CRM data loads into Silver completed successfully. ===';
