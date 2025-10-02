/********************************************************************************************
 Script Name   : InsertingERPDataInSilver.sql
 Description   : This script cleanse the ERP data and then insert it into silver layer tables.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-10-01
 Modified Date : <YYYY-MM-DD>  |  <Your Initials>  |  <Reason for modification>

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/

use ERP_CRM;

-- ============================
-- ERP Inventory
-- ============================
print 'Starting ERP Inventory cleanse...';

select sku,
	   warehouse_id,
	   replace(current_stock, '-', '') as current_stock,
	   replace(safety_stock, '-', '') as safety_stock,
	   case when try_convert(datetime2 ,inventory_last_updated, 23) is not null
				then convert(datetime2 ,inventory_last_updated, 23)
			when try_convert(datetime2 ,inventory_last_updated, 103) is not null
				then convert(datetime2 ,inventory_last_updated, 103)
			when try_convert(datetime2 ,inventory_last_updated, 101) is not null
				then convert(datetime2 ,inventory_last_updated, 101)
			else NULL
		end as inventory_last_updated,
		'Bronze.erp_inventory' as source_file,
		sysdatetime() as load_datetime
from Bronze.erp_inventory;

print 'Finished ERP Inventory cleanse.';

go

-- ============================
-- ERP Inventory Transactions
-- ============================
print 'Starting ERP Inventory Transactions cleanse...';

select transaction_id,
	   sku,
	   warehouse_id,
	   trim(transaction_type) as transaction_type,
	   change_quantity,
	   case when try_convert(datetime2, replace(transaction_timestamp, 'T', ' '), 23) is not null
		       then convert(datetime2, replace(transaction_timestamp, 'T', ' '), 23)
	        when try_convert(datetime2, replace(transaction_timestamp, 'T', ' '), 103) is not null
		       then convert(datetime2, replace(transaction_timestamp, 'T', ' '), 103)
			when try_convert(datetime2, replace(transaction_timestamp, 'T', ' '), 101) is not null
		       then convert(datetime2, replace(transaction_timestamp, 'T', ' '), 101)
			else NULL
	   end as transaction_timestamp,
       'Bronze.erp_inventory_transactions' as source_file,
       sysdatetime() as load_datetime
from Bronze.erp_inventory_transactions;

print 'Finished ERP Inventory Transactions cleanse.';

go

-- ============================
-- ERP Products
-- ============================
print 'Starting ERP Products cleanse...';

select sku,
	   product_name,
	   trim(product_category) as product_category,
	   cast(product_price as decimal(10,2)) as product_price,
	   supplier_id,
	   'Bronze.erp_products' as source_file,
	   sysdatetime() as load_datetime
from Bronze.erp_products;

print 'Finished ERP Products cleanse.';

go

-- ============================
-- ERP Purchase Orders
-- ============================
print 'Starting ERP Purchase Orders cleanse...';

select purchase_order_id,
	   supplier_id,
	   sku,
	   ordered_quantity,
	   case when order_date_cleaned > expected_delivery_date_cleaned
				then expected_delivery_date_cleaned
		    else order_date_cleaned
	   end as order_date,
	   case when order_date_cleaned > expected_delivery_date_cleaned
				then order_date_cleaned
			else expected_delivery_date_cleaned
	   end as expected_delivery_date,
	   delivery_status,
	   'Bronze.erp_purchase_orders' as source_file,
	   sysdatetime() as load_datetime

from (
	select purchase_order_id,
		   supplier_id,
		   sku,
		   ordered_quantity,
		   case when try_convert(datetime2 ,order_date, 23) is not null
					then convert(datetime2 ,order_date, 23)
				when try_convert(datetime2 ,order_date, 103) is not null
					then convert(datetime2 ,order_date, 103)
				when try_convert(datetime2 ,order_date, 101) is not null
					then convert(datetime2 ,order_date, 101)
				else NULL
		   end as order_date_cleaned,
		   case when try_convert(datetime2 ,expected_delivery_date, 23) is not null
					then convert(datetime2 ,expected_delivery_date, 23)
				when try_convert(datetime2 ,expected_delivery_date, 103) is not null
					then convert(datetime2 ,expected_delivery_date, 103)
				when try_convert(datetime2 ,expected_delivery_date, 101) is not null
					then convert(datetime2 ,expected_delivery_date, 101)
				else NULL
		   end as expected_delivery_date_cleaned,
		   trim(delivery_status) as delivery_status
	from Bronze.erp_purchase_orders
) f;

print 'finished ERP Purchase Orders cleanse.';

go

-- ============================
-- ERP Suppliers
-- ============================
print 'Starting ERP Suppliers cleanse...';

select supplier_id,
	   supplier_name,
	   lead_time_days,
	   reliability_score,
	   'Bronze.erp_suppliers' as source_file,
	   sysdatetime() as load_datetime
from Bronze.erp_suppliers;

print 'Finished ERP Suppliers cleanse.';

go
