/********************************************************************************************
 Script Name   : CreatingBronzeTables.sql
 Description   : This script creates Bronze tables with metadata cols.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-09-23
 Modified Date : <YYYY-MM-DD>  |  <Your Initials>  |  <Reason for modification>

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/

-- In Bronze, constraints (like PK/FK/NOT NULL) are usually skipped
-- Always be Specific, don't just make col called 'name', make it 'customer_name' so you avoid conflicts later
-- Adding the path of the source file is important to track it later if problems happened
-- Adding date of creating the table is also important
-- u: user_table, v: view, p: stored_procedure, if: inline_table (valued function)

use ERP_CRM;

if object_id ('Bronze.crm_customers', 'u') is not null
	print 'Bronze.crm_customers existed';
else
	create table Bronze.crm_customers (
		customer_id int,
		customer_name varchar(100),
		location varchar(100),
		industry_type varchar(100),
		account_manager varchar(100),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

-- return_date make it varchar here not datetime because maybe it is corrupted, in silver you will make it date

if object_id ('Bronze.crm_returns', 'u') is not null
	print 'Bronze.crm_returns existed';
else
	create table Bronze.crm_returns (
		return_id int,
		order_id int,
		sku varchar(100),
		returns_quantity int,
		return_reason varchar(255),
		return_date varchar(100),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

if object_id ('Bronze.crm_sales_leads', 'u') is not null
	print 'Bronze.crm_sales_leads existed';
else
	create table Bronze.crm_sales_leads (
		lead_id int,
		customer_id int,
		product_interest varchar(100),
		expected_order_quantity int,
		expected_order_date varchar(100),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

if object_id ('Bronze.crm_sales_orders', 'u') is not null
	print 'Bronze.crm_sales_orders existed';
else
	create table Bronze.crm_sales_orders (
		order_id int,
		customer_id int,
		sku varchar(100),
		order_quantity int,
		order_date varchar(100),
		order_status varchar(100),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

if object_id ('Bronze.erp_inventory', 'u') is not null
	print 'Bronze.erp_inventory existed';
else
	create table Bronze.erp_inventory (
		sku varchar(100),
		warehouse_id varchar(100),
		current_stock int, 
		safety_stock int,
		inventory_last_updated varchar(100),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

if object_id ('Bronze.erp_inventory_transactions', 'u') is not null
	print 'Bronze.erp_inventory_transactions existed';
else
	create table Bronze.erp_inventory_transactions (
		transaction_id int,
		sku varchar(100),
		warehouse_id varchar(100),
		transaction_type varchar(100),
		change_quantity int,
		transaction_timestamp varchar(100),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

if object_id ('Bronze.erp_products', 'u') is not null
	print 'Bronze.erp_products existed';
else
	create table Bronze.erp_products (
		sku varchar(100),
		product_name varchar(255),
		product_category varchar(100),
		product_price decimal(10,2),
		supplier_id int,

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

if object_id ('Bronze.erp_purchase_orders', 'u') is not null
	print 'Bronze.erp_purchase_orders existed';
else
	create table Bronze.erp_purchase_orders (
		purchase_order_id int,
		supplier_id int,
		sku varchar(100),
		ordered_quantity int,
		order_date varchar(100),
		expected_delivery_date varchar(100),
		delivery_status varchar(100),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

if object_id ('Bronze.erp_suppliers', 'u') is not null
	print 'Bronze.erp_suppliers existed';
else
	create table Bronze.erp_suppliers (
		supplier_id int,
		supplier_name varchar(100),
		lead_time_days int,
		reliability_score decimal(10,2),

		-- MetaData
		source_file varchar(255),
		load_datetime datetime default getdate()
	);
go

