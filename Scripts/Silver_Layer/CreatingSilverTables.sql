/********************************************************************************************
 Script Name   : CreatingSilverTables.sql
 Description   : This script creates Silver tables & cast the cols to the suitable data types.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-09-25
 Modified Date : <YYYY-MM-DD>  |  <Your Initials>  |  <Reason for modification>

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/
-- datetime2 is better than datetime
-- it is better to make pk in silver layer
-- sysdatetime() is better with datetime2 than get_date()
-- select name from sys.key_constraints where type = 'PK' list primary keys

use ERP_CRM;

if object_id ('Silver.crm_customers', 'u') is not null
	drop table Silver.crm_customers;

create table Silver.crm_customers (
	customer_id varchar(50),
	customer_name varchar(50),
	location varchar(50),
	industry_type varchar(50),
	account_manager varchar(100),

	-- MetaData
	source_file varchar(100),
	load_datetime datetime2 default sysdatetime(),

);
go


if object_id ('Silver.crm_returns', 'u') is not null
	drop table Silver.crm_returns;

create table Silver.crm_returns (
	return_id varchar(50),
	order_id varchar(50),
	sku varchar(50),
	returns_quantity int,
	return_reason varchar(100),
	return_date datetime2,

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),

);
go

if object_id ('Silver.crm_sales_leads', 'u') is not null
	drop table Silver.crm_sales_leads;

create table Silver.crm_sales_leads (
	lead_id varchar(50),
	customer_id varchar(50),
	product_interest varchar(50),
	expected_order_quantity int,
	expected_order_date datetime2,

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),

);
go

if object_id ('Silver.crm_sales_orders', 'u') is not null
	drop table Silver.crm_sales_orders;

create table Silver.crm_sales_orders (
	order_id varchar(50),
	customer_id varchar(50),
	sku varchar(50),
	order_quantity int,
	order_date datetime2,
	order_status varchar(50),

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),

);
go

if object_id ('Silver.erp_inventory', 'u') is not null
	drop table Silver.erp_inventory;

create table Silver.erp_inventory (
	sku varchar(50),
	warehouse_id varchar(50),
	current_stock int, 
	safety_stock int,
	inventory_last_updated datetime2,

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),

);
go

if object_id ('Silver.erp_inventory_transactions', 'u') is not null
	drop table Silver.erp_inventory_transactions;

create table Silver.erp_inventory_transactions (
	transaction_id varchar(50),
	sku varchar(50),
	warehouse_id varchar(50),
	transaction_type varchar(50),
	change_quantity int,
	transaction_timestamp datetime2,

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),

);
go

if object_id ('Silver.erp_products', 'u') is not null
	drop table Silver.erp_products;

create table Silver.erp_products (
	sku varchar(50),
	product_name varchar(255),
	product_category varchar(50),
	product_price decimal(10,2),
	supplier_id varchar(50),

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),

);
go

if object_id ('Silver.erp_purchase_orders', 'u') is not null
	drop table Silver.erp_purchase_orders;

create table Silver.erp_purchase_orders (
	purchase_order_id varchar(50),
	supplier_id varchar(50),
	sku varchar(50),
	ordered_quantity int,
	order_date datetime2,
	expected_delivery_date datetime2,
	delivery_status varchar(50),

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),
	
);
go

if object_id ('Silver.erp_suppliers', 'u') is not null
	drop table Silver.erp_suppliers;

create table Silver.erp_suppliers (
	supplier_id varchar(50),
	supplier_name varchar(100),
	lead_time_days int,
	reliability_score decimal(10,2),

	-- MetaData
	source_file varchar(255),
	load_datetime datetime2 default sysdatetime(),

);
go

