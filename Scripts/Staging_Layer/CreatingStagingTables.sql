/********************************************************************************************
 Script Name   : CreatingStagingTables.sql
 Description   : This script creates the tables which will contains the batch data directly from CSVs.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-09-23
 Modified Date : <YYYY-MM-DD>  |  <Your Initials>  |  <Reason for modification>

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/

-- Staging schema: temporary landing for raw CSVs
-- No metadata (source_file, load_datetime) here
-- All cleansing/casting happens later into Bronze


use ERP_CRM;

if object_id ('Staging.crm_customers', 'u') is not null
	drop table Staging.crm_customers;
create table Staging.crm_customers (
	customer_id int,
	customer_name varchar(100),
	location varchar(100),
	industry_type varchar(100),
	account_manager varchar(100)
);
go

-- return_date make it varchar here not datetime because maybe it is corrupted, in silver you will make it date

if object_id ('Staging.crm_returns', 'u') is not null
	drop table Staging.crm_returns;
create table Staging.crm_returns (
	return_id int,
	order_id int,
	sku varchar(100),
	returns_quantity int,
	return_reason varchar(255),
	return_date varchar(100)
);
go

if object_id ('Staging.crm_sales_leads', 'u') is not null
	drop table Staging.crm_sales_leads;
create table Staging.crm_sales_leads (
	lead_id int,
	customer_id int,
	product_interest varchar(100),
	expected_order_quantity int,
	expected_order_date varchar(100)
);
go

if object_id ('Staging.crm_sales_orders', 'u') is not null
	drop table Staging.crm_sales_orders;
create table Staging.crm_sales_orders (
	order_id int,
	customer_id int,
	sku varchar(100),
	order_quantity int,
	order_date varchar(100),
	order_status varchar(100)
);
go

if object_id ('Staging.erp_inventory', 'u') is not null
	drop table Staging.erp_inventory;
create table Staging.erp_inventory (
	sku varchar(100),
	warehouse_id varchar(100),
	current_stock int, 
	safety_stock int,
	inventory_last_updated varchar(100)
);

if object_id ('Staging.erp_inventory_transactions', 'u') is not null
	drop table Staging.erp_inventory_transactions;
create table Staging.erp_inventory_transactions (
	transaction_id int,
	sku varchar(100),
	warehouse_id varchar(100),
	transaction_type varchar(100),
	change_quantity int,
	transaction_timestamp varchar(100)
);
go

if object_id ('Staging.erp_products', 'u') is not null
	drop table Staging.erp_products;
create table Staging.erp_products (
	sku varchar(100),
	product_name varchar(255),
	product_category varchar(100),
	product_price decimal(10,2),
	supplier_id int
);
go

if object_id ('Staging.erp_purchase_orders', 'u') is not null
	drop table Staging.erp_purchase_orders;
create table Staging.erp_purchase_orders (
	purchase_order_id int,
	supplier_id int,
	sku varchar(100),
	ordered_quantity int,
	order_date varchar(100),
	expected_delivery_date varchar(100),
	delivery_status varchar(100)
);
go

if object_id ('Staging.erp_suppliers', 'u') is not null
	drop table Staging.erp_suppliers;
create table Staging.erp_suppliers (
	supplier_id int,
	supplier_name varchar(100),
	lead_time_days varchar(50),
	reliability_score decimal(10,2)
);
go

