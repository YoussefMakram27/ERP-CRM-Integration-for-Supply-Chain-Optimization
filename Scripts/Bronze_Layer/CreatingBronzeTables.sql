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
	drop table Bronze.crm_customers;

create table Bronze.crm_customers (
	customer_id varchar(100),
	customer_name varchar(100),
	location varchar(100),
	industry_type varchar(100),
	account_manager varchar(100),
);
go

-- return_date make it varchar here not datetime because maybe it is corrupted, in silver you will make it date

if object_id ('Bronze.crm_returns', 'u') is not null
	drop table Bronze.crm_returns;

create table Bronze.crm_returns (
	return_id varchar(50),
	order_id varchar(50),
	sku varchar(100),
	returns_quantity int,
	return_reason varchar(255),
	return_date varchar(100),
);
go

if object_id ('Bronze.crm_sales_leads', 'u') is not null
	drop table Bronze.crm_sales_leads;

create table Bronze.crm_sales_leads (
	lead_id varchar(100),
	customer_id varchar(100),
	product_interest varchar(100),
	expected_order_quantity varchar(100),
	expected_order_date varchar(100),
);
go

if object_id ('Bronze.crm_sales_orders', 'u') is not null
	drop table Bronze.crm_sales_orders;

create table Bronze.crm_sales_orders (
	order_id varchar(50),
	customer_id varchar(100),
	sku varchar(100),
	order_quantity varchar(100),
	order_date varchar(100),
	order_status varchar(100),
);
go

if object_id ('Bronze.erp_inventory', 'u') is not null
	drop table Bronze.erp_inventory;

create table Bronze.erp_inventory (
	sku varchar(100),
	warehouse_id varchar(100),
	current_stock int, 
	safety_stock int,
	inventory_last_updated varchar(100),
);
go

if object_id ('Bronze.erp_inventory_transactions', 'u') is not null
	drop table Bronze.erp_inventory_transactions;

create table Bronze.erp_inventory_transactions (
	transaction_id varchar(50),
	sku varchar(100),
	warehouse_id varchar(100),
	transaction_type varchar(100),
	change_quantity int,
	transaction_timestamp varchar(100),
);
go

if object_id ('Bronze.erp_products', 'u') is not null
	drop table Bronze.erp_products;

create table Bronze.erp_products (
	sku varchar(100),
	product_name varchar(255),
	product_category varchar(100),
	product_price varchar(100),
	supplier_id varchar(100),
);
go

if object_id ('Bronze.erp_purchase_orders', 'u') is not null
	drop table Bronze.erp_purchase_orders;

create table Bronze.erp_purchase_orders (
	purchase_order_id varchar(100),
	supplier_id varchar(100),
	sku varchar(100),
	ordered_quantity varchar(100),
	order_date varchar(100),
	expected_delivery_date varchar(100),
	delivery_status varchar(100),
);
go

if object_id ('Bronze.erp_suppliers', 'u') is not null
	drop table Bronze.erp_suppliers;

create table Bronze.erp_suppliers (
	supplier_id varchar(100),
	supplier_name varchar(100),
	lead_time_days varchar(100),
	reliability_score varchar(100),
);
go

