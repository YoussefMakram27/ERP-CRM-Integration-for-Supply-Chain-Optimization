/********************************************************************************************
 Script Name   : InsertingTheDataInBronze.sql
 Description   : This script inserts the data from CSVs to Bronze tables with truncate & bulk insert.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-09-23
 Modified Date : 2025-09-24  |  Intial 1  |  Changing the Data in the CSVs

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/

create or alter procedure Bronze.load_Bronze as 
begin
	begin try
		truncate table Bronze.crm_customers
		bulk insert Bronze.crm_customers
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_customers.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.crm_returns
		bulk insert Bronze.crm_returns
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_returns.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.crm_sales_leads
		bulk insert Bronze.crm_sales_leads
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_sales_leads.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.crm_sales_orders
		bulk insert Bronze.crm_sales_orders
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_sales_orders.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.erp_inventory
		bulk insert Bronze.erp_inventory
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_inventory.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.erp_inventory_transactions
		bulk insert Bronze.erp_inventory_transactions
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_inventory_transactions.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.erp_products
		bulk insert Bronze.erp_products
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_products.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.erp_purchase_orders
		bulk insert Bronze.erp_purchase_orders
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_purchase_orders.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Bronze.erp_suppliers
		bulk insert Bronze.erp_suppliers
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_suppliers.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);
	end try
	begin catch
		print '===================='
		print 'Error Occured During Loading Data into Bronze Layer'
		print 'Error Message: ' + error_message();
		print '===================='
	end catch
end


