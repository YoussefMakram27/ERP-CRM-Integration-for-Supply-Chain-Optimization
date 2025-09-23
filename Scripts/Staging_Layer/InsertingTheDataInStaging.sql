/********************************************************************************************
 Script Name   : InsertingTheDataInStaging.sql
 Description   : This script inserts the data from CSVs to Staging tables with truncate & bulk insert.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-09-23
 Modified Date : <YYYY-MM-DD>  |  <Your Initials>  |  <Reason for modification>

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/
create or alter procedure Staging.load_staging as 
begin
	begin try
		truncate table Staging.crm_customers
		bulk insert Staging.crm_customers
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_customers.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.crm_returns
		bulk insert Staging.crm_returns
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_returns.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.crm_sales_leads
		bulk insert Staging.crm_sales_leads
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_sales_leads.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.crm_sales_orders
		bulk insert Staging.crm_sales_orders
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\crm_sales_orders.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.erp_inventory
		bulk insert Staging.erp_inventory
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_inventory.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.erp_inventory_transactions
		bulk insert Staging.erp_inventory_transactions
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_inventory_transactions.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.erp_products
		bulk insert Staging.erp_products
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_products.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.erp_purchase_orders
		bulk insert Staging.erp_purchase_orders
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_purchase_orders.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);

		truncate table Staging.erp_suppliers
		bulk insert Staging.erp_suppliers
		from 'D:\Just Data\ERP_CRM\erp_crm_datasets\erp_suppliers.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			Tablock
		);
	end try
	begin catch
		print '===================='
		print 'Error Occured During Loading Data into Staging Layer'
		print 'Error Message: ' + error_message();
		print '===================='
	end catch
end

