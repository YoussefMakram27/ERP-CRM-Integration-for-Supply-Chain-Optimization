/********************************************************************************************
 Script Name   : InsertingTheDataInBronze.sql
 Description   : This script inserts data into bronze tables from staging tables using upsert approach.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-09-23
 Modified Date : <YYYY-MM-DD>  |  <Your Initials>  |  <Reason for modification>

 Target DB     : ERP_CRM
 Dependencies  : <Other scripts, tables, procedures, jobs...>
 Notes         : land data as-is, don’t trust it yet, but track everything about it.

*********************************************************************************************/
-- go is not allowed inside stored procedure
create or alter procedure Bronze.load_bronze as
begin 
	begin try

		merge Bronze.crm_customers as target
		using Staging.crm_customers as source
			on target.customer_id = source.customer_id
		when matched then
			update set 
				target.customer_name = source.customer_name,
				target.location = source.location,
				target.industry_type = source.industry_type,
				target.account_manager = source.account_manager,
				target.source_file = 'crm_customers.csv',
				target.load_datetime = GETDATE()

		when not matched by target then
			insert (customer_id, customer_name, location, industry_type, account_manager,
					source_file, load_datetime)
			values (source.customer_id, source.customer_name, source.location,
					source.industry_type, source.account_manager, 'crm_customers.csv', GETDATE());


		MERGE Bronze.crm_returns AS target
		USING Staging.crm_returns AS source
			ON target.return_id = source.return_id
		WHEN MATCHED THEN
			UPDATE SET
				target.order_id = source.order_id,
				target.sku = source.sku,
				target.returns_quantity = source.returns_quantity,
				target.return_reason = source.return_reason,
				target.return_date = source.return_date,
				target.source_file = 'crm_returns.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (return_id, order_id, sku, returns_quantity, return_reason, return_date, source_file, load_datetime)
			VALUES (source.return_id, source.order_id, source.sku, source.returns_quantity, source.return_reason,
					source.return_date, 'crm_returns.csv', GETDATE());


		MERGE Bronze.crm_sales_leads AS target
		USING Staging.crm_sales_leads AS source
			ON target.lead_id = source.lead_id
		WHEN MATCHED THEN
			UPDATE SET
				target.customer_id = source.customer_id,
				target.product_interest = source.product_interest,
				target.expected_order_quantity = source.expected_order_quantity,
				target.expected_order_date = source.expected_order_date,
				target.source_file = 'crm_sales_leads.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (lead_id, customer_id, product_interest, expected_order_quantity, expected_order_date,
					source_file, load_datetime)
			VALUES (source.lead_id, source.customer_id, source.product_interest, source.expected_order_quantity,
					source.expected_order_date, 'crm_sales_leads.csv', GETDATE());


		MERGE Bronze.crm_sales_orders AS target
		USING Staging.crm_sales_orders AS source
			ON target.order_id = source.order_id
		WHEN MATCHED THEN
			UPDATE SET
				target.customer_id = source.customer_id,
				target.sku = source.sku,
				target.order_quantity = source.order_quantity,
				target.order_date = source.order_date,
				target.order_status = source.order_status,
				target.source_file = 'crm_sales_orders.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (order_id, customer_id, sku, order_quantity, order_date, order_status, source_file, load_datetime)
			VALUES (source.order_id, source.customer_id, source.sku, source.order_quantity, source.order_date,
					source.order_status, 'crm_sales_orders.csv', GETDATE());


		MERGE Bronze.erp_inventory AS target
		USING Staging.erp_inventory AS source
			ON target.sku = source.sku AND target.warehouse_id = source.warehouse_id
		WHEN MATCHED THEN
			UPDATE SET
				target.current_stock = source.current_stock,
				target.safety_stock = source.safety_stock,
				target.inventory_last_updated = source.inventory_last_updated,
				target.source_file = 'erp_inventory.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (sku, warehouse_id, current_stock, safety_stock, inventory_last_updated, source_file, load_datetime)
			VALUES (source.sku, source.warehouse_id, source.current_stock, source.safety_stock,
					source.inventory_last_updated, 'erp_inventory.csv', GETDATE());


		MERGE Bronze.erp_inventory_transactions AS target
		USING Staging.erp_inventory_transactions AS source
			ON target.transaction_id = source.transaction_id
		WHEN MATCHED THEN
			UPDATE SET
				target.sku = source.sku,
				target.warehouse_id = source.warehouse_id,
				target.transaction_type = source.transaction_type,
				target.change_quantity = source.change_quantity,
				target.transaction_timestamp = source.transaction_timestamp,
				target.source_file = 'erp_inventory_transactions.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (transaction_id, sku, warehouse_id, transaction_type, change_quantity, transaction_timestamp,
					source_file, load_datetime)
			VALUES (source.transaction_id, source.sku, source.warehouse_id, source.transaction_type,
					source.change_quantity, source.transaction_timestamp, 'erp_inventory_transactions.csv', GETDATE());


		MERGE Bronze.erp_products AS target
		USING Staging.erp_products AS source
			ON target.sku = source.sku
		WHEN MATCHED THEN
			UPDATE SET
				target.product_name = source.product_name,
				target.product_category = source.product_category,
				target.product_price = source.product_price,
				target.supplier_id = source.supplier_id,
				target.source_file = 'erp_products.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (sku, product_name, product_category, product_price, supplier_id, source_file, load_datetime)
			VALUES (source.sku, source.product_name, source.product_category, source.product_price,
					source.supplier_id, 'erp_products.csv', GETDATE());


		MERGE Bronze.erp_purchase_orders AS target
		USING Staging.erp_purchase_orders AS source
			ON target.purchase_order_id = source.purchase_order_id
		WHEN MATCHED THEN
			UPDATE SET
				target.supplier_id = source.supplier_id,
				target.sku = source.sku,
				target.ordered_quantity = source.ordered_quantity,
				target.order_date = source.order_date,
				target.expected_delivery_date = source.expected_delivery_date,
				target.delivery_status = source.delivery_status,
				target.source_file = 'erp_purchase_orders.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (purchase_order_id, supplier_id, sku, ordered_quantity, order_date, expected_delivery_date,
					delivery_status, source_file, load_datetime)
			VALUES (source.purchase_order_id, source.supplier_id, source.sku, source.ordered_quantity,
					source.order_date, source.expected_delivery_date, source.delivery_status,
					'erp_purchase_orders.csv', GETDATE());


		MERGE Bronze.erp_suppliers AS target
		USING Staging.erp_suppliers AS source
			ON target.supplier_id = source.supplier_id
		WHEN MATCHED THEN
			UPDATE SET
				target.supplier_name = source.supplier_name,
				target.lead_time_days = source.lead_time_days,
				target.reliability_score = source.reliability_score,
				target.source_file = 'erp_suppliers.csv',
				target.load_datetime = GETDATE()

		WHEN NOT MATCHED BY TARGET THEN
			INSERT (supplier_id, supplier_name, lead_time_days, reliability_score, source_file, load_datetime)
			VALUES (source.supplier_id, source.supplier_name, source.lead_time_days, source.reliability_score,
					'erp_suppliers.csv', GETDATE());
		
	end try
	begin catch
		print '===================='
		print 'Error Occured During Loading Data into Bronze Layer'
		print 'Error Message: ' + error_message();
		print '===================='
	end catch
end

