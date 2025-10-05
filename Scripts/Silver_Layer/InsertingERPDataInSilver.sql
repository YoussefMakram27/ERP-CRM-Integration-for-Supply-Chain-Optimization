/********************************************************************************************
 Script Name   : InsertingERPDataInSilver.sql
 Description   : Cleanse the ERP data and then merge it into silver layer tables.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-10-01
 
 Target DB     : ERP_CRM
*********************************************************************************************/

create or alter procedure Silver.load_silver_ERP as
begin
-- ============================
-- erp_inventory
-- ============================
print 'starting erp_inventory cleanse and load...';

	merge Silver.erp_inventory as target
	using (
		select sku,
			   warehouse_id,
			   case 
					when isnumeric(replace(current_stock,'-',''))=1 
					then cast(replace(current_stock,'-','') as int) 
					else 0 
			   end as current_stock,

			   case 
					when isnumeric(replace(safety_stock,'-',''))=1 
					then cast(replace(safety_stock,'-','') as int) 
					else 0 
			   end as safety_stock,

			   case 
					when try_convert(datetime2, inventory_last_updated, 23) is not null
					then convert(datetime2, inventory_last_updated, 23)
					when try_convert(datetime2, inventory_last_updated, 103) is not null
					then convert(datetime2, inventory_last_updated, 103)
					when try_convert(datetime2, inventory_last_updated, 101) is not null
					then convert(datetime2, inventory_last_updated, 101)
					else null 
			   end as inventory_last_updated

		from Bronze.erp_inventory
		where sku is not null and warehouse_id is not null
	) as source
		on target.sku = source.sku and target.warehouse_id = source.warehouse_id
	when matched then
		update set 
				target.current_stock = source.current_stock,
				target.safety_stock = source.safety_stock,
				target.inventory_last_updated = source.inventory_last_updated

	when not matched then
		insert (sku, warehouse_id, current_stock, safety_stock, inventory_last_updated,
				source_file, load_datetime)
		values (source.sku, source.warehouse_id, source.current_stock, source.safety_stock,
				source.inventory_last_updated, 'Bronze.erp_inventory.csv', sysdatetime());

	print 'finished erp_inventory load.';

	-- ============================
	-- erp_inventory_transactions
	-- ============================
	print 'starting erp_inventory_transactions cleanse and load...';

	merge Silver.erp_inventory_transactions as target
	using (
		select transaction_id,
			   sku,
			   warehouse_id,
			   trim(transaction_type) as transaction_type,

			   case 
					when change_quantity = 0 
					then null 
					else change_quantity 
			   end as change_quantity,

			   case 
					when try_convert(datetime2, replace(transaction_timestamp,'T',' '), 23) is not null
					then convert(datetime2, replace(transaction_timestamp,'T',' '), 23)
					when try_convert(datetime2, replace(transaction_timestamp,'T',' '), 103) is not null
					then convert(datetime2, replace(transaction_timestamp,'T',' '), 103)
					when try_convert(datetime2, replace(transaction_timestamp,'T',' '), 101) is not null
					then convert(datetime2, replace(transaction_timestamp,'T',' '), 101)
					else null 
			   end as transaction_timestamp

		from Bronze.erp_inventory_transactions
		where transaction_id is not null
	) as source
		on target.transaction_id = source.transaction_id

	when matched then
		update set 
				target.sku = source.sku,
				target.warehouse_id = source.warehouse_id,
				target.transaction_type = source.transaction_type,
				target.change_quantity = source.change_quantity,
				target.transaction_timestamp = source.transaction_timestamp

	when not matched then
		insert (transaction_id, sku, warehouse_id, transaction_type, change_quantity,
				transaction_timestamp, source_file, load_datetime)
		values (source.transaction_id, source.sku, source.warehouse_id, source.transaction_type,
				source.change_quantity, source.transaction_timestamp, 
				'Bronze.erp_inventory_transactions.csv', sysdatetime());

	print 'finished erp_inventory_transactions load.';

	-- ============================
	-- erp_products
	-- ============================
	print 'starting erp_products cleanse and load...';

	merge Silver.erp_products as target
	using (
		select sku,
			   trim(replace(product_name,'"','')) as product_name,
			   trim(product_category) as product_category,

			   case 
					when try_cast(product_price as decimal(10,2)) is not null 
					and cast(product_price as decimal(10,2)) > 0
					then cast(product_price as decimal(10,2)) else null 
			   end as product_price,

			   supplier_id

		from Bronze.erp_products
		where sku is not null
	) as source
		on target.sku = source.sku
	when matched then
		update set 
				target.product_name = source.product_name,
				target.product_category = source.product_category,
				target.product_price = source.product_price,
				target.supplier_id = source.supplier_id

	when not matched then
		insert (sku, product_name, product_category, product_price, supplier_id,
				source_file, load_datetime)
		values (source.sku, source.product_name, source.product_category, source.product_price,
				source.supplier_id, 'Bronze.erp_products.csv', sysdatetime());

	print 'finished erp_products load.';

	-- ============================
	-- erp_purchase_orders
	-- ============================
	print 'starting erp_purchase_orders cleanse and load...';

	merge Silver.erp_purchase_orders as target
	using (
		select purchase_order_id,
			   supplier_id,
			   sku,
			   case 
					when isnumeric(ordered_quantity)=1 and cast(ordered_quantity as int) > 0
					then cast(ordered_quantity as int) 
					else null 
			   end as ordered_quantity,

			   case 
					when order_date_cleaned <= expected_delivery_date_cleaned
					then order_date_cleaned 
					else expected_delivery_date_cleaned 
			   end as order_date,

			   case 
					when expected_delivery_date_cleaned >= order_date_cleaned
					then expected_delivery_date_cleaned 
					else order_date_cleaned 
			   end as expected_delivery_date,

			   trim(delivery_status) as delivery_status

		from (
			select purchase_order_id,
				   supplier_id,
				   sku,
				   ordered_quantity,
				   case when try_convert(datetime2, order_date, 23) is not null then convert(datetime2, order_date, 23)
						when try_convert(datetime2, order_date, 103) is not null then convert(datetime2, order_date, 103)
						when try_convert(datetime2, order_date, 101) is not null then convert(datetime2, order_date, 101)
						else null end as order_date_cleaned,
				   case when try_convert(datetime2, expected_delivery_date, 23) is not null then convert(datetime2, expected_delivery_date, 23)
						when try_convert(datetime2, expected_delivery_date, 103) is not null then convert(datetime2, expected_delivery_date, 103)
						when try_convert(datetime2, expected_delivery_date, 101) is not null then convert(datetime2, expected_delivery_date, 101)
						else null end as expected_delivery_date_cleaned,

				   trim(delivery_status) as delivery_status
			from Bronze.erp_purchase_orders
		) f
		where purchase_order_id is not null
	) as source
		on target.purchase_order_id = source.purchase_order_id
	when matched then
		update set 
				target.supplier_id = source.supplier_id,
				target.sku = source.sku,
				target.ordered_quantity = source.ordered_quantity,
				target.order_date = source.order_date,
				target.expected_delivery_date = source.expected_delivery_date,
				target.delivery_status = source.delivery_status

	when not matched then
		insert (purchase_order_id, supplier_id, sku, ordered_quantity, order_date, 
				expected_delivery_date, delivery_status, source_file, load_datetime)
		values (source.purchase_order_id, source.supplier_id, source.sku, source.ordered_quantity,
				source.order_date, source.expected_delivery_date, source.delivery_status,
				'Bronze.erp_purchase_orders.csv', sysdatetime());

	print 'finished erp_purchase_orders load.'

	-- ============================
	-- erp_suppliers
	-- ============================
	print 'starting erp_suppliers cleanse and load...';

	merge Silver.erp_suppliers as target
	using (
		select supplier_id,
			   trim(supplier_name) as supplier_name,
			   case when try_cast(lead_time_days as int) is not null and cast(lead_time_days as int) >= 0
					then cast(lead_time_days as int) else null end as lead_time_days,

			   reliability_score

		from Bronze.erp_suppliers
		where supplier_id is not null
	) as source
		on target.supplier_id = source.supplier_id
	when matched then
		update set 
				target.supplier_name = source.supplier_name,
				target.lead_time_days = source.lead_time_days,
				target.reliability_score = source.reliability_score

	when not matched then
		insert (supplier_id, supplier_name, lead_time_days, reliability_score, source_file, load_datetime)
		values (source.supplier_id, source.supplier_name, source.lead_time_days,
				source.reliability_score, 'Bronze.erp_suppliers.csv', sysdatetime());

	print 'finished erp_suppliers load.';

end