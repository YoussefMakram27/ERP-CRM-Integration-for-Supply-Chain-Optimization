/********************************************************************************************
 Script Name   : InsertingCRMDataInSilver.sql
 Description   : This script cleanse the CRM data and then insert it into silver layer tables.

 Author        : Youssef M.Makram M.Osman
 Created Date  : 2025-10-01

 Target DB     : ERP_CRM
*********************************************************************************************/

create or alter procedure Silver.load_silver_CRM as
begin
	---------------------------------------------------
	-- Load CRM Customers
	---------------------------------------------------

	print '=== Starting load for CRM Customers...';

	merge Silver.crm_customers as target
	using (
		select 
			TRIM(customer_id) AS customer_id,

			nullif(trim(customer_name), '') as customer_name,

			case 
				when location in ('N/A', 'Unknown', '') then NULL
				else trim(location)
			end as location,

			trim(industry_type) AS industry_type,

			trim(account_manager) as account_manager

		from (
			select *,
				   ROW_NUMBER() over(
						partition by customer_id
						order by customer_id
				   ) flag
			from Bronze.crm_customers
		) f 
		where flag = 1 and customer_id is not null
	) as source
		on target.customer_id = source.customer_id

	when matched then
		update set
			-- you don't need to update customer_id when matched
			-- If it changes, it usually means a new record instead of an update.
			target.customer_name = source.customer_name,
			target.location = source.location,
			target.industry_type = source.industry_type,
			target.account_manager = source.account_manager

	when not matched then
		insert (customer_id, customer_name, location, industry_type, account_manager, source_file,
				load_datetime)
		values (source.customer_id, source.customer_name, source.location, source.industry_type,
				source.account_manager, 'Bronze.crm_customers.csv', sysdatetime());
	
	PRINT '>>> Finished load for CRM Customers./n';

	---------------------------------------------------
	-- Load CRM Returns
	---------------------------------------------------
	print '=== Starting load for CRM Returns...';

	merge Silver.crm_returns as target
	using (
		select 
			  trim(return_id) as return_id,
			  nullif(trim(order_id), '') as order_id,
			  nullif(trim(sku), '') as sku,

			  case 
				when isnumeric(returns_quantity) = 1 
					 and TRY_CONVERT(int, returns_quantity) >= 0
					then TRY_CONVERT(int, returns_quantity)
				else NULL
			  end as returns_quantity,

			  case 
				when return_reason IN ('N/A','Unknown','') then NULL
				else trim(return_reason)
			  end as return_reason,

			  case 
					when TRY_CONVERT(datetime2, return_date, 23) is not NULL
						then CONVERT(datetime2, return_date, 23)
					when TRY_CONVERT(datetime2, return_date, 105) is not NULL
						then CONVERT(datetime2, return_date, 105)
					when TRY_CONVERT(datetime2, return_date, 101) is not NULL
						then CONVERT(datetime2, return_date, 101)
					else NULL
			  end as return_date
		from (
			select *,
				   ROW_NUMBER() over (
					   partition by return_id
					   order by return_id
				   ) flag
			from Bronze.crm_returns
		) f
		where flag = 1
		  and return_id is not NULL
	) as source
		on target.return_id = source.return_id

	when matched then
		update set
			target.order_id = source.order_id,
			target.sku = source.sku,
			target.returns_quantity = source.returns_quantity,
			target.return_reason = source.return_reason,
			target.return_date = source.return_date

	when not matched then
		insert (return_id, order_id, sku, returns_quantity, return_reason, return_date, 
				source_file, load_datetime)
		values (source.return_id, source.order_id, source.sku, source.returns_quantity,
				source.return_reason, source.return_date, 'Bronze.crm_returns.csv', sysdatetime());
		
	print '>>> Finished load for CRM Returns.';

	---------------------------------------------------
	-- Load CRM Sales Leads
	---------------------------------------------------
	print '=== Starting UPSERT into Silver.crm_sales_leads...';

	merge Silver.crm_sales_leads as target
	using (
		select 
			trim(lead_id) AS lead_id,

			nullif(trim(customer_id), '') AS customer_id,

			case 
				when product_interest in ('', 'N/A', 'Unknown') then NULL
				else trim(product_interest)
			end as product_interest,

			case 
				when ISNUMERIC(expected_order_quantity) = 1
					 and TRY_CONVERT(int, expected_order_quantity) > 0
					then TRY_CONVERT(int, expected_order_quantity)
				else NULL
			end as expected_order_quantity,

			case 
				when TRY_CONVERT(datetime2, expected_order_date, 23) is not NULL
					 and TRY_CONVERT(datetime2, expected_order_date, 23) between '2000-01-01' and DATEADD(YEAR, 2, GETDATE())
					then CONVERT(datetime2, expected_order_date, 23)
				when TRY_CONVERT(datetime2, expected_order_date, 105) is not NULL
					 and TRY_CONVERT(datetime2, expected_order_date, 105) between '2000-01-01' and DATEADD(YEAR, 2, GETDATE())
					then CONVERT(datetime2, expected_order_date, 105)
				when TRY_CONVERT(datetime2, expected_order_date, 101) is not NULL
					 and TRY_CONVERT(datetime2, expected_order_date, 101) between '2000-01-01' and DATEADD(YEAR, 2, GETDATE())
					then CONVERT(datetime2, expected_order_date, 101)
				else NULL
			end as expected_order_date

		from (
			select *,
				   ROW_NUMBER() over (
					   partition by lead_id
					   order by lead_id
				   ) flag
			from Bronze.crm_sales_leads
		) f
		where flag = 1
		  and lead_id is not NULL
	) as source
	on target.lead_id = source.lead_id

	when matched then
		update set
			target.customer_id = source.customer_id,
			target.product_interest = source.product_interest,
			target.expected_order_quantity = source.expected_order_quantity,
			target.expected_order_date = source.expected_order_date

	when not matched then
		insert (lead_id, customer_id, product_interest, expected_order_quantity, expected_order_date, 
				source_file, load_datetime)
		values (source.lead_id, source.customer_id, source.product_interest, source.expected_order_quantity,
				source.expected_order_date, 'Bronze.crm_sales_leads.csv', sysdatetime());

	PRINT '>>> Finished UPSERT into Silver.crm_sales_leads.';

	---------------------------------------------------
	-- Load CRM Sales Orders
	---------------------------------------------------
	print '=== Starting UPSERT into Silver.crm_sales_orders...';

	merge Silver.crm_sales_orders as target
	using (
		select 
			trim(order_id) AS order_id,

			nullif(trim(customer_id), '') AS customer_id,

			nullif(trim(sku), '') AS sku,

			case 
				when ISNUMERIC(REPLACE(order_quantity, '-', '')) = 1 
					 and TRY_CONVERT(int, REPLACE(order_quantity, '-', '')) >= 0
					then TRY_CONVERT(int, REPLACE(order_quantity, '-', ''))
				else NULL
			end as order_quantity,

			case 
				when TRY_CONVERT(datetime2, order_date, 23) is not NULL
					 and TRY_CONVERT(datetime2, order_date, 23) between '2000-01-01' and DATEADD(YEAR, 1, GETDATE())
					then CONVERT(datetime2, order_date, 23)
				when TRY_CONVERT(datetime2, order_date, 105) is not NULL
					 and TRY_CONVERT(datetime2, order_date, 105) between '2000-01-01' and DATEADD(YEAR, 1, GETDATE())
					then CONVERT(datetime2, order_date, 105)
				when TRY_CONVERT(datetime2, order_date, 101) is not NULL
					 and TRY_CONVERT(datetime2, order_date, 101) between '2000-01-01' and DATEADD(YEAR, 1, GETDATE())
					then CONVERT(datetime2, order_date, 101)
				else NULL
			end as order_date,

			case 
				when trim(order_status) in ('', 'Unknown', 'N/A') then NULL
				else trim(order_status)
			end as order_status

		from (
			select *,
				   ROW_NUMBER() over (
					   partition by order_id
					   order by order_id
				   ) flag
			from Bronze.crm_sales_orders
		) f
		where flag = 1
		  and order_id is not NULL
	) as source
	on target.order_id = source.order_id

	when matched then
		update set
			target.customer_id = source.customer_id,
			target.sku = source.sku,
			target.order_quantity = source.order_quantity,
			target.order_date = source.order_date,
			target.order_status = source.order_status

	when not matched then
		insert (order_id, customer_id, sku, order_quantity, order_date, order_status, 
				source_file, load_datetime)
		values (source.order_id, source.customer_id, source.sku, source.order_quantity, 
				source.order_date, source.order_status, 'Bronze.crm_sales_orders.csv', sysdatetime());

	PRINT '>>> Finished UPSERT into Silver.crm_sales_orders.';

end