
drop table if exists Gold.dim_date;
GO

with dates AS (
    select CAST('2020-01-01' AS date) AS date_value
    UNION ALL
    select DATEADD(day, 1, date_value)
    from dates
    WHERE DATEADD(day, 1, date_value) <= '2025-12-31'
)
select 
    convert(int, format(date_value, 'yyyyMMdd')) as date_key,
	datename(month, date_value) as month_name,
	datename(weekday, date_value) as day_name,
	case
		when datepart(weekday, date_value) in (1,7) then 1 else 0
	end as is_weekend
into Gold.dim_date
from dates
option (MAXRECURSION 0);  
GO

alter table Gold.dim_date
alter column date_key int NOT NULL;
go

alter table Gold.dim_date
add constraint PK_dim_date primary key (date_key);
go

--------------------------------------------------

drop table if exists Gold.dim_customers;
go

select 
	row_number() over(order by customer_id) as customer_key,
	customer_id,
	customer_name,
	location,
	industry_type,
	account_manager

into Gold.dim_customers
from Silver.crm_customers;
go

alter table Gold.dim_customers
alter column customer_key int not null
go

alter table Gold.dim_customers
add constraint PK_dim_customers primary key (customer_key);
go

--------------------------------

drop table if exists Gold.dim_product;
GO

select
    ROW_NUMBER() OVER (ORDER BY sku) AS product_key,  
    sku,                                              
    product_name,
    product_category,
    product_price,
    supplier_id
into Gold.dim_product
from Silver.erp_products;
GO

alter table Gold.dim_product
alter column product_key INT NOT NULL;
GO

alter table Gold.dim_product
add constraint PK_dim_product PRIMARY KEY (product_key);
GO

--------------------------

drop table if exists Gold.dim_supplier;
GO

select
    ROW_NUMBER() OVER (ORDER BY supplier_id) AS supplier_key,  -- surrogate key
    supplier_id,
    supplier_name AS supplier_name,
    lead_time_days,
    reliability_score
into Gold.dim_supplier
from Silver.erp_suppliers;
GO

alter table Gold.dim_supplier
alter column supplier_key INT NOT NULL;
GO

alter table Gold.dim_supplier
add constraint PK_dim_supplier PRIMARY KEY (supplier_key);
GO

---------------------------------------------

drop table if exists Gold.dim_warehouse;
GO

select
    ROW_NUMBER() OVER (ORDER BY warehouse_id) AS warehouse_key,  
    warehouse_id
into Gold.dim_warehouse
from (
    select DISTINCT warehouse_id
    from Silver.erp_inventory
) AS w;
GO

alter table Gold.dim_warehouse
alter column warehouse_key INT NOT NULL;
GO

alter table Gold.dim_warehouse
add constraint PK_dim_warehouse PRIMARY KEY (warehouse_key);
GO

