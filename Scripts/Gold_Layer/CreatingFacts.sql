drop table if exists gold.fact_sales;
go

select
    row_number() over (order by sa.order_id) as sales_key,
    sa.order_id,
    cu.customer_key,
    pr.product_key,
    d.date_key,
    sa.order_quantity,
    pr.product_price,
    sa.order_quantity * pr.product_price as total_revenue,
    sa.order_status
into gold.fact_sales
from silver.crm_sales_orders as sa
left join gold.dim_customers as cu
    on sa.customer_id = cu.customer_id
left join gold.dim_product as pr
    on sa.sku = pr.sku
left join gold.dim_date as d 
    on convert(int, format(sa.order_date, 'yyyyMMdd')) = d.date_key;
go

alter table gold.fact_sales
alter column sales_key int not null;
go

alter table gold.fact_sales
add constraint pk_fact_sales primary key (sales_key);
go

alter table gold.fact_sales 
add constraint fk_fact_sales_customer 
foreign key (customer_key) references gold.dim_customers(customer_key);

alter table gold.fact_sales 
add constraint fk_fact_sales_product 
foreign key (product_key) references gold.dim_product(product_key);

alter table gold.fact_sales 
add constraint fk_fact_sales_date 
foreign key (date_key) references gold.dim_date(date_key);
go

-----------------------------------------------------

drop table if exists gold.fact_inventory;
go

select
    row_number() over (order by i.warehouse_id, i.sku, i.inventory_last_updated) as inventory_key,
    d.date_key,
    p.product_key,
    w.warehouse_key,
    i.current_stock as stock_on_hand,
    i.safety_stock,
    case 
        when i.current_stock > i.safety_stock then 1 else 0 
    end as excess_stock_flag,
    case 
        when i.current_stock = 0 then 1 else 0 
    end as stockout_flag
into gold.fact_inventory
from silver.erp_inventory as i
left join gold.dim_product as p
    on i.sku = p.sku
left join gold.dim_warehouse as w
    on i.warehouse_id = w.warehouse_id
left join gold.dim_date as d
    on convert(int, format(i.inventory_last_updated, 'yyyyMMdd')) = d.date_key;
go

alter table gold.fact_inventory
alter column inventory_key int not null;
go

alter table gold.fact_inventory
add constraint pk_fact_inventory primary key (inventory_key);
go

alter table gold.fact_inventory 
add constraint fk_fact_inventory_date 
foreign key (date_key) references gold.dim_date(date_key);

alter table gold.fact_inventory 
add constraint fk_fact_inventory_product 
foreign key (product_key) references gold.dim_product(product_key);

alter table gold.fact_inventory 
add constraint fk_fact_inventory_warehouse 
foreign key (warehouse_key) references gold.dim_warehouse(warehouse_key);
go

-----------------------------------------------------

drop table if exists gold.fact_returns;
go

select
    row_number() over (order by r.return_id) as return_key,
    dr.date_key as return_date_key,
    p.product_key,
    r.returns_quantity
into gold.fact_returns
from silver.crm_returns as r
left join gold.dim_product as p
    on r.sku = p.sku
left join gold.dim_date as dr
    on convert(int, format(r.return_date, 'yyyyMMdd')) = dr.date_key;
go

alter table gold.fact_returns
alter column return_key int not null;
go

alter table gold.fact_returns
add constraint pk_fact_returns primary key (return_key);
go

alter table gold.fact_returns 
add constraint fk_fact_returns_return_date 
foreign key (return_date_key) references gold.dim_date(date_key);

alter table gold.fact_returns 
add constraint fk_fact_returns_product 
foreign key (product_key) references gold.dim_product(product_key);
go

-----------------------------------------------------

drop table if exists gold.fact_purchases;
go

select
    row_number() over (order by p.purchase_order_id) as purchase_key,
    s.supplier_key,
    pr.product_key,
    od.date_key as order_date_key,
    ed.date_key as expected_delivery_date_key,
    p.ordered_quantity as quantity_ordered,
    p.delivery_status,
    s_lead.lead_time_days
into gold.fact_purchases
from silver.erp_purchase_orders as p
left join gold.dim_supplier as s
    on p.supplier_id = s.supplier_id
left join gold.dim_product as pr
    on p.sku = pr.sku
left join gold.dim_date as od
    on convert(int, format(p.order_date, 'yyyyMMdd')) = od.date_key
left join gold.dim_date as ed
    on convert(int, format(p.expected_delivery_date, 'yyyyMMdd')) = ed.date_key
left join silver.erp_suppliers as s_lead
    on p.supplier_id = s_lead.supplier_id;
go

alter table gold.fact_purchases
alter column purchase_key int not null;
go

alter table gold.fact_purchases
add constraint pk_fact_purchases primary key (purchase_key);
go

alter table gold.fact_purchases 
add constraint fk_fact_purchases_supplier 
foreign key (supplier_key) references gold.dim_supplier(supplier_key);

alter table gold.fact_purchases 
add constraint fk_fact_purchases_product 
foreign key (product_key) references gold.dim_product(product_key);

alter table gold.fact_purchases 
add constraint fk_fact_purchases_order_date 
foreign key (order_date_key) references gold.dim_date(date_key);

alter table gold.fact_purchases 
add constraint fk_fact_purchases_expected_delivery_date 
foreign key (expected_delivery_date_key) references gold.dim_date(date_key);
go
