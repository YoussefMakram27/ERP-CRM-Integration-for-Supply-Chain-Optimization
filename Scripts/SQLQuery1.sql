use ERP_CRM;

select count(*) counter
from Bronze.crm_customers



go

delete from Bronze.crm_customers 
where customer_id in (
	select customer_id
	from (
		select *,
				row_number() over(
					partition by customer_id
					order by customer_id
				) as dup_flag

		from Bronze.crm_customers
	) f
	where dup_flag > 1
) or customer_id is null
