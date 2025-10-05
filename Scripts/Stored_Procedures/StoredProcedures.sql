--begin try
--	print 'Loading data into Bronze tables...'
--	exec Bronze.load_bronze;
--end try
--begin catch
--	print 'Error: ' + error_message()
--end catch

begin try
	print 'Loading data into Silver CRM tables'
	exec Silver.load_silver_CRM;
end try
begin catch
	print 'Error: ' + error_message()
end catch

go

begin try
	print 'Loading data into Silver ERP tables'
	exec Silver.load_silver_ERP;
end try
begin catch
	print 'Error: ' + error_message()
end catch
