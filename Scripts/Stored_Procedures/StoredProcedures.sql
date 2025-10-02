
begin try
	print 'Loading data into Bronze tables...'
	exec Bronze.load_bronze;
end try
begin catch
	print 'Error: ' + error_message()
end catch
