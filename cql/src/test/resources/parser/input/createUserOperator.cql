create operator userOp as "com.huawei.UserOperator" 
		input (id string, name string, cc int) 
		output (id string, name string, type int, cc int) 
		properties ("client.port" = "8080");
		
create operator userOp as "com.huawei.UserOperator" 
		input (id string, name string, cc int) 
		output (id string, name string, type int, cc int);

		