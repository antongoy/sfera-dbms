main_exe:     
	gcc -g -O0 db.c -o db
    
all:
	gcc -g -O0 db.c -std=c99 -shared -fPIC -o libmydb.so

gen_workload:
	@python test/gen_workload.py --output custom_workload/my_workload --config config.schema.yml 
	@python test/runner.py --new --workload custom_workload/my_workload.in --so fall14BDB/example/libdbsophia.so
	rm -fr test/my.db

runner_custom:
	@python test/runner.py --workload custom_workload/workload_custom.in --so ./libmydb.so
	
runner:
	@python test/runner.py --workload custom_workload/workload.in --so ./libmydb.so
	
runner_my:
	@python test/runner.py --workload custom_workload/my_workload.in --so ./libmydb.so
	
runner_sophia:
	@python test/runner.py --workload custom_workload/workload.in --so fall14BDB/example/libdbsophia.so
