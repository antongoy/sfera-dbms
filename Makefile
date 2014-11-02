OBJ_LIST =  db_create.c db_io.c db_mem_util.c \
            db_util.c db_insert.c db_delete.c \
            db_search.c db_interfaces.c db_data_structures.h \
            db_macros.h
ADD_FLAGS = -O3
DEBUG_FLAG = -g
SO_FLAGS = -std=c99 -shared -fPIC
  
all: $(OBJ_LIST)
	gcc $(ADD_FLAGS) $^  $(SO_FLAGS) -o libmydb.so
	
exe:
	gcc $(OBJ_LIST) main.c -o main
	
old_all:
	gcc db.c -o $(SO_FLAGS) libmydb.so

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
