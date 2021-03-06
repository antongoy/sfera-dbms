OBJ_LIST =  db_create.c db_io.c db_mem_util.c \
            db_util.c db_insert.c db_delete.c \
            db_search.c db_interfaces.c db_cache.c main.c
ADD_FLAGS = -O3
DEBUG_FLAG = -g -O0
SO_FLAGS = -std=c99 -shared -fPIC
COND_COMPL = -D _CACHE_=1
  
all: $(OBJ_LIST)
	gcc  -DHASH_FUNCTION=HASH_SFH $(ADD_FLAGS) $^  $(SO_FLAGS) -o libmydb.so
	
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
