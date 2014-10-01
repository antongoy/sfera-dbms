all:
	gcc db.c db.h -shared -fPIC -o db.so
	
