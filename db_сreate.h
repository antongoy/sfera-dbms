#ifndef _DB_CREATE_H_
#define _DB_CREATE_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "db_data_structures.h"
#include "db_io.h"
#include "db_mem_util.h"
#include "db_insert.h"
#include "db_search.h"
#include "db_delete.h"
#include "db_macros.h"

struct DB *dbcreate(const char *file, const struct DBC conf);
struct DB *dbopen(const char *file);
int dbclose (struct DB *close_db);

#endif // _DB_CREATE_H_