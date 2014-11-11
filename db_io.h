#ifndef _DB_IO_H_
#define _DB_IO_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "db_data_structures.h"
#include "db_cache.h"
#include "db_mem_util.h"
#include "db_util.h"
#include "db_macros.h"

struct BTREE* read_from_file(struct DB_IMPL *db, size_t offset);
int write_in_file (struct DB_IMPL *db, struct BTREE *node);
int remove_from_file(struct DB_IMPL *db, struct BTREE *x);

#endif //_DB_IO_H_