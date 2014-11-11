#ifndef _DB_MEM_UTIL_H_
#define _DB_MEM_UTIL_H_

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "db_data_structures.h"
#include "db_macros.h"

struct BTREE* allocate_node(size_t t, size_t leaf);
void free_htable_node(struct DB_IMPL *db, struct HTABLE_NODE *htable_node);
void free_node (struct DB_IMPL *db, struct BTREE *node);
struct DBT * allocate_dbt(size_t size);
void free_dbt (struct DBT *ptr);

#endif // _DB_MEM_UTIL_H_