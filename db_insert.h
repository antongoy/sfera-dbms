#ifndef _DB_INSERT_H_
#define _DB_INSERT_H_

#include <stdio.h>

#include "db_data_structures.h"
#include "db_io.h"

int insert_key (struct DB *db, struct DBT *key, const struct DBT *value);
void insert_key_nonfull (struct DB_IMPL *db, struct BTREE *x, struct DBT *key, const struct DBT *value);
struct BTREE* split_child(struct DB_IMPL *db, struct BTREE *x,  struct BTREE *y, size_t i);

#endif // _DB_INSERT_H_