#ifndef _DB_SEARCH_H_
#define _DB_SEARCH_H_

#include "db_data_structures.h"
#include "db_io.h"

int search_key(struct DB *aux_db, struct DBT *key, struct DBT *value);
int search_key_inside(struct DB_IMPL *db, struct BTREE *x, struct DBT *key, struct DBT *value);

#endif // _DB_SEARCH_H_