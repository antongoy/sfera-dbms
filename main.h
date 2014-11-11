#ifndef _MAIN_H_
#define _MAIN_H_

#include <time.h>

#include "db_data_structures.h"
#include "db_io.h"
#include "db_delete.h"
#include "db_insert.h"
#include "db_macros.h"
#include "db_search.h"
#include "db_create.h"

int memcmpwrapp2 (const struct DBT *value1, const struct DBT *value2);
void printBTREE (FILE *f, struct DB_IMPL *db, struct BTREE *x, int depth, int n);
void print_node (struct BTREE *x);
void print_statistics(struct DB_IMPL *db);
void value_gen(char * str, size_t size);
int num_keys_in_btree(struct DB_IMPL *db, struct BTREE *x);

#endif // _MAIN_H_