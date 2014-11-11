#ifndef _DB_UTIL_H_
#define _DB_UTIL_H_

#include <string.h>

#include "db_data_structures.h"
#include "db_macros.h"

void set_bit_true(byte *b, int pos);
void set_bit_false(byte *b, int pos);
int power(int a, int n);
int find_true_bit(byte *b);
int my_round(double f);
int is_in_node (struct BTREE *x, const struct DBT *key, size_t *pos);
int memcmp_wrapper (const struct DBT *value1, const struct DBT *value2);
void dbtcpy(struct DBT *data1, const struct DBT *data2);
void nodecpy(struct BTREE *dest, struct BTREE *src);


#endif // _DB_UTIL_H