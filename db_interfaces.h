#ifndef _DB_INTERFACES_H_
#define _DB_INTERFACES_H_

#include "db_data_structures.h"

int db_del(struct DB *db, void *key, size_t key_len);
int db_get(struct DB *db, void *key, size_t key_len, void **val, size_t *val_len);
int db_put(struct DB *db, void *key, size_t key_len, void *val, size_t val_len);

int db_close(struct DB *db);

#endif // _DB_INTERFACES_H_