#ifndef _DELETE_H_
#define _DELETE_H_

#include "db_data_structures.h"
#include "db_io.h"
#include "db_util.h"
#include "db_mem_util.h"
#include "db_macros.h"

int delete_from_btree_leaf (struct BTREE *x, size_t key_pos);
int merge_nodes (struct DB_IMPL *db, struct BTREE *y, struct DBT *key,
        struct DBT *value, struct BTREE *z);
void get_predecessor_key (struct DB_IMPL *db, struct BTREE *x,
        struct DBT *pred_key, struct DBT *pred_value);
void get_successor_key (struct DB_IMPL *db, struct BTREE *x,
        struct DBT *succ_key, struct DBT *succ_value);
int keys_swap_rl(struct BTREE *left_child, struct BTREE *parent,
        size_t separator_pos, struct BTREE *right_child);
int keys_swap_lr(struct BTREE *left_child, struct BTREE *parent,
        size_t separator_pos, struct BTREE *right_child);
void shift_keys(struct BTREE *x, long pos);
int delete_from_btree (struct DB_IMPL *db, struct BTREE *x, const struct DBT *key);
int delete_key(struct DB *aux_db, const struct DBT *key);


#endif // _DELETE_H_