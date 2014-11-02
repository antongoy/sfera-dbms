#include "db_search.h"

int search_key(struct DB *aux_db, struct DBT *key, struct DBT *value) {
    struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
    value->data = (byte *)malloc(MAX_SIZE_VALUE);
    return search_key_inside(db, db->root, key, value);
}

int search_key_inside(struct DB_IMPL *db, struct BTREE *x, struct DBT *key, struct DBT *value) {
    long i = 0;

    int flag = is_in_node(x, key, &i);

    if (flag == 1) {
        dbtcpy(value, &(x->values[i]));
        return 0;
    } else {
        if (x->leaf) {
            return -1;
        }
        struct BTREE *new_node = read_from_file(db, x->offsets_children[i]);
        if (new_node == NULL) {
            perror("Error in search_key_inside function (read_from_file)");
            exit(1);
        }
        int  res =  search_key_inside(db, new_node, key, value);
        free_node(db, new_node);
        return res;
    }
}