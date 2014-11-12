#include "db_insert.h"

int insert_key(struct DB *aux_db, struct DBT *key, const struct DBT *value) {
    struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
    struct BTREE *temp = db->root;

    if (temp->n == 2*db->t - 1) {
        struct BTREE *s = allocate_node(db->t, 1);
        db->root = s;
        db->root->leaf = 0;
        db->root->n = 0;
        db->root->self_offset = temp->self_offset;
        temp->self_offset = -1;
        write_in_file(db, temp);
        db->root->offsets_children[0] = temp->self_offset;
        db->cur_n_blocks++;
        write_in_file(db, db->root);
        struct BTREE *t = split_child(db, db->root, temp, 0);
        free_node(db, temp);
        insert_key_nonfull(db, s, key, value);
        free_node(db,t);
        return 0;
    } else {
        insert_key_nonfull(db, temp, key, value);
        return 0;
    }
}

void insert_key_nonfull (struct DB_IMPL *db, struct BTREE *x,
        struct DBT *key, const struct DBT *value){
    long i = x->n;
    size_t j;
    int flag = is_in_node(x, key, &j);

    if (x->leaf) {
        if (x->n == 0) {
            dbtcpy(&(x->keys[0]), key);
            dbtcpy(&(x->values[0]), value);
        } else {

            if (flag) {
                dbtcpy(&(x->values[j]), value);
                write_in_file(db, x);
                return;
            }

            for (i = x->n; i > j; i--) {
                dbtcpy(&(x->keys[i]), &(x->keys[i-1]));
                dbtcpy(&(x->values[i]), &(x->values[i-1]));
            }

            dbtcpy(&(x->keys[j]), key);
            dbtcpy(&(x->values[j]), value);
        }
        x->n++;
        write_in_file(db, x);
    } else {
        if (flag) {
            dbtcpy(&(x->values[j]), value);
            write_in_file(db, x);
            return;
        }
        struct BTREE *child = read_from_file(db, x->offsets_children[j]);

        if (!memcmp_wrapper(&child->keys[db->t-1], key)) {
            dbtcpy(&(child->values[db->t-1]), value);
            write_in_file(db, child);
            free_node(db, child);
            return;
        }

        if (child->n == 2*db->t - 1) {
            struct BTREE *newChild = split_child(db, x, child, j);
            if (memcmp_wrapper(key, &x->keys[j]) > 0) {
                insert_key_nonfull(db, newChild, key, value);
            } else {
                insert_key_nonfull(db, child, key, value);
            }
            free_node(db, newChild);
        } else {
            insert_key_nonfull(db, child, key, value);
        }
        free_node(db,child);
    }
}

struct BTREE* split_child(struct DB_IMPL *db, struct BTREE *x,  struct BTREE *y, size_t i) {
    long j;
    //Create new node
    struct BTREE *z = allocate_node(db->t, 1);
    if (z == NULL) {
        fprintf(stderr, "In split_child function: Don't create new node\n");
        return NULL;
    }

    //Copy simple information to new node 'z'
    z->leaf = y->leaf;
    z->n = db->t - 1;
    z->self_offset = -1;

    //Copy second part of y to z ++++++++++++
    for(j = 0; j < db->t-1; j++) {
        dbtcpy(&(z->keys[j]), &(y->keys[j+db->t]));
        dbtcpy(&(z->values[j]), &(y->values[j+db->t]));
    }

    //If not leaf then copy offsets_children ++++++++++
    if(!y->leaf) {
        for(j = 0; j < db->t; j++) {
            z->offsets_children[j] = y->offsets_children[j+db->t];
        }
    }

    //Set new size of y node
    y->n = db->t - 1;

    //Make place for new children in parent node +++++++
    for (j = x->n+1; j > i + 1; j--) {
        x->offsets_children[j] = x->offsets_children[j-1];
    }

    //Write
    db->cur_n_blocks++;
    if (write_in_file(db, z) < 0) {
        fprintf(stderr, "In split_child function: error in the write_in_file()\n");
        return NULL;
    }

    x->offsets_children[i+1] = z->self_offset;

    //Make place for new key and value ++++++
    for (j = x->n; j > i; j--) {
        dbtcpy(&x->keys[j], &x->keys[j-1]);
        dbtcpy(&x->values[j], &x->values[j-1]);
    }

    //Copy to free place node from son +++++++
    dbtcpy(&x->keys[i], &y->keys[db->t-1]);
    dbtcpy(&x->values[i], &y->values[db->t-1]);

    x->n++;
    if (write_in_file(db, y) < 0) {
        fprintf(stderr, "In split_child function: error in the write_in_file()\n");
        return NULL;
    }

    if (write_in_file(db, x) < 0) {
        fprintf(stderr, "In split_child function: error in the write_in_file()\n");
        return NULL;
    }

    return z;
}