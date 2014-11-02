#include "db_delete.h"

int delete_from_btree_leaf (struct BTREE *x, size_t key_pos) {
    size_t i;

    for (i = key_pos; i < x->n - 1 ; i++) {
        dbtcpy(&x->keys[i], &x->keys[i+1]);
        dbtcpy(&x->values[i], &x->values[i+1]);
    }

    x->n--;

    return 0;
}

int merge_nodes (struct DB_IMPL *db, struct BTREE *y, struct DBT *key,
        struct DBT *value, struct BTREE *z) {
    size_t j;

    dbtcpy(&y->keys[db->t-1], key);
    dbtcpy(&y->values[db->t-1], value);

    for(j = 1; j < db->t; j++) {
        dbtcpy(&y->keys[j+db->t-1], &z->keys[j-1]);
        dbtcpy(&y->values[j+db->t-1], &z->values[j-1]);
    }

    if (!y->leaf) {
        for (j = 0; j < db->t + 1; j++) {
            y->offsets_children[db->t+j] = z->offsets_children[j];
        }
    }

    y->n = 2*db->t - 1;

    return 0;
}

void get_predecessor_key (struct DB_IMPL *db, struct BTREE *x,
        struct DBT *pred_key, struct DBT *pred_value) {
    if (x->leaf) {
        dbtcpy(pred_key, &x->keys[x->n - 1]);
        dbtcpy(pred_value, &x->values[x->n - 1]);
    } else {
        struct BTREE *y = read_from_file(db, x->offsets_children[x->n]);
        if (y == NULL) {
            perror("In get_predcessor_key function (read_from_file): ");
            exit(1);
        }
        get_predecessor_key(db, y, pred_key, pred_value);
        free_node(db, y);
    }

}

void get_successor_key (struct DB_IMPL *db, struct BTREE *x,
        struct DBT *succ_key, struct DBT *succ_value) {
    if (x->leaf) {
        dbtcpy(succ_key, &x->keys[0]);
        dbtcpy(succ_value, &x->values[0]);
    } else {
        struct BTREE *y = read_from_file(db, x->offsets_children[0]);
        get_successor_key(db, y, succ_key, succ_value);
        free_node(db, y);
    }
}

int keys_swap_rl(struct BTREE *left_child, struct BTREE *parent,
        size_t separator_pos, struct BTREE *right_child) {

    dbtcpy(&left_child->keys[left_child->n], &parent->keys[separator_pos]);
    dbtcpy(&left_child->values[left_child->n], &parent->values[separator_pos]);

    if (!left_child->leaf) {
        left_child->offsets_children[left_child->n + 1] = right_child->offsets_children[0];
    }

    left_child->n++;

    dbtcpy(&parent->keys[separator_pos], &right_child->keys[0]);
    dbtcpy(&parent->values[separator_pos], &right_child->values[0]);

    size_t i;
    for (i = 0; i < right_child->n - 1; i++) {
        dbtcpy(&right_child->keys[i], &right_child->keys[i+1]);
        dbtcpy(&right_child->values[i], &right_child->values[i+1]);
    }

    if (!right_child->leaf) {
        for (i = 0; i < right_child->n; i++) {
            right_child->offsets_children[i] = right_child->offsets_children[i+1];
        }
    }

    right_child->n--;

    return 0;
}

int keys_swap_lr(struct BTREE *left_child, struct BTREE *parent,
        size_t separator_pos, struct BTREE *right_child) {
    size_t i;

    for (i = right_child->n; i >= 1; i--) {
        dbtcpy(&right_child->keys[i], &right_child->keys[i-1]);
        dbtcpy(&right_child->values[i], &right_child->values[i-1]);
    }

    if (!right_child->leaf) {
        for (i = right_child->n + 1; i >= 1; i--) {
            right_child->offsets_children[i] = right_child->offsets_children[i-1];
        }
    }

    dbtcpy(&right_child->keys[0], &parent->keys[separator_pos]);
    dbtcpy(&right_child->values[0], &parent->values[separator_pos]);

    right_child->n++;

    dbtcpy(&parent->keys[separator_pos], &left_child->keys[left_child->n - 1]);
    dbtcpy(&parent->values[separator_pos], &left_child->values[left_child->n - 1]);

    if (!right_child->leaf) {
        right_child->offsets_children[0] = left_child->offsets_children[left_child->n];
    }

    left_child->n--;

    return 0;
}

void shift_keys(struct BTREE *x, long pos) {
    long j;

    for(j = pos; j < x->n - 1; j++) {
        dbtcpy(&x->keys[j], &x->keys[j+1]);
        dbtcpy(&x->values[j], &x->values[j+1]);
    }


    if (!x->leaf) {
        for(j = pos+1; j < x->n ; j++) {
            x->offsets_children[j] = x->offsets_children[j+1];
        }
    }

    x->n--;
}

int delete_from_btree (struct DB_IMPL *db, struct BTREE *x, const struct DBT *key) {
    size_t pos;

    if (is_in_node(x, key, &pos)) {
        size_t key_index = pos;

        if (x->leaf) {
            delete_from_btree_leaf(x, key_index);
            write_in_file(db, x);
            return 0;
        } else {
            struct BTREE *y = read_from_file(db, x->offsets_children[key_index]);

            if (y->n >= db->t) {
                struct DBT *pred_key = allocate_dbt((size_t)MAX_SIZE_KEY);
                struct DBT *pred_value = allocate_dbt((size_t)MAX_SIZE_VALUE);

                get_predecessor_key(db, y, pred_key, pred_value);

                delete_from_btree(db, y, pred_key);

                dbtcpy(&x->keys[key_index], pred_key);
                dbtcpy(&x->values[key_index], pred_value);

                write_in_file(db, x);

                free_dbt(pred_key);
                free_dbt(pred_value);
                free_node(db, y);
                return 0;
            }

            struct BTREE *z = read_from_file(db, x->offsets_children[key_index + 1]);

            if (z->n >= db->t) {
                struct DBT *succ_key = allocate_dbt((size_t)MAX_SIZE_KEY);
                struct DBT *succ_value = allocate_dbt((size_t)MAX_SIZE_VALUE);

                get_successor_key(db, z, succ_key, succ_value);

                delete_from_btree(db, z, succ_key);

                dbtcpy(&x->keys[key_index], succ_key);
                dbtcpy(&x->values[key_index], succ_value);

                write_in_file(db, x);

                free_dbt(succ_key);
                free_dbt(succ_value);
                free_node(db, y);
                free_node(db, z);
                return 0;
            }

            merge_nodes(db, y, &x->keys[key_index], &x->values[key_index], z);

            remove_from_file(db, z);
            free_node(db, z);

            shift_keys(x, key_index);

            if (x->n == 0) {
                remove_from_file(db, db->root);
                remove_from_file(db, y);
                db->cur_n_blocks++;
                free_node(db, db->root);
                db->root = y;
                y->self_offset = -1;
                write_in_file(db, y);
            } else {
                write_in_file(db, x);
            }

            delete_from_btree(db, y, key);

            if (db->root != y) {
                free_node(db, y);
            }

            return 0;
        }
    } else {
        size_t subtree_index = pos;
        if (x->leaf) {
            return 0;
        } else {
            struct BTREE *y = read_from_file(db, x->offsets_children[subtree_index]);

            if (y->n > db->t - 1) {
                int ret = delete_from_btree(db, y, key);
                free_node(db, y);
                return ret;
            }

            struct BTREE *z = NULL;
            struct BTREE *w = NULL;

            if (subtree_index != x->n) {
                z =  read_from_file(db, x->offsets_children[subtree_index + 1]);
                if (z->n >= db->t) {
                    keys_swap_rl(y, x, subtree_index, z);

                    write_in_file(db, x);
                    write_in_file(db, z);
                    write_in_file(db, y);
                    free_node(db, z);

                    int ret  = delete_from_btree(db, y, key);

                    free_node(db, y);

                    return ret;
                }
            }

            if (subtree_index != 0) {
                w = read_from_file(db, x->offsets_children[subtree_index - 1]);
                if (w->n >= db->t) {
                    keys_swap_lr(w, x, subtree_index - 1, y);

                    write_in_file(db, x);
                    write_in_file(db, w);
                    write_in_file(db, y);

                    free_node(db, w);
                    if (z != NULL) {
                        free_node(db, z);
                    }

                    int ret = delete_from_btree(db, y, key);

                    free_node(db, y);

                    return ret;
                }
            }

            if (subtree_index == x->n) {
                merge_nodes(db, w, &x->keys[x->n-1], &x->values[x->n-1], y);

                remove_from_file(db, y);
                free_node(db, y);

                shift_keys(x, x->n);

                if (x->n == 0) {
                    remove_from_file(db, db->root);
                    remove_from_file(db, w);
                    db->cur_n_blocks++;
                    free_node(db, db->root);
                    db->root = w;
                    w->self_offset = -1;
                    write_in_file(db, w);
                } else {
                    write_in_file(db, x);
                    write_in_file(db, w);
                }

                int ret = delete_from_btree(db, w, key);

                if (db->root != w) {
                    free_node(db, w);
                }

                return ret;

            } else {

                if (w != NULL) {
                    free_node(db, w);
                }

                merge_nodes(db, y, &x->keys[subtree_index], &x->values[subtree_index], z);

                remove_from_file(db, z);
                free_node(db, z);

                shift_keys(x, subtree_index);

                if (x->n == 0) {
                    remove_from_file(db, db->root);
                    remove_from_file(db, y);
                    db->cur_n_blocks++;
                    free_node(db, db->root);
                    db->root = y;
                    y->self_offset = -1;
                    write_in_file(db, y);
                } else {
                    write_in_file(db, x);
                    write_in_file(db, y);
                }

                int ret = delete_from_btree(db, y, key);

                if (db->root != y) {
                    free_node(db, y);
                }

                return ret;
            }
        }

    }

}

int delete_key(struct DB *aux_db, const struct DBT *key) {
    struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
    int ret =  delete_from_btree(db, db->root, key);
    return ret;
}