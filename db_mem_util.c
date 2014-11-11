#include "db_mem_util.h"

struct BTREE* allocate_node(size_t t, size_t leaf) {
    size_t i;
    size_t n = 2 * t;

    struct BTREE * root = (struct BTREE *)malloc(sizeof(struct BTREE));
    if (root == NULL) {
        fprintf(stderr, "In allocate_node function: Memory allocation error (root).\n");
        return NULL;
    }

    root->n = 0;
    root->self_offset = -1;
    root->leaf = leaf;

    root->offsets_children = (size_t *)malloc(sizeof(size_t)*n);
    if (root->offsets_children == NULL) {
        fprintf(stderr, "In allocate_node function: Memory allocation error (root->offsets_children).\n");
        return NULL;
    }

    memset(root->offsets_children, 0, sizeof(size_t)*n);
    root->keys = (struct DBT *)malloc(sizeof(struct DBT)*(n - 1));
    if (root->keys == NULL) {
        fprintf(stderr, "In allocate_node function: Memory allocation error (root->keys).\n");
        return NULL;
    }

    root->values = (struct DBT *)malloc(sizeof(struct DBT)*(n - 1));
    if (root->values == NULL) {
        fprintf(stderr, "In allocate_node function: Memory allocation error (root->values).\n");
        return NULL;
    }

    for (i = 0; i < n - 1; i++) {
        root->values[i].size = 0;
        root->keys[i].size = 0;
        root->values[i].data = (byte *)malloc(sizeof(byte)*(MAX_SIZE_VALUE - sizeof(size_t)));
        if (root->values[i].data == NULL) {
            fprintf(stderr, "In allocate_node function: Memory allocation error (root->values[%lu].data).\n", i);
            return NULL;
        }

        memset(root->values[i].data, 0, sizeof(byte)*(MAX_SIZE_VALUE - sizeof(size_t)));
        root->keys[i].data = (byte *)malloc(sizeof(byte)*(MAX_SIZE_KEY - sizeof(size_t)));
        if (root->keys[i].data == NULL) {
            fprintf(stderr, "In allocate_node function: Memory allocation error (root->keys[%lu].data).\n", i);
            return NULL;
        }
        memset(root->keys[i].data, 0, sizeof(byte)*(MAX_SIZE_KEY - sizeof(size_t)));
    }
    return root;
}

void free_node (struct DB_IMPL *db, struct BTREE *node) {
    int i;
    for (i = 0; i < 2*db->t - 1; i++) {
        free(node->values[i].data);
        free(node->keys[i].data);
    }
    free(node->keys);
    free(node->values);
    free(node->offsets_children);
    free(node);

}

struct DBT * allocate_dbt(size_t size) {
    struct DBT *dbt = (struct DBT *)malloc(sizeof(struct DBT));
    if (dbt == NULL) {
        perror("In function allocate_dbt function (malloc)");
        return NULL;
    }
    dbt->size = size;
    dbt->data = (size_t *)malloc(size);
    if (dbt->data == NULL)  {
        perror("In function allocate_dbt function (malloc)");
        return NULL;
    }
    return dbt;
}

void free_dbt (struct DBT *ptr) {
    free(ptr->data);
    free(ptr);
}

void free_htable_node(struct DB_IMPL *db, struct HTABLE_NODE *htable_node) {
    struct CACHE_STORAGE_NODE *csn = htable_node->data->data;

    free_node(db, csn->page);
    free(csn);
    free(htable_node->data);
    free(htable_node);
}
