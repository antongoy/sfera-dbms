#ifndef _DB_DATA_STRUCTURES_H_
#define _DB_DATA_STRUCTURES_H_

#include <stddef.h>

typedef char byte;

struct DBT {
    void  *data;
    size_t size;
};

struct DB {
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, const struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT  *data);
    int (*put)(struct DB *db,  struct DBT *key, const struct DBT *data);
    int (*sync)(struct DB *db);
};

struct DB_IMPL {
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, const struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db,  struct DBT *key, const struct DBT *data);
    int (*sync)(struct DB *db);

    struct BTREE *root; // Root of btree

    size_t file_desc; // File descriptor which is associated with database
    size_t t; // Degree of tree nodes
    size_t cur_n_blocks; // Current number of blocks in btree
    size_t n_blocks; // Max number of blocks in btree
    size_t chunk_size; // Configuration information for DB
    size_t db_size; // Max size of database in bytes

    byte *mask; // Mask for searching free blocks
    byte *buf; // Auxiliary buffer
};

struct BTREE {
    size_t n; // Number of keys
    size_t self_offset; // Offset of this node in file
    size_t leaf; // Is this node a leaf?
    size_t *offsets_children; // Offsets of children nodes in file

    struct DBT *keys; // Keys
    struct DBT *values; // Values
};

struct DBC {
    size_t db_size;
    size_t chunk_size;
    //size_t mem_size;
};

#endif // _DB_DATA_STRUCTURES_H_