#include <stddef.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#define KEY_SIZE 4
#define VALUE_SIZE 8
#define OFFSET_SIZE 4

typedef char byte;

/* check `man dbopen` */
struct DBT {
     void  *data;
     size_t size;
};

struct DB {
    /* Public API */
	int fileDescriptor; /* File descriptor is associated with database */
	struct BTREE *root; /* Root of btree */
	int freeOffset;
    int (*close)(const struct DB *db);
    int (*del)(const struct DB *db, const struct DBT *key);
    int (*get)(const struct DB *db, struct DBT *key, struct DBT  *data);
    int (*put)(const struct DB *db,  struct DBT *key, const struct DBT *data);
    int (*sync)(const struct DB *db);
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct BTREE {
	int n; /* Number of keys */
	byte leaf; /* Is this node a leaf? */
	struct DBT *keys; /* Keys */
	struct DBT *values;
	int *offsets; /* Offsets of nodes in file */
};

struct DBC {
        /* Maximum on-disk file size */
        /* 512MB by default */
        size_t db_size;
        /* Maximum chunk (node/data chunk) size */
        /* 4KB by default */
        size_t chunk_size;
        /* Maximum memory size */
        /* 16MB by default */
        size_t mem_size;
};

struct DB *dbcreate(const char *file, const struct DBC *conf);
struct DB *dbopen(const char *file);

