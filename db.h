#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#define MAX_SIZE_KEY 16
#define MAX_SIZE_VALUE 112
#define OFFSET_SIZE 4
#define MIN(a,b) ((a)>(b)?(b):(a))

typedef char byte;

/* check `man dbopen` */
struct DBT {
	 void  *data;
	 size_t size;
};

struct DB {
	/* Public API */
	int (*close)(const struct DB *db);
	int (*del)(const struct DB *db, const struct DBT *key);
	int (*get)(const struct DB *db, struct DBT *key, struct DBT  *data);
	int (*put)(const struct DB *db,  struct DBT *key, const struct DBT *data);
	int (*sync)(const struct DB *db);
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB_IMPL {
	int (*close)(const struct DB *db);
	int (*del)(const struct DB *db, const struct DBT *key);
	int (*get)(const struct DB *db, struct DBT *key, struct DBT  *data);
	int (*put)(const struct DB *db,  struct DBT *key, const struct DBT *data);
	int (*sync)(const struct DB *db);

	size_t fileDescriptor; /* File descriptor which is associated with database */
	struct BTREE *root; /* Root of btree */
	size_t t; /* Degree of tree nodes */
	byte *mask; // Mask for searching free blocks
	byte *buf;
	size_t curNumOfBlocks;
	size_t numOfBlocks;
	size_t chunkSize; // Configutaion information for DB
	size_t dbSize; 
	
};

struct BTREE {
	size_t n; /* Number of keys */
	size_t selfOffset;
	size_t leaf; /* Is this node a leaf? */
	size_t *offsetsChildren; /* Offsets of nodes in file */
	struct DBT *keys; /* Keys */
 	struct DBT *values;
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

struct BTREE_POS {
	struct BTREE *node;
	size_t pos;
};

struct DB *dbcreate(const char *file, const struct DBC *conf);
struct DB *dbopen(const char *file);

