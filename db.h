#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <unistd.h>
#include <time.h>


#define MAX_SIZE_KEY 30
#define MAX_SIZE_VALUE 30
#define DB_HEADER_SIZE sizeof(size_t) * 4 
#define BYTE_SIZE 8
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
	int (*close)(struct DB *db);
	int (*del)(struct DB *db, const struct DBT *key);
	int (*get)(struct DB *db, struct DBT *key, struct DBT  *data);
	int (*put)(struct DB *db,  struct DBT *key, const struct DBT *data);
	int (*sync)(struct DB *db);
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct DB_IMPL {
	int (*close)(struct DB *db);
	int (*del)(struct DB *db, const struct DBT *key);
	int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
	int (*put)(struct DB *db,  struct DBT *key, const struct DBT *data);
	int (*sync)(struct DB *db);

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
		//size_t mem_size;
};

struct DB *dbcreate(const char *file, const struct DBC conf);
struct DB *dbopen(const char *file);

struct BTREE* readFromFile(struct DB_IMPL *db, size_t offset);
struct BTREE* allocateNode(size_t t, size_t leaf);
int writeInFile (struct DB_IMPL *db, struct BTREE *node);
void freeNode(struct DB_IMPL *db, struct BTREE *node);
void print_node (FILE *f, struct BTREE *x);
int insert_key (struct DB *db, struct DBT *key, const struct DBT *value);
int search_key(struct DB *db, struct DBT *key, struct DBT *value);
int dbclose (struct DB *dbForClosing);
int delete_key(struct DB *aux_db, const struct DBT *key);
int memcmpwrapp (const struct DBT *value1, const struct DBT *value2);
int is_in_node (struct BTREE *x, const struct DBT *key, size_t *pos);

int db_close(struct DB *db);
int db_del(struct DB *db, void *key, size_t key_len);
int db_get(struct DB *db, void *key, size_t key_len, void **val, size_t *val_len);
int db_put(struct DB *db, void *key, size_t key_len, void *val, size_t val_len);
