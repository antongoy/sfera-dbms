#include "db.h"

typedef struct DB DB;
typedef struct DBC DBC;
typedef struct DBT DBT;
typedef struct BTREE BTREE;
typedef struct DB_IMPL DB_IMPL;

BTREE* allocateNode(DB_IMPL * db, size_t leaf);

BTREE* readFromFile(DB_IMPL *db, size_t offset) {
	size_t i;
	BTREE * newNode = allocateNode(db, 1);
	
	if(lseek(db->fileDescriptor, offset, 0)) {
		return NULL;
	}
	
	read(db->fileDescriptor, db->buf, db->chunkSize);

	byte * ptr = db->buf;
	memcpy(&db->root->selfOffset, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&db->root->leaf, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&db->root->n, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(db->root->offsetsChildren, ptr, sizeof(size_t)*(2*db->t));
	
	int n = 2*db->t - 1;

	for(i = 0; i < n; i++) {
		memcpy(&newNode->keys[i].size, ptr, MAX_SIZE_KEY/2);
		ptr += MAX_SIZE_KEY/2;
		memcpy(&newNode->keys[i].data, ptr, MAX_SIZE_KEY/2);
		ptr += MAX_SIZE_KEY/2;
		memcpy(&newNode->values[i].size, ptr, MAX_SIZE_VALUE/2);
		ptr += MAX_SIZE_VALUE/2;
		memcpy(&newNode->values[i].data, ptr, MAX_SIZE_VALUE/2);
	}
	
	return newNode;

}

int writeInFile (DB_IMPL *db, BTREE *node) {
	size_t i;

	if (node->selfOffset == -1) {
		for (i = 0; db->mask[i] != 0 && i <  db->numOfBlocks; i++);
		
		if (i == db->numOfBlocks) {
			return -1;
		}
		node->selfOffset = i * db->chunkSize + db->numOfBlocks + db->chunkSize; 
		int offsetInMetadate = sizeof(size_t) + sizeof(int) + sizeof(size_t); 

		if (lseek(db->fileDescriptor, 0, 0)) {
			return -1;
		}
		write(db->fileDescriptor, db->curNumOfBlocks, sizeof(size_t));
			
		offsetInMetadate = db->chunkSize;
		offsetInMetadate += node->selfOffset / db->chunkSize;

		if (lseek(db->fileDescriptor, offsetInMetadate, 0)) {
			return -1;
		}
		byte temp = 1;
		byte *tempPtr = &temp;  
		write(db->fileDescriptor, tempPtr, sizeof(byte));
	}
	byte *ptr = db->buf;
	memcpy(ptr, &node->selfOffset, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(ptr, &node->leaf, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(ptr, &node->n, sizeof(size_t));
	ptr += sizeof(size_t);
	if (db->curNumOfBlocks > 0) {
		memcpy(ptr, node->offsetsChildren, sizeof(size_t)*(2*db->t));
		ptr += sizeof(size_t)*(2*db->t);
	}
	int n = 2*db->t - 1;

	for(i = 0; i < n; i++) {
		memcpy(ptr, &node->keys[i].size, MAX_SIZE_KEY/2);
		ptr += MAX_SIZE_KEY/2;
		memcpy(ptr, &node->keys[i].data, MAX_SIZE_KEY/2);
		ptr += MAX_SIZE_KEY/2;
		memcpy(ptr, &node->values[i].size, MAX_SIZE_VALUE/2);
		ptr += MAX_SIZE_VALUE/2;
		memcpy(ptr, &node->values[i].data, MAX_SIZE_VALUE/2);
	}


	if (lseek(db->fileDescriptor, node->selfOffset, 0)) {
		return -1;
	}

	write(db->fileDescriptor, db->buf, db->chunkSize);		

	return 0;
}

BTREE* allocateNode(DB_IMPL * db, size_t leaf) {
	BTREE * root = (BTREE *)malloc(sizeof(BTREE));
	
	root->n = 0;
	root->selfOffset = -1;
	root->leaf = leaf;
	
	root->offsetsChildren = (size_t *)malloc(sizeof(size_t)*2*db->t);
	root->keys = (DBT *)malloc(sizeof(DBT)*(2*db->t - 1));
	root->values = (DBT *)malloc(sizeof(DBT)*(2*db->t - 1));
	size_t i, n = 2*db->t - 1;
	for (i = 0; i < n; i++) {
		root->values[i].size = MAX_SIZE_VALUE/2;
		root->keys[i].size = MAX_SIZE_KEY/2;
		root->values[i].data = (byte *)malloc(sizeof(byte)*MAX_SIZE_VALUE/2);
		root->keys[i].data = (byte *)malloc(sizeof(byte)*MAX_SIZE_KEY/2);
	}
	return root;
} 

struct DB_IMPL* dbcreate(const char *file, const struct DBC *conf) {
	size_t fd, i;
	
	fd = creat(file, 0);
	

	DB_IMPL * newDB = (struct DB_IMPL *)malloc(sizeof(struct DB_IMPL));
	
	int temp = (conf->chunk_size - 3 * sizeof(size_t)) / (sizeof(size_t) + MAX_SIZE_KEY + MAX_SIZE_VALUE);
	
	temp = temp % 2 == 0 ? temp - 1 : temp;

	newDB->t = (temp + 1) / 2;

	int m = ((conf->db_size / conf->chunk_size) - 1) / (conf->chunk_size + 1);
	newDB->numOfBlocks = m * conf->chunk_size;
	newDB->mask = (byte *)malloc(conf->chunk_size * m);
	
	for(i = 0; i < m; i++) {
		newDB->mask[i] = 0;
	}
	
	newDB->fileDescriptor = fd;
	newDB->curNumOfBlocks = 0;
	newDB->chunkSize = conf->chunk_size;
	newDB->dbSize = conf->db_size;
	
	newDB->buf = (byte *)malloc(sizeof(byte)*conf->chunk_size);

	lseek(newDB->fileDescriptor, 0, 0);
	
	size_t * metaData = (size_t *)malloc(sizeof(size_t)*3);
	size_t *p = metaData;
	
	memcpy(p, &(newDB->curNumOfBlocks), sizeof(size_t));
	p += sizeof(size_t);
	memcpy(p, &(newDB->t), sizeof(size_t));
	p += sizeof(size_t);	
	memcpy(p, &(newDB->chunkSize), sizeof(size_t));

	write(newDB->fileDescriptor, metaData, sizeof(size_t)*3);
	
	newDB->root = allocateNode(newDB, 1);
	newDB->curNumOfBlocks++;
	
	writeInFile(newDB, newDB->root);
	
	return newDB;

}

DB* dbopen (const char *file) {
	size_t fd, i;
	
	if (fd = open(file, O_RDWR | O_APPEND, 0)) {
		return NULL;
	}
	
	DB_IMPL * newDB = (DB_IMPL *)malloc(sizeof(DB_IMPL));
	newDB->fileDescriptor = fd;

	size_t * metaData = (size_t *)malloc(2 * sizeof(size_t) + sizeof(int));
	read(newDB->fileDescriptor, metaData,  2 * sizeof(size_t) + sizeof(int));
	
	size_t *ptr = metaData;
	memcpy(&newDB->curNumOfBlocks, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newDB->t, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newDB->chunkSize, ptr, sizeof(size_t));

	newDB->buf = (byte *)malloc(sizeof(byte)*newDB->chunkSize);
	newDB->mask = (byte *)malloc(sizeof(byte)*newDB->numOfBlocks);	

	int m = newDB->numOfBlocks / newDB->chunkSize;
	byte * p = newDB->mask;
	lseek(newDB->fileDescriptor, newDB->chunkSize, 0);
	for(i = 1; i <= m; i++) {
		read(newDB->fileDescriptor, p, newDB->chunkSize);
		p += newDB->chunkSize;
	}

	newDB->root = readFromFile(newDB, newDB->chunkSize + newDB->numOfBlocks);

	return (DB *)newDB;	
}

void print_statistics(DB_IMPL *db) {
	printf("\nDatabase information:\n");
	printf("---------------------------------------------\n");
	printf("CurNumOfBlocks = %lu\n", db->curNumOfBlocks);
	printf("NumOfBlocks = %lu\n", db->numOfBlocks);
	printf("ChunkSize = %lu\n", db->chunkSize);
	printf("t = %lu\n", db->t);
	
}

void splitChild(DB_IMPL *db, BTREE *x, size_t i) {
	BTREE *z = allocateNode(db, 1);
	BTREE *y = readFromFile(db, x->offsetsChildren[i]);
	size_t j;
	z->leaf = y->leaf;
	z->n = db->t - 1;
	for(j = 0; j < db->t-1; j++) {
		memcpy(z->keys[j].data,z->keys[j + db->t].data, MAX_SIZE_KEY/2);
		memcpy(z->values[j].data,z->values[j + db->t].data, MAX_SIZE_VALUE/2);
	}
	if(!y->leaf) {
		for(j = 0; j < db->t; j++) {
			z->offsetsChildren[j] = y->offsetsChildren[j+db->t];
		}
	}
	y->n = db->t - 1;
	for (j = x->n; j >= i; j--) {
		x->offsetsChildren[j+1] = x->offsetsChildren[j];
	}
	writeInFile(db, z);
	x->offsetsChildren[i] = z->selfOffset;
	for (j = x->n - 1; j>=i-1; j--) {
		memcpy(x->keys[j+1].data,x->keys[j].data, MAX_SIZE_KEY/2);
		memcpy(x->values[j+1].data,x->values[j].data, MAX_SIZE_VALUE/2);
	} 
	memcpy(x->keys[i].data,y->keys[db->t].data, MAX_SIZE_KEY/2);
	memcpy(x->values[i].data,y->values[db->t].data, MAX_SIZE_VALUE/2);
	x->n++;
	writeInFile(db, y);
	writeInFile(db, x);
}

void insertNonfullNode(DB_IMPL *db, BTREE *node, DBT *key, DBT *value) {}

int insertNode(DB_IMPL *db, DBT *key, DBT *value) {
	BTREE *temp = db->root;
	if(db->root->n = 2*db->t - 1) {
		BTREE *s = allocateNode(db, 1);
		db->root = s;
		s->leaf = 0;
		s->n = 0;
		s->selfOffset = 0;
		temp->selfOffset = -1;
		writeInFile(db, temp);
		s->offsetsChildren[0] = temp->selfOffset;
		writeInFile(db, s);
		splitChild(db, s, 0);
		insertNonfullNode(db, s, key, value); 
	} else {
		insertNonfullNode(db, temp, key, value);
	}
}

int main (void) {
	DBC conf;
	conf.db_size = 512 * 1024 * 1024;
	conf.chunk_size = 4096;
	conf.mem_size = 16 * 1024 * 1024;
	struct DB_IMPL * myDB = dbcreate("database.db", &conf); 
	print_statistics(myDB);
}
