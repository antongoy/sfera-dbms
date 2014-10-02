#include "db.h"

typedef struct DB DB;
typedef struct DBC DBC;
typedef struct DBT DBT;
typedef struct BTREE BTREE;

BTREE *allocateNode (int numOfBlocks) {
	BTREE *newBTREE;
	int i;

	newBTREE = (BTREE *)malloc(sizeof(BTREE));
	newBTREE->keys = (DBT *)malloc(sizeof(DBT)*numOfBlocks);
	newBTREE->values = (DBT *)malloc(sizeof(DBT)*numOfBlocks);
	newBTREE->offsets = (int *)malloc(sizeof(int)*(numOfBlocks + 1));

	for (i = 0; i < numOfBlocks; i++) {
		newBTREE->keys[i].data = (int *)malloc(sizeof(int));
		newBTREE->keys[i].size = KEY_SIZE;
	}

	for (i = 0; i < numOfBlocks; i++) {
		newBTREE->values[i].data = (char *)malloc(sizeof(char)*VALUE_SIZE);
		newBTREE->values[i].size = VALUE_SIZE;
	}

	
	return newBTREE;	
}


int writeInFile(DB *db, BTREE * node) {
	int i;
	int offset1 = db->freeOffset;
	if (lseek(db->fileDescriptor, db->freeOffset, 0)) { 
		return -1;
	}

	for (i = 0; i < db->root->n; i++) { /* First, write key + value (in Coreman, key_i) */
		write(db->fileDescriptor, node->keys[i].data, node->keys[i].size);	
		write(db->fileDescriptor, node->values[i].data, node->values[i].size);	
	}
	
	for (i = 0; i < db->root->n + 1; i++) { /* Second, after keys and values, we write offsets, which point to children of this node */
		write(db->fileDescriptor, node->offsets[i], sizeof(int));
	}
	
	if (node != db->root) { /* If node it's not root then we can free memory. The root is always stored in memory */
		freeNode();
	}

	db->freeOffset = offset1 + 4096;
	return 0;
}

BTREE *readFromFile(DB *db, int offsetRead) {
	if (lseek(db->fileDescriptor, offsetRead, 0)) { /* Move to the position which is needed to read */
		return NULL;
	}

	BTREE * newNode = allocateNode(db->t);
}



DB *dbcreate(const char *file, const DBC *conf) {
	int fd;
	int i,j;
	DB * newDB;

	if (fd = creat(file, 0)) {
		return NULL;
	}

	newDB = (DB *)malloc(sizeof(DB)); /* Allocate memory for the new database */

	BTREE *tempPtr = newDB->root; /* It's a temporary pointer to B-tree root that is stored in database */
	int numOfBlocks = (int)(conf->chunk_size / (KEY_SIZE + VALUE_SIZE)); /* Number of blocks in one node */
	newDB->freeOffset = 0;
	newDB->t = numOfBlocks;

	tempPtr = allocateNode(numOfBlocks); /* Allocate memory for the root node that will be always stored in RAM */
	
	tempPtr->n = 0;
	tempPtr->leaf = 1;
	for (i = 0; i < numOfBlocks; i++) {
		*((int *)tempPtr->keys[i].data) = -1; /* "-1" means that the value is not defined */
		for (j = 0; j < VALUE_SIZE; j++)
		*((char *)tempPtr->values[i].data + i) = -1;
	}

	for (i = 0; i < numOfBlocks + 1; i++) {
		tempPtr->offsets[i] = -1;
	}

	writeInFile(newDB, tempPtr);
		
	return newDB;
}

int main (void) {
	DBC conf;
	conf.chunk_size = 4096;
	DB * my = dbcreate("newDataBase", &conf);
}
