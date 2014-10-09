#include "db.h"

typedef struct DB DB;
typedef struct DBC DBC;
typedef struct DBT DBT;
typedef struct BTREE BTREE;
typedef struct DB_IMPL DB_IMPL;
typedef struct BTREE_POS BTREE_POS;

BTREE* readFromFile(DB_IMPL *db, size_t offset);
BTREE* allocateNode(size_t t, size_t leaf);
int writeInFile (DB_IMPL *db, BTREE *node);
void freeNode(BTREE *node);
void print_node (BTREE *x);


BTREE* readFromFile(DB_IMPL *db, size_t offset) {
	size_t i;
	BTREE *newNode = allocateNode(db->t, 1);

	//Move to start position of node
	lseek(db->fileDescriptor, offset, 0);

	//Read to buffer
	read(db->fileDescriptor, db->buf, db->chunkSize);

	//Parse the buffer. Metadata
	byte * ptr = db->buf;
	memcpy(&newNode->selfOffset, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newNode->leaf, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newNode->n, ptr, sizeof(size_t));
	ptr += sizeof(size_t);

	//Parse the buffer. Offsets of children
	memcpy(newNode->offsetsChildren, ptr, sizeof(size_t)*(2*db->t));
	ptr +=  sizeof(size_t)*(2*db->t);

	//Parse the buffer. Keys and values
	int n = 2*db->t - 1;
	for(i = 0; i < n; i++) {
		memcpy(&newNode->keys[i].size, ptr, sizeof(size_t));
		ptr += sizeof(size_t);
		memcpy(newNode->keys[i].data, ptr, MAX_SIZE_KEY- sizeof(size_t));
		ptr += MAX_SIZE_KEY- sizeof(size_t);
		memcpy(&newNode->values[i].size, ptr, sizeof(size_t));
		ptr += sizeof(size_t);
		memcpy(newNode->values[i].data, ptr, MAX_SIZE_VALUE - sizeof(size_t));
	}
	
	return newNode;

}

int writeInFile (DB_IMPL *db, BTREE *node) {
	size_t i;

	// If selfOffset = -1 then node hasn't written yet
	if (node->selfOffset == -1) {

		//Find free space in file - first bit that is set to zero
		for (i = 0; db->mask[i] != 0 && i <  db->numOfBlocks; i++);

		//We can exceed size of mask. Then database is full
		if (i == db->numOfBlocks) {
			return -1;
		}

		//Calculate new selfOffset
		node->selfOffset = i * db->chunkSize + db->numOfBlocks + db->chunkSize;

		//Move to start of file and change curNumBlocks
		if (lseek(db->fileDescriptor, 0, 0) == -1) {
			fprintf(stderr, "In function writeInFile: it's impossible to move to beginning of the file\n");
			return -1;
		}

		if (write(db->fileDescriptor, db->curNumOfBlocks, sizeof(size_t)) != -1) {
			fprintf(stderr, "In function writeInFile: System call write() generated the error\n");
			return -1;
		}

		//Move to bit which is responsible for new node
		int offsetInMetadata = db->chunkSize + i;
		if (lseek(db->fileDescriptor, offsetInMetadata, 0) == -1) {
			fprintf(stderr, "In function writeInFile: System call lseek() generated the error\n");
			return -1;
		}

		//Set 1 in file and in 'db' structure
		byte temp = 1;
		byte *tempPtr = &temp;

		if (write(db->fileDescriptor, tempPtr, sizeof(byte)) == -1) {
			fprintf(stderr, "In function writeInFile: System call write() generated the error\n");
			return -1;
		}
		db->mask[i] = 1;
	}

	//Auxiliary pointer 'ptr' for copying data in 'buf'
	byte *ptr = db->buf;

	//Copy header with block metadata
	memcpy(ptr, &node->selfOffset, sizeof(size_t));

	ptr += sizeof(size_t);
	memcpy(ptr, &node->leaf, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(ptr, &node->n, sizeof(size_t));
	ptr += sizeof(size_t);

	//Copy 'offsetsChildren'
	memcpy(ptr, node->offsetsChildren, sizeof(size_t)*(2*db->t));
	ptr += sizeof(size_t)*(2*db->t);

	//Copy 'keys' and 'values'
	int n = 2*db->t - 1;
	for(i = 0; i < n; i++) {
		memcpy(ptr, &node->keys[i].size, sizeof(size_t));
		ptr += sizeof(size_t);
		memcpy(ptr, node->keys[i].data, MAX_SIZE_KEY - sizeof(size_t));
		ptr += MAX_SIZE_KEY - sizeof(size_t);
		memcpy(ptr, &node->values[i].size, sizeof(size_t));
		ptr += sizeof(size_t);
		memcpy(ptr, node->values[i].data, MAX_SIZE_VALUE - sizeof(size_t));
	}

	//Write in file node
	if (lseek(db->fileDescriptor, node->selfOffset, 0) == -1) {
		fprintf(stderr, "In function writeInFile: System call lseek() generated the error\n");
		return -1;
	}

	if(write(db->fileDescriptor, db->buf, db->chunkSize) == -1) {
		fprintf(stderr, "In function writeInFile: System call write() generated the error\n");
		return -1;
	}
	return 0;
}

BTREE* allocateNode(size_t t, size_t leaf) {
	size_t i;
	size_t n = 2 * t;

	BTREE * root = (BTREE *)malloc(sizeof(BTREE));

	root->n = 0;
	root->selfOffset = -1;
	root->leaf = leaf;

	root->offsetsChildren = (size_t *)malloc(sizeof(size_t)*n);
	root->keys = (DBT *)malloc(sizeof(DBT)*(n - 1));
	root->values = (DBT *)malloc(sizeof(DBT)*(n - 1));

	for (i = 0; i < n - 1; i++) {
		root->values[i].size = 0;
		root->keys[i].size = 0;
		root->values[i].data = (byte *)malloc(sizeof(byte)*(MAX_SIZE_VALUE - sizeof(size_t)));
		root->keys[i].data = (byte *)malloc(sizeof(byte)*(MAX_SIZE_KEY - sizeof(size_t)));
	}
	return root;
} 

struct DB* dbcreate(const char *file, const struct DBC *conf) {
	size_t fd, i;

	if ((fd = open(file, O_CREAT|O_RDWR|O_TRUNC,0)) == -1) {
		fprintf(stderr, "In dbcreate function: Impossible creating new file\n");
		return NULL;
	}

	//Allocate memory for new Database
	DB_IMPL * newDB = (struct DB_IMPL *)malloc(sizeof(struct DB_IMPL));

	//Fill simple fields in newDB structure
	newDB->fileDescriptor = fd;
	newDB->curNumOfBlocks = 0;
	newDB->chunkSize = conf->chunk_size;
	newDB->dbSize = conf->db_size;

	//Calculating auxiliary variable 'temp', which is needed for the evaluating 't' - degree of B-tree
	int temp = (conf->chunk_size - 3 * sizeof(size_t)) / (sizeof(size_t) + MAX_SIZE_KEY + MAX_SIZE_VALUE);
	temp = temp % 2 == 0 ? temp - 1 : temp;
	newDB->t = (temp + 1) / 2;

	//Calculating auxiliary variable 'm', which is needed for the evaluating 'numOfBlocks'is max number of nodes that stored in file
	int m = ((conf->db_size / conf->chunk_size) - 1) / (conf->chunk_size + 1);
	newDB->numOfBlocks = m * conf->chunk_size;

	//Allocate memory for bit mask - it shows free spaces in file
	newDB->mask = (byte *)malloc(conf->chunk_size * m);
	
	//At  the initial moment in mask all bits is zero
	for(i = 0; i < m; i++) {
		newDB->mask[i] = 0;
	}

	//Memory for auxiliary buffer. It is needed for writing in file
	newDB->buf = (byte *)malloc(sizeof(byte)*conf->chunk_size);

	//Move to start of file
	if (lseek(newDB->fileDescriptor, 0, 0) == -1) {
		fprintf(stderr, "In dbcreate function: system call lseek throws the error\n");
		return NULL;
	}

	//'metaData' is auxiliary buffer for database metadata
	size_t * metaData = (size_t *)malloc(sizeof(size_t)*3);
	size_t *p = metaData;

	//Copy to buffer database metadata
	memcpy(p, &(newDB->curNumOfBlocks), sizeof(size_t));
	p += sizeof(size_t);
	memcpy(p, &(newDB->t), sizeof(size_t));
	p += sizeof(size_t);    
	memcpy(p, &(newDB->chunkSize), sizeof(size_t));

	//Write in file database metadata
	if(write(newDB->fileDescriptor, metaData, sizeof(size_t)*3) == -1) {
		fprintf(stderr, "In dbcreate function: system call write() throws the error\n");
		return NULL;
	}

	//Allocate memory for root node
	newDB->root = allocateNode(newDB->t, 1);

	// -1 is mean that this node hasn't written in file yet
	newDB->root->selfOffset = -1;

	//We have new node!
	newDB->curNumOfBlocks++;

	//No comments
	if(writeInFile(newDB, newDB->root) == -1) {
		return NULL;
	}

	free(metaData);
	return (DB *)newDB;
}

DB* dbopen (const char *file) {
	size_t fd, i;
	
	if (fd = open(file, O_RDWR | O_APPEND, 0)) {
		fprintf(stderr, "In dbopen function: imposible open database\n");
		return NULL;
	}
	
	DB_IMPL *newDB = (DB_IMPL *)malloc(sizeof(DB_IMPL));
	if (newDB == NULL) {
		fprintf(stderr, "In dbopen function: Memory wasn't allocated for newDB\n");
		return NULL;
	}
	

	newDB->fileDescriptor = fd;

	//Metadata buffer for database header
	size_t *metaData = (size_t *)malloc(2 * sizeof(size_t) + sizeof(int));
	if (metaData == NULL) {
		fprintf(stderr, "In dbopen function: Memory wasn't allocated for metaData\n");
		return NULL;
	}
	
	if (read(newDB->fileDescriptor, metaData,  2 * sizeof(size_t) + sizeof(int)) == -1) {
		fprintf(stderr, "In dbopen function: imposible read data\n");
		return NULL;
	}

	//Parse metadata
	size_t *ptr = metaData;
	memcpy(&newDB->curNumOfBlocks, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newDB->t, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newDB->chunkSize, ptr, sizeof(size_t));

	//Allocate memory
	newDB->buf = (byte *)malloc(sizeof(byte)*newDB->chunkSize);
	if (newDB->buf == NULL) {
		fprintf(stderr, "In dbopen function: : Memory wasn't allocated for newDB->buf\n");
	}
	
	newDB->mask = (byte *)malloc(sizeof(byte)*newDB->numOfBlocks);    
	if (newDB->mask == NULL) {
		fprintf(stderr, "In dbopen function: : Memory wasn't allocated for newDB->mask\n");
	}
	
	int m = newDB->numOfBlocks / newDB->chunkSize;

	//Read bit mask from file
	byte * p = newDB->mask;

	if (lseek(newDB->fileDescriptor, newDB->chunkSize, 0) == -1) {
		fprintf(stderr, "In dbopen function: system call lseek() throws the error\n");
		return NULL;
	}

	for(i = 1; i <= m; i++) {
		if (read(newDB->fileDescriptor, p, newDB->chunkSize) == -1) {
			fprintf(stderr, "In dbopen function: imposible read from database\n");
			return NULL;
		}
		p += newDB->chunkSize;
	}

	newDB->root = readFromFile(newDB, newDB->chunkSize + newDB->numOfBlocks);

	if (newDB->root == NULL) {
		fprintf(stderr, "In dbopen function: imposible read	 the node from file\n");
		return NULL;
	}
	free(metaData);

	return (DB *)newDB;    
}

void print_statistics(DB_IMPL *db) {
	int i;
	printf("\n\t------ Database information ------\n");
	printf("\n");
	printf("--- Current number of blocks: %lu\n", db->curNumOfBlocks);
	printf("--- Maximum number of blocks: %lu\n", db->numOfBlocks);
	printf("--- Chunk size: %lu\n", db->chunkSize);
	printf("--- Database size: %lu\n", db->dbSize);
	printf("--- Degree of tree: %lu\n", db->t);
	printf("--- Bitmask (first 10 elements): ");
	for(i = 0; i < 20; i++) {
		printf("%d", db->mask[i]);
	}
	printf("\n");
	printf("--- Offset of root node in file: %lu\n", db->root->selfOffset);
	printf("--- Number of keys in root node: %lu\n", db->root->n);
	printf("--- Keys in root: \n       ");
	 int j = 0, numCol = 5;
	for(i = 0; i < db->root->n; i++, j++) {
		if (j == numCol) {
		 printf("\n");
		 j = 0;
		}
		printf("%2d->%2lu  ", i, *((size_t *)db->root->keys[i].data));
	}
	printf("\n");
}

void print_node (BTREE *x) {
	int i, numCol = 5;
	printf("\t------ Node information ------\n\n");
	printf("--- Offset of node in file: %lu\n", x->selfOffset);
	printf("--- Number of keys in  node: %lu\n", x->n);
	printf("--- Leaf flag: %lu\n", x->leaf);
	printf("--- Keys in node: \n");
	int j = 0;
	for(i = 0; i < x->n; i++, j++) {
		if (j == numCol) {
		 printf("\n");
		 j = 0;
		}
		printf("%2d->%2lu  ", i, *((size_t *)x->keys[i].data));
	}
	j=0;
	printf("\n--- Values: \n");
	
	for(i = 0; i < x->n; i++, j++) {
		if (j == numCol) {
			printf("\n");
			j = 0;
		}
		printf("%2d->%s  ", i, (char *)x->values[i].data);
	}
	printf("\n");
}

void dbtcpy(DBT *data1, DBT *data2) {
	data1->size = data2->size;
	memcpy(data1->data, data2->data, data1->size);
}

BTREE* splitChild(DB_IMPL *db, BTREE *x,  BTREE *y, size_t i) {
	long j;

	//Create new node
	BTREE *z = allocateNode(db->t, 1);

	//Copy simple information to new node 'z'
	z->leaf = y->leaf;
	z->n = db->t - 1;
	z->selfOffset = -1;

	//Copy second part of y to z ++++++++++++
	for(j = 0; j < db->t-1; j++) {
		dbtcpy(&z->keys[j], &y->keys[j+db->t]);
		dbtcpy(&z->values[j], &y->values[j+db->t]);
	}

	//If not leaf then copy offsetsChildren ++++++++++
	if(!y->leaf) {
		for(j = 0; j < db->t; j++) {
			z->offsetsChildren[j] = y->offsetsChildren[j+db->t];
		}
	}

	//Set new size of y node
	y->n = db->t - 1;

	//Make place for new children in parent node +++++++
	for (j = x->n+1; j > i + 1; j--) {
		x->offsetsChildren[j] = x->offsetsChildren[j-1];
	}

	//Write
	writeInFile(db, z);
	   db->curNumOfBlocks++;
	x->offsetsChildren[i+1] = z->selfOffset;

	//Make place for new key and value ++++++
	for (j = x->n; j > i; j--) {
		dbtcpy(&x->keys[j], &x->keys[j-1]);
		dbtcpy(&x->values[j], &x->values[j-1]);
	}

	//Copy to free place node from son +++++++
	dbtcpy(&x->keys[i], &y->keys[db->t-1]);
	dbtcpy(&x->values[i], &y->values[db->t-1]);

	x->n++;

	writeInFile(db, y);
	writeInFile(db, x);
	return z;
}

void freeNode (BTREE *node) {
	int i;
	for (i = 0; i < node->n; i++) {
		free(node->values[i].data);
		free(node->keys[i].data);
	}
	free(node->keys);
	free(node->values);
	free(node->offsetsChildren);

}

int memcmpwrapp (DBT *value1, DBT *value2) {
	return memcmp(value1->data, value2->data, MIN(value1->size, value2->size));
}

int memcmpwrapp2 (DBT *value1, DBT *value2) {
	if (*((size_t *)value1->data) < *((size_t *)value2->data)) {
		return -1;
	}
	if (*((size_t *)value1->data) > *((size_t *)value2->data)) {
		return 1;
	}
	if (*((size_t *)value1->data) == *((size_t *)value2->data)) {
		return 0;
	}
}
void insertNonfullNode(DB_IMPL *db, BTREE *x, DBT *key, DBT *value) {
	long i = x->n;

	if (x->leaf) {
		if (x->n == 0) {
			dbtcpy(&x->keys[0], key);
			dbtcpy(&x->values[0], value);
		} else {
			while (memcmpwrapp2(key, &x->keys[i-1]) < 0) {
				dbtcpy(&x->keys[i], &x->keys[i-1]);
				dbtcpy(&x->values[i], &x->values[i-1]);
				i--;
				if (!i) {
					break;
				}
			}
			dbtcpy(&x->keys[i], key);
			dbtcpy(&x->values[i], value);
		}
		
		x->n++;
		writeInFile(db, x);

	} else {

		do {
			i--;
			if (i == -1) {
				break;
			}
		} while (i >= 0 && memcmpwrapp2(key, &x->keys[i]) < 0);

		i++;
		BTREE *child = readFromFile(db, x->offsetsChildren[i]);
		if (child->n == 2*db->t - 1) {
			BTREE *newChild = splitChild(db, x, child, i);
			if (memcmpwrapp2(key, &x->keys[i]) > 0) {
				insertNonfullNode(db, newChild, key, value);
			} else {
				insertNonfullNode(db, child, key, value);
			}
			freeNode(newChild);
		} else {
			insertNonfullNode(db, child, key, value);
		}
		freeNode(child);
	}
}

int insertNode(DB_IMPL *db, DBT *key, DBT *value) {
	BTREE *temp = db->root;

	if (db->root->n == 2*db->t - 1) {
		BTREE *s = allocateNode(db->t, 1);
		db->root = s;
		db->root->leaf = 0;
		db->root->n = 0;
		db->root->selfOffset = temp->selfOffset;
		temp->selfOffset = -1;
		writeInFile(db, temp);
		db->root->offsetsChildren[0] = temp->selfOffset;
		writeInFile(db, db->root);
		db->curNumOfBlocks++;
		BTREE *t = splitChild(db, db->root, temp, 0);
		insertNonfullNode(db, s, key, value);
		freeNode(t);
	} else {
		insertNonfullNode(db, temp, key, value);
	}
}

BTREE_POS* searchInTreeInside(DB_IMPL *db, BTREE *x, DBT *key) {
	long i = 0;
	while (i <= x->n-1 && memcmpwrapp2(key, &x->keys[i]) > 0) {
		i++;
	}
	
	if (i>=0 && i <= x->n-1 && memcmpwrapp2(key, &x->keys[i]) == 0) {
		BTREE_POS *result = (BTREE_POS *)malloc(sizeof(BTREE_POS));
		result->node = x;
		result->pos = i;
		return result;
	} else {
		if (x->leaf) {
			return NULL;
		} else {
			
			BTREE *newNode = readFromFile(db, x->offsetsChildren[i]);
			if (x != db->root) {
				freeNode(x);
			}
			return searchInTreeInside(db, newNode, key);
		}
	}
}

BTREE_POS* searchInTree(DB_IMPL *db, DBT *key) {
   return searchInTreeInside(db, db->root, key );
}

int dbclose (DB *dbForClosing) {
	DB_IMPL *db = (DB_IMPL *)dbForClosing;
	freeNode(db->root);
	free(db->mask);
	free(db->buf);
	close(db->fileDescriptor);
	free(db);
}

void printBTREE (DB_IMPL *db, BTREE *x) {
	BTREE *child;
	size_t i;
	//FILE *fp = fopen("btree", "w");
	printf("\t--------- START ROOT OF SUBTREE -------\n");
	print_node(x);
	printf("\t----------- START ITS CHILDREN --------\n");
	for(i = 0; i < x->n + 1; i++) {
		child = readFromFile(db, x->offsetsChildren[i]);
		printf("\n\t ------ IT'S %lu CHILD --------\n", i);
		print_node(child);
		freeNode(child);
	}
	
		for (i = 0; i <x->n+1; i++) {
			child = readFromFile(db, x->offsetsChildren[i]);
			
			printf("\t----- CHILDREN NUM: %lu ------\n", i);
			if (!child->leaf) {
			printBTREE(db, child);
			}
			freeNode(child);
		}
	
		printf("\n\t--------- END OF TREE --------- \n\n\n");
	
}

int main (void) {
	printf("\n--------------------------------------------------\n");
	printf("--------------- NEW RUN OF PROGRAM ---------------\n");
	printf("--------------------------------------------------\n");
	
	DBC conf;
	conf.db_size = 512 * 1024 * 1024;
	conf.chunk_size = 4096;
	conf.mem_size = 16 * 1024 * 1024;

	struct DB_IMPL * myDB = (DB_IMPL *)dbcreate("database.db", &conf);
	if (myDB == NULL) {
		fprintf(stderr, "Fatal error: imposible create database\n");
		unlink("database.db");
		return -1;
	}
	DBT key;
	DBT value;
	byte str[9] = "abcdefgh";
	size_t i, r, y;
	
	key.data = (size_t *)malloc(sizeof(size_t));
	key.size = sizeof(size_t);
	value.data = (byte *)malloc(9);
	value.size = 9;
	
	srand ( time(NULL) );
	for(i = 0; i < 1000; i++) {
		r = rand() % 1000000; 
		if (i == 12) {
			y = r;
			printf("\tVALUEDATA: %s", (char *)value.data);
		}
		memcpy(value.data, str, value.size);
		memcpy(key.data, &r, key.size);
		insertNode(myDB, &key, &value);
	}
	
	print_statistics(myDB);
	
	memcpy(key.data, &y, key.size);
	BTREE_POS *res = searchInTree(myDB, &key);
	if (res == NULL) {
		printf("There isn't key in this database\n");
		close(myDB->fileDescriptor);
		    unlink("database.db");
		return 0;
	}
	print_node(res->node);
	printf("---- Key: %lu -----\n", y);
	printf("---- Position in node: %lu -----\n", res->pos);
	close(myDB->fileDescriptor);
	unlink("database.db");
	
	return 0;
}
