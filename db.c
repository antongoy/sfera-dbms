#include "db.h"

int db_close(struct DB *db) {
	db->close(db);
}

int db_del(struct DB *db, void *key, size_t key_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	return db->del(db, &keyt);
}

int db_get(struct DB *db, void *key, size_t key_len, void **val, size_t *val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {0, 0};
	int rc = db->get(db, &keyt, &valt);
	*val = valt.data;
	*val_len = valt.size;
	return rc;
}

int db_put(struct DB *db, void *key, size_t key_len,
	void *val, size_t val_len) {
	struct DBT keyt = {
		.data = key,
		.size = key_len
	};
	struct DBT valt = {
		.data = val,
		.size = val_len
	};
	return db->put(db, &keyt, &valt);
}



void setBitTrue(byte *b, int pos) {
	byte mask = 1;
	mask <<= pos - 1;
	*b |= mask;
}

void setBitFalse(byte *b, int pos) {
	byte mask = 1;
	mask <<= pos - 1;
	mask = ~mask;
	*b &= mask;
}

int power(int a, int n) {
	int i;
	int product = 1;
	if (n == 0) {
		return 1;
	}
	
	for(i = 0; i < n; i++) {
		product *= a;
	}
	return product;
}

int findTrueBit(byte *b) {
	byte mask;
	byte addmask;
	int i;
	
	for (i = 7; i >= 0; i--) {
		mask = ~(*b);
		addmask = power(2, i);
		mask |= addmask;
		mask &= *b;
		if (mask == 0) {
			break;
		}
		
	}
	
	return i + 1;
}

struct BTREE* readFromFile(struct DB_IMPL *db, size_t offset) {
	size_t i;
	struct BTREE *newNode = allocateNode(db->t, 1);

	//Move to start position of node
	if (lseek(db->fileDescriptor, offset, 0) < 0) {
		fprintf(stderr, "In readFromFile function: lseek() error\n");
		return NULL;
	}

	//Read to buffer
	if(read(db->fileDescriptor, db->buf, db->chunkSize) != db->chunkSize) {
		fprintf(stderr, "In readFromFile function: read() error\n");
		return NULL;
	}

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
		ptr += MAX_SIZE_VALUE - sizeof(size_t);
	}
	
	return newNode;

}

int myround(double f) {
	int r = (int)f;
	return r + 1;
}

void removeFromFile(struct DB_IMPL *db, struct BTREE *x) {
	int numOfBytesForMask = db->numOfBlocks / BYTE_SIZE;
	int m = myround(numOfBytesForMask / db->chunkSize);
	int pos_in_mask = (x->selfOffset - (m*db->chunkSize + db->chunkSize)) / db->chunkSize;
	int pos_in_byte = pos_in_mask;
	int num_of_byte = pos_in_mask;
	num_of_byte /= BYTE_SIZE;
	pos_in_byte %= BYTE_SIZE;
	pos_in_byte = BYTE_SIZE - pos_in_byte;
	setBitFalse(&db->mask[num_of_byte], pos_in_byte);
}

int writeInFile (struct DB_IMPL *db, struct BTREE *node) {
	size_t i, j;

	// If selfOffset = -1 then node hasn't written yet
	if (node->selfOffset == -1) {

		//Find free space in file - first bit that is set to zero
		for (j = 0; (i = findTrueBit(&(db->mask[j]))) == 0 && j <  db->numOfBlocks; j++);
		
		//We can exceed size of mask. Then database is full
		if (j == db->numOfBlocks) {
			return -1;
		}
		
		int numOfBytesForMask = db->numOfBlocks / BYTE_SIZE;

		//Calculate new selfOffset
		node->selfOffset = (j * BYTE_SIZE + 8 - i) * db->chunkSize + myround(numOfBytesForMask / db->chunkSize) * db->chunkSize + db->chunkSize;

		//Move to start of file and change curNumBlocks
		if (lseek(db->fileDescriptor, 0, 0) == -1) {
			fprintf(stderr, "In function writeInFile: it's impossible to move to beginning of the file\n");
			return -1;
		}

		if (write(db->fileDescriptor, &db->curNumOfBlocks, sizeof(size_t)) != sizeof(size_t)) {
			fprintf(stderr, "In function writeInFile: System call write() generated the error\n");
			return -1;
		}

		//Move to bit which is responsible for new node
		int offsetInMetadata = db->chunkSize + j;
		if (lseek(db->fileDescriptor, offsetInMetadata, 0) == -1) {
			fprintf(stderr, "In function writeInFile: System call lseek() generated the error\n");
			return -1;
		}
		//printf("BEFORE: %uc",db->mask[j] )
		setBitTrue(&(db->mask[j]), i);
		
		if (write(db->fileDescriptor, &(db->mask[j]), sizeof(byte)) == -1) {
			fprintf(stderr, "In function writeInFile: System call write() generated the error\n");
			return -1;
		}
		
	}

	//Auxiliary pointer 'ptr' for copying data in 'buf'
	//byte *ptr = db->buf;
	size_t k = 0;
	memset(db->buf, 0, db->chunkSize);
	//Copy header with block metadata
	memcpy(db->buf + k, &(node->selfOffset), sizeof(size_t));
	k += sizeof(size_t);
	memcpy(db->buf + k, &(node->leaf), sizeof(size_t));
	k += sizeof(size_t);
	memcpy(db->buf + k, &(node->n), sizeof(size_t));
	k += sizeof(size_t);

	//Copy 'offsetsChildren'
	int n = 2*db->t;
	memcpy(db->buf + k, node->offsetsChildren, sizeof(size_t)*n);
	k += sizeof(size_t) * n;

	//Copy 'keys' and 'values'
	
	for(i = 0; i < n - 1; i++) {
		memcpy(db->buf + k, &(node->keys[i].size), sizeof(size_t));
		k += sizeof(size_t);
		memcpy(db->buf + k, node->keys[i].data, MAX_SIZE_KEY - sizeof(size_t));
		k += MAX_SIZE_KEY - sizeof(size_t);
		memcpy(db->buf + k, &(node->values[i].size), sizeof(size_t));
		k += sizeof(size_t);
		memcpy(db->buf + k, node->values[i].data, MAX_SIZE_VALUE - sizeof(size_t));
		k +=  MAX_SIZE_VALUE - sizeof(size_t);
	}

	//Write in file node
	if (lseek(db->fileDescriptor, node->selfOffset, 0) == -1) {
		fprintf(stderr, "In function writeInFile: System call lseek() generated the error\n");
		return -1;
	}

	if(write(db->fileDescriptor, db->buf, db->chunkSize) != db->chunkSize) {
		fprintf(stderr, "In function writeInFile: System call write() generated the error\n");
		return -1;
	}
	return 0;
}

struct BTREE* allocateNode(size_t t, size_t leaf) {
	size_t i;
	size_t n = 2 * t;

	struct BTREE * root = (struct BTREE *)malloc(sizeof(struct BTREE));
	if (root == NULL) {
		fprintf(stderr, "In allocateNode function: Memory allocation error (root).\n");
		return NULL;
	}
	
	root->n = 0;
	root->selfOffset = -1;
	root->leaf = leaf;

	root->offsetsChildren = (size_t *)malloc(sizeof(size_t)*n);
	if (root->offsetsChildren == NULL) {
		fprintf(stderr, "In allocateNode function: Memory allocation error (root->offsetsChildren).\n");
		return NULL;
	}
	
	memset(root->offsetsChildren, 0, sizeof(size_t)*n);
	root->keys = (struct DBT *)malloc(sizeof(struct DBT)*(n - 1));
	if (root->keys == NULL) {
		fprintf(stderr, "In allocateNode function: Memory allocation error (root->keys).\n");
		return NULL;
	}
	
	root->values = (struct DBT *)malloc(sizeof(struct DBT)*(n - 1));
	if (root->values == NULL) {
		fprintf(stderr, "In allocateNode function: Memory allocation error (root->values).\n");
		return NULL;
	}
	
	for (i = 0; i < n - 1; i++) {
		root->values[i].size = 0;
		root->keys[i].size = 0;
		root->values[i].data = (byte *)malloc(sizeof(byte)*(MAX_SIZE_VALUE - sizeof(size_t)));
		if (root->values[i].data == NULL) {
			fprintf(stderr, "In allocateNode function: Memory allocation error (root->values[%lu].data).\n", i);
			return NULL;
		}
		
		memset(root->values[i].data, 0, sizeof(byte)*(MAX_SIZE_VALUE - sizeof(size_t)));
		root->keys[i].data = (byte *)malloc(sizeof(byte)*(MAX_SIZE_KEY - sizeof(size_t)));
		if (root->keys[i].data == NULL) {
			fprintf(stderr, "In allocateNode function: Memory allocation error (root->keys[%lu].data).\n", i);
			return NULL;
		}
		memset(root->keys[i].data, 0, sizeof(byte)*(MAX_SIZE_KEY - sizeof(size_t)));
	}
	return root;
} 

struct DB* dbcreate(const char *file, const struct DBC conf) {
	long fd; 
	size_t i;

	if ((fd = open(file, O_CREAT|O_RDWR|O_TRUNC,  S_IRWXU)) < 0) {
		fprintf(stderr, "In dbcreate function: Impossible creating new file\n");
		return NULL;
	}

	//Allocate memory for new Database
	struct DB_IMPL * newDB = (struct DB_IMPL *)malloc(sizeof(struct DB_IMPL));
	if (newDB == NULL) {
		fprintf(stderr, "In dbcreate function: Memory wasn't allocated for newDB\n");
		return NULL;
	}
	
	newDB->put = &insertNode;
	newDB->get = &searchInTree;
	newDB->close = &dbclose;
	newDB->del = &deleteKey;
	
	//Fill simple fields in newDB structure
	newDB->fileDescriptor = fd;
	newDB->curNumOfBlocks = 0;
	newDB->chunkSize = conf.chunk_size;
	newDB->dbSize = conf.db_size;

	//Calculating auxiliary variable 'temp', which is needed for the evaluating 't' - degree of B-tree
	int temp = (conf.chunk_size - 3 * sizeof(size_t)) / (sizeof(size_t) + MAX_SIZE_KEY + MAX_SIZE_VALUE);
	temp = temp % 2 == 0 ? temp - 1 : temp;
	newDB->t = (temp + 1) / 2;

	//Calculating auxiliary variable 'm', which is needed for the evaluating 'numOfBlocks'is max number of nodes that stored in file
	double m = ((conf.db_size / conf.chunk_size) - 1) / ((double)(conf.chunk_size + 1));
	newDB->numOfBlocks = m * conf.chunk_size;
	//Allocate memory for bit mask - it shows free spaces in file
	newDB->mask = (byte *)malloc(newDB->numOfBlocks / 8);
	memset(newDB->mask, 0, newDB->numOfBlocks / 8);
	//newDB->mask = (byte *)malloc(sizeof(byte)*newDB->numOfBlocks);
	//memset(newDB->mask, 0, newDB->numOfBlocks);
	
	//Memory for auxiliary buffer. It is needed for writing in file
	newDB->buf = (byte *)malloc(sizeof(byte) * newDB->chunkSize);
	memset(newDB->buf, 0, newDB->chunkSize);
	//Move to start of file
	if (lseek(newDB->fileDescriptor, 0, 0) == -1) {
		fprintf(stderr, "In dbcreate function: system call lseek throws the error\n");
		return NULL;
	}

	//'metaData' is auxiliary buffer for database metadata
	size_t *metaData = (size_t *)malloc(DB_HEADER_SIZE);
	//size_t *p = metaData;

	//Copy to buffer database metadata
	size_t k = 0;
	memcpy(metaData + k, &(newDB->curNumOfBlocks), sizeof(size_t));
	k ++;
	memcpy(metaData + k, &(newDB->t), sizeof(size_t));
	k ++;    
	memcpy(metaData + k, &(newDB->chunkSize), sizeof(size_t));
	k ++; 
	memcpy(metaData + k, &(newDB->numOfBlocks), sizeof(size_t));
	
	
	//Write in file database metadata
	if(write(newDB->fileDescriptor, metaData, DB_HEADER_SIZE) != DB_HEADER_SIZE) {
		fprintf(stderr, "In dbcreate function: system call write() throws the error\n");
		return NULL;
	}

	//Allocate memory for root node
	newDB->root = allocateNode(newDB->t, 1);
	if (newDB->root == NULL) {
		fprintf(stderr, "In dbcreate function: Don't create new node\n");
		return NULL;
	}
	

	// -1 is mean that this node hasn't written in file yet
	newDB->root->selfOffset = -1;

	//We have new node!
	newDB->curNumOfBlocks++;

	//No comments
	if(writeInFile(newDB, newDB->root) < 0) {
		fprintf(stderr, "In dbcreate function: Error in the writeInFile()\n");
		return NULL;
	}

	free(metaData);
	return (struct DB *)newDB;
}

struct DB* dbopen (const char *file) {
	 long fd; 
	 size_t i;
	
	if ((fd = open(file, O_RDWR, S_IRWXU)) == -1) {
		fprintf(stderr, "In dbopen function: imposible open database\n");
		return NULL;
	}
	
	struct DB_IMPL *newDB = (struct DB_IMPL *)malloc(sizeof(struct DB_IMPL));
	if (newDB == NULL) {
		fprintf(stderr, "In dbopen function: Memory wasn't allocated for newDB\n");
		return NULL;
	}
	
	newDB->put = &insertNode;
	newDB->get = &searchInTree;
	newDB->close = &dbclose;
	

	newDB->fileDescriptor = fd;

	//Metadata buffer for database header
	size_t *metaData = (size_t *)malloc(DB_HEADER_SIZE);
	if (metaData == NULL) {
		fprintf(stderr, "In dbopen function: Memory wasn't allocated for metaData\n");
		return NULL;
	}
	
	if (lseek(newDB->fileDescriptor, 0, 0) < 0) {
		fprintf(stderr, "In dbopen function: lseek() error\n");
		return NULL;
	}
	
	if (read(newDB->fileDescriptor, metaData, DB_HEADER_SIZE) !=  DB_HEADER_SIZE) {
		fprintf(stderr, "In dbopen function: imposible read data\n");
		return NULL;
	}

	//Parse metadata
	size_t k = 0;
	memcpy(&(newDB->curNumOfBlocks), metaData + k, sizeof(size_t));
	k++;
	memcpy(&(newDB->t), metaData + k, sizeof(size_t));
	k++;
	memcpy(&(newDB->chunkSize), metaData + k, sizeof(size_t));
	k++;
	memcpy(&(newDB->numOfBlocks), metaData + k, sizeof(size_t));

	//Allocate memory
	newDB->buf = (byte *)malloc(sizeof(byte)*newDB->chunkSize);
	memset(newDB->buf, 0, newDB->chunkSize);
	if (newDB->buf == NULL) {
		fprintf(stderr, "In dbopen function: : Memory wasn't allocated for newDB->buf\n");
		return NULL;
	}
	
	newDB->mask = (byte *)malloc(newDB->numOfBlocks / BYTE_SIZE);

	if (newDB->mask == NULL) {
		fprintf(stderr, "In dbopen function: : Memory wasn't allocated for newDB->mask\n");
		return NULL;
	}
	
	memset(newDB->mask, 0, newDB->numOfBlocks / BYTE_SIZE); 
	
	//Read bit mask from file
	byte * p = newDB->mask;

	if (lseek(newDB->fileDescriptor, newDB->chunkSize, 0) == -1) {
		fprintf(stderr, "In dbopen function: system call lseek() throws the error\n");
		return NULL;
	}
	
	long m = myround((newDB->numOfBlocks / BYTE_SIZE)/newDB->chunkSize);
	long l = (newDB->numOfBlocks / BYTE_SIZE) - (m - 1) * newDB->chunkSize;
	
	if (read(newDB->fileDescriptor, p, newDB->chunkSize*(m - 1) + l) == -1) {
		fprintf(stderr, "In dbopen function: imposible read from database \n");
		return NULL;
	}

	newDB->root = readFromFile(newDB, m*newDB->chunkSize + newDB->chunkSize);

	if (newDB->root == NULL) {
		fprintf(stderr, "In dbopen function: imposible read the node from file\n");
		return NULL;
	}
	free(metaData);

	return (struct DB *)newDB;    
}

void print_statistics(struct DB_IMPL *db) {
	int i;
	printf("\n\t------ Database information ------\n");
	printf("\n");
	printf("--- Current number of blocks: %lu\n", db->curNumOfBlocks);
	printf("--- Maximum number of blocks: %lu\n", db->numOfBlocks);
	printf("--- Chunk size: %lu\n", db->chunkSize);
	printf("--- Database size: %lu\n", db->dbSize);
	printf("--- Degree of tree: %lu\n", db->t);
	printf("--- Bitmask (first 10 elements): ");
	for(i = 0; i < 10; i++) {
		printf("%hhu ", db->mask[i]);
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

void print_node (struct BTREE *x) {
	int i, numColKeys = 4, numColValue = 2;
	//printf("\n\t====================  NODE INFORMATION ==================== \n\n");
	printf("\n\t\t Offset of node in file: %lu\n", x->selfOffset);
	printf("\t\t Number of keys in  node: %lu\n", x->n);
	printf("\t\t Leaf flag: %lu\n", x->leaf);
	printf("\t\t Keys in node: \n\t\t");
	
	int j = 0;
	for(i = 0; i < x->n; i++, j++) {
		if (j == numColKeys) {
		 printf("\n\t\t");
		 j = 0;
		}
		*((char *)x->keys[i].data + x->keys[i].size) = '\0';
		printf("%2d->%s  ", i, (char *)x->keys[i].data);
	}
	
	j=0;
	printf("\n\t\t Values: \n\t\t");
	
	for(i = 0; i < x->n; i++, j++) {
		if (j == numColValue) {
			printf("\n\t\t");
			j = 0;
		}
		printf("%2d----%s  ", i, (char *)(x->values[i].data));
	}
	printf("\n");
}

void dbtcpy(struct DBT *data1, const struct DBT *data2) {
	data1->size = data2->size;
	memcpy(data1->data, data2->data, data1->size);
}

struct BTREE* splitChild(struct DB_IMPL *db, struct BTREE *x,  struct BTREE *y, size_t i) {
	long j;
	//Create new node
	struct BTREE *z = allocateNode(db->t, 1);
	if (z == NULL) {
		fprintf(stderr, "In splitChild function: Don't create new node\n");
		return NULL;
	}
	
	//Copy simple information to new node 'z'
	z->leaf = y->leaf;
	z->n = db->t - 1;
	z->selfOffset = -1;

	//Copy second part of y to z ++++++++++++
	for(j = 0; j < db->t-1; j++) {
		dbtcpy(&(z->keys[j]), &(y->keys[j+db->t]));
		dbtcpy(&(z->values[j]), &(y->values[j+db->t]));
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
	db->curNumOfBlocks++;
	if (writeInFile(db, z) < 0) {
		fprintf(stderr, "In splitChild function: error in the writeInFile()\n");
		return NULL;
	}
	
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
	if (writeInFile(db, y) < 0) {
		fprintf(stderr, "In splitChild function: error in the writeInFile()\n");
		return NULL;
	}
	
	if (writeInFile(db, x) < 0) {
		fprintf(stderr, "In splitChild function: error in the writeInFile()\n");
		return NULL;
	}
	
	return z;
}

void freeNode (struct DB_IMPL *db, struct BTREE *node) {
	int i;
	for (i = 0; i < 2*db->t - 1; i++) {
		free(node->values[i].data);
		free(node->keys[i].data);
	}
	free(node->keys);
	free(node->values);
	free(node->offsetsChildren);
	free(node);

}

int memcmpwrapp (const struct DBT *value1, const struct DBT *value2) {
	return memcmp(value1->data, value2->data, MIN(value1->size, value2->size));
}

int memcmpwrapp2 (const struct DBT *value1, const struct DBT *value2) {
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

void insertNonfullNode(struct DB_IMPL *db, struct BTREE *x, struct DBT *key, const struct DBT *value) {
	long i = x->n;

	if (x->leaf) {
		if (x->n == 0) {
			dbtcpy(&(x->keys[0]), key);
			dbtcpy(&(x->values[0]), value);
		} else {
			while (memcmpwrapp(key, &(x->keys[i-1])) < 0) {
				dbtcpy(&(x->keys[i]), &(x->keys[i-1]));
				dbtcpy(&(x->values[i]), &(x->values[i-1]));
				i--;
				if (!i) {
					break;
				}
			}
			dbtcpy(&(x->keys[i]), key);
			dbtcpy(&(x->values[i]), value);
		}
		
		x->n++;
		writeInFile(db, x);

	} else {

		do {
			i--;
			if (i == -1) {
				break;
			}
		} while (i >= 0 && memcmpwrapp(key, &x->keys[i]) < 0);

		i++;
		struct BTREE *child = readFromFile(db, x->offsetsChildren[i]);
		if (child->n == 2*db->t - 1) {
			struct BTREE *newChild = splitChild(db, x, child, i);
			if (memcmpwrapp(key, &x->keys[i]) > 0) {
				insertNonfullNode(db, newChild, key, value);
			} else {
				insertNonfullNode(db, child, key, value);
			}
			freeNode(db, newChild);
		} else {
			insertNonfullNode(db, child, key, value);
		}
		freeNode(db,child);
	}
}


void printBTREE (struct DB_IMPL *db, struct BTREE *x, int depth, int n) {
	struct BTREE *child;
	size_t i;
	
	printf("\n=============================================================================================\n");
	printf("================================= START ROOT OF SUBTREE ======================================\n");
	printf("=================================== DEPTH: %d NUM: %d ==========================================\n", depth, n);
	print_node(x);
	printf("\n=============================================================================================\n");
	printf("=================================== START ITS CHILDREN =======================================\n");
	printf("=============================================================================================\n");
	for(i = 0; i < x->n + 1; i++) {
		if (x->n == 0) {
			break;
		}
		
		child = readFromFile(db, x->offsetsChildren[i]);
		if(x->leaf == 0) {
			printf("\n========================================= IT'S %lu CHILD =======================================\n", i);
			print_node(child);
			freeNode(db, child);
		}
	}
	
	for (i = 0; i <x->n+1; i++) {
		if (x->n == 0) {
			break;
		}
		child = readFromFile(db, x->offsetsChildren[i]);
		if (child->leaf == 0) {
			printBTREE(db, child, depth + 1, i);
			freeNode(db,child);
		}
	}
	printf("\n=================================================================================================\n");
	printf("============================================= END OF TREE =======================================\n");
	printf("=================================================================================================\n\n");
}


int insertNode(struct DB *aux_db, struct DBT *key, const struct DBT *value) {
	struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
	struct BTREE *temp = db->root;
	
	//printBTREE(db, db->root, 0 , 0);

	if (temp->n == 2*db->t - 1) {
		struct BTREE *s = allocateNode(db->t, 1);
		db->root = s;
		db->root->leaf = 0;
		db->root->n = 0;
		db->root->selfOffset = temp->selfOffset;
		temp->selfOffset = -1;
		writeInFile(db, temp);
		db->root->offsetsChildren[0] = temp->selfOffset;
		db->curNumOfBlocks++;
		writeInFile(db, db->root);
		struct BTREE *t = splitChild(db, db->root, temp, 0);
		freeNode(db, temp);
		insertNonfullNode(db, s, key, value);
		freeNode(db,t);
		return 0;
	} else {
		insertNonfullNode(db, temp, key, value);
		return 0;
	}
}

int searchInTreeInside(struct DB_IMPL *db, struct BTREE *x, struct DBT *key, struct DBT *value) {
	long i = 0;
	//print_node(x);
	for (i = 0; memcmpwrapp(key, &x->keys[i]) > 0; i++) {
		if (i == x->n) {
			break;
		}
		
	}
	if (i != x->n) {
		if (i>=0 && i <= x->n-1 && memcmpwrapp(key, &x->keys[i]) == 0) {
			dbtcpy(value, &(x->values[i]));
			if (x != db->root) {
				freeNode(db,x);
			}
			return 0;
		} else {
			if (x->leaf) {
				if (x != db->root) {
					freeNode(db,x);
				}
				return -1;
			} else {
				
				struct BTREE *newNode = readFromFile(db, x->offsetsChildren[i]);
				if (x != db->root) {
					freeNode(db,x);
				}
				return searchInTreeInside(db, newNode, key, value);
			}
		}
	} else {
		if (x->leaf) {
			if (x != db->root) {
				freeNode(db,x);
			}
			return -1;
		} else {
			struct BTREE *newNode = readFromFile(db, x->offsetsChildren[i]);
			if (x != db->root) {
				freeNode(db,x);
			}
			return searchInTreeInside(db, newNode, key, value);
		}
	}
}

int searchInTree(struct DB *aux_db, struct DBT *key, struct DBT *value) {
	struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
	value->data = (byte *)malloc(MAX_SIZE_VALUE);
	return searchInTreeInside(db, db->root, key, value);
}

int dbclose (struct DB *dbForClosing) {
	struct DB_IMPL *db = (struct DB_IMPL *)dbForClosing;
	freeNode(db, db->root);
	free(db->mask);
	free(db->buf);
	close(db->fileDescriptor);
	free(db);
	return 0;
}

void valueGen(char * str, size_t size) {
	size_t i;
	
	for (i = 0; i < size; i++) {
		str[i] = 'a' + rand() % 26;
	}
	
}

struct DBT * allocateDBT (size_t size) {
	struct DBT *dbt = (struct DBT *)malloc(sizeof(struct DBT));
	dbt->size = size;
	dbt->data = (size_t *)malloc(size);
	return dbt;
}

void freeDBT (struct DBT *ptr) {
	free(ptr->data);
	free(ptr);
}

void merge_nodes(struct DB_IMPL *db, struct BTREE *y, const struct DBT *key, 
		const struct DBT *value, struct BTREE *z) {
	size_t j;
	
	dbtcpy(&y->keys[db->t], key);
	dbtcpy(&y->values[db->t], value);
	for(j = 1; j < db->t; j++) {
		dbtcpy(&y->keys[j+db->t], &z->keys[j]);
		dbtcpy(&y->values[j+db->t], &z->values[j]);
	}
}

int posInNode(struct BTREE *x, const struct DBT *key) {
	size_t i = 0;
	for(i = 0; memcmpwrapp(key, &x->keys[i]) > 0; i++) {
		if(i == x->n - 1) {
			i++;
			break;
		}
	}
	
	return i;
}

void deleteFromLeaf(struct DB_IMPL *db,  struct BTREE *x, const struct DBT *key, size_t pos) {
	size_t i;
	
	for(i = pos; i < x->n - 1; i++) {
		dbtcpy(&x->keys[i], &x->keys[i+1]);
		dbtcpy(&x->values[i], &x->values[i+1]);
	}
	
	x->n--;
}

void keysSwapDirect(struct BTREE *cur_child, int cur_pos, struct BTREE *parent, struct BTREE *next_child) {
	dbtcpy(&cur_child->keys[cur_child->n], &parent->keys[cur_pos]);
	dbtcpy(&cur_child->values[cur_child->n], &parent->values[cur_pos]);
	
	cur_child->n++;
	
	dbtcpy(&parent->keys[cur_pos], &next_child->keys[0]);
	dbtcpy(&parent->values[cur_pos], &next_child->values[0]);
	
	if (!cur_child->leaf) {
		cur_child->offsetsChildren[cur_child->n] = next_child->offsetsChildren[0];
	}
	
	size_t i;
	for (i = 0; i < next_child->n - 1; i++) {
		dbtcpy(&next_child->keys[i], &next_child->keys[i+1]);
		dbtcpy(&next_child->values[i], &next_child->values[i+1]);
	}
	
	if (!next_child->leaf) {
		for (i = 0; i < next_child->n; i++) {
			next_child->offsetsChildren[i] = next_child->offsetsChildren[i+1];
		}
	}
	
	next_child->n--;
	
	
}

void keysSwapUndirect(struct BTREE *prev_child, int prev_pos,  struct BTREE *parent, struct BTREE *cur_child) {
	size_t i;
	
	for (i = cur_child->n; i > 0; i++) {
		dbtcpy(&cur_child->keys[i], &cur_child->keys[i-1]);
		dbtcpy(&cur_child->values[i], &cur_child->values[i-1]);
	}
	
	if (!cur_child->leaf) {
		for (i = cur_child->n + 1; i > 0; i++) {
			cur_child->offsetsChildren[i] = cur_child->offsetsChildren[i-1];
		}
	}
	
	dbtcpy(&cur_child->keys[0], &parent->keys[prev_pos]);
	dbtcpy(&cur_child->values[0], &parent->values[prev_pos]);
	
	cur_child->n++;
	
	dbtcpy(&parent->keys[prev_pos], &prev_child->keys[0]);
	dbtcpy(&parent->values[prev_pos], &prev_child->values[0]);
	
	if (!cur_child->leaf) {
		cur_child->offsetsChildren[0] = prev_child->offsetsChildren[prev_child->n];
	}
	
	prev_child->n--;
	
}

int deleteKeyInside (struct DB_IMPL *db, struct BTREE *x, const struct DBT *key) {
	size_t i,j;
	printf("HEY\n");
	i = posInNode(x, key);
	
	if (i != 0 && i != x->n) {
		if (memcmpwrapp(&x->keys[i], key) == 0) { /* Key is in this node */
			// x is leaf
			if (x->leaf) { 
				deleteFromLeaf(db, x, key, i);
				writeInFile(db, x);
				return 0;
			} else { /* x is intrenal node */
				struct BTREE *y = readFromFile(db, x->offsetsChildren[i]);
				// Go to left branch 
				if(y->n >= db->t) {
					struct DBT *save_key = allocateDBT(y->keys[y->n - 1].size);
					struct DBT *save_value = allocateDBT(y->values[y->n - 1].size);
					dbtcpy(save_key, &y->keys[y->n - 1]);
					dbtcpy(save_value, &y->values[y->n - 1]);
					
					deleteKeyInside(db, y, save_key);
					
					dbtcpy(&x->keys[i], save_key);
					dbtcpy(&x->values[i], save_value);
					
					writeInFile(db, x);
				
					freeNode(db, y);
					freeDBT(save_value);
					freeDBT(save_key);
					return 0;
				}
				
				struct BTREE *z = readFromFile(db, x->offsetsChildren[i+1]);
				//Go to the right branch
				if (z->n >= db->t) {
					struct DBT *save_key = allocateDBT(z->keys[0].size);
					struct DBT *save_value = allocateDBT(z->values[0].size);
					dbtcpy(save_key, &z->keys[0]);
					dbtcpy(save_value, &z->values[0]);
					
					deleteKeyInside(db, z, save_key);
					
					dbtcpy(&x->keys[i], save_key);
					dbtcpy(&x->values[i], save_value);
					
					writeInFile(db, x);
					
					freeNode(db, y);
					freeNode(db, z);
					freeDBT(save_value);
					freeDBT(save_key);
					return 0;
				}
				
				// Both child nodes have t-1 keys
				size_t j;
				merge_nodes(db, y, &x->keys[i], &x->values[i], z);
				
				removeFromFile(db, z);
				freeNode(db, z);
				
				for(j = i; j < x->n - 1; j++) {
					dbtcpy(&x->keys[j], &x->keys[j+1]);
					dbtcpy(&x->values[j], &x->values[j+1]);
				}
				
				for(j = i+1; j < x->n; j++) {
					x->offsetsChildren[j] = x->offsetsChildren[j+1];
				}
				
				x->n--;
				
				deleteKeyInside(db, y, key);
				
				if (x == db->root && x->n == 0) {
					removeFromFile(db, y);
					removeFromFile(db, db->root);
					y->selfOffset = db->root->selfOffset;
					freeNode(db, db->root);
					db->root = y;
					writeInFile(db, y);
				} else {
					writeInFile(db, x);
					freeNode(db, y);
				}
				
				return 0;
			}
		} else { // Key is not in the node
			if (x->leaf) {
				return -1;
			}
			struct BTREE *y = readFromFile(db, x->offsetsChildren[i]);
			if (y->n == db->t - 1) {
				struct BTREE *z = readFromFile(db, x->offsetsChildren[i+1]);
				if (z->n >= db->t) {
					keysSwapDirect(y, i, x, z);
					writeInFile(db, x);
					writeInFile(db, z);
					freeNode(db, z);
					int ret = deleteKeyInside(db, y, key);
					freeNode(db, y);
					return ret;
				} else {
					struct BTREE *w = readFromFile(db, x->offsetsChildren[i-1]);
					if (w->n >= db->t) {
						keysSwapUndirect(w, i - 1, x, y);
						writeInFile(db, x);
						writeInFile(db, w);
						freeNode(db, z);
						freeNode(db, w);
						int ret = deleteKeyInside(db, y, key);
						freeNode(db, y);
						return ret;
					}
					
					merge_nodes(db, y, &x->keys[i], &x->values[i], z);
					removeFromFile(db, z);
					freeNode(db, z);
					
					size_t j;
					for(j = i-1; j < x->n - 1; j++) {
						dbtcpy(&x->keys[j], &x->keys[j+1]);
						dbtcpy(&x->values[j], &x->values[j+1]);
					}
					
					for(j = i; j < x->n; j++) {
						x->offsetsChildren[j] = x->offsetsChildren[j+1];
					}
					
					x->n--;
					
					int ret = deleteKeyInside(db, y, key);
					
					if (x == db->root && x->n == 0) {
						removeFromFile(db, y);
						y->selfOffset = db->root->selfOffset;
						removeFromFile(db, db->root);
						freeNode(db, db->root);
						db->root = y;
						writeInFile(db, y);
					} else {
						writeInFile(db, x);
						freeNode(db, y);
					}
					return ret;
				}
			} else {
				int ret = deleteKeyInside(db, y, key);
				freeNode(db, y);
				return ret;
			}
		}
	} else {
		if (i == 0) {
			if (memcmpwrapp(&x->keys[i], key) == 0) {
				if (x->leaf) { 
					deleteFromLeaf(db, x, key, i);
					writeInFile(db, x);
					return 0;
				} else {
					struct BTREE *y = readFromFile(db, x->offsetsChildren[i]);
					// Go to left branch 
					if(y->n >= db->t) {
						struct DBT *save_key = allocateDBT(y->keys[y->n - 1].size);
						struct DBT *save_value = allocateDBT(y->values[y->n - 1].size);
						dbtcpy(save_key, &y->keys[y->n - 1]);
						dbtcpy(save_value, &y->values[y->n - 1]);
						
						deleteKeyInside(db, y, save_key);
						
						dbtcpy(&x->keys[i], save_key);
						dbtcpy(&x->values[i], save_value);
						
						writeInFile(db, x);
					
						freeNode(db, y);
						freeDBT(save_value);
						freeDBT(save_key);
						return 0;
					}
					
					struct BTREE *z = readFromFile(db, x->offsetsChildren[i+1]);
					//Go to the right branch
					if (z->n >= db->t) {
						struct DBT *save_key = allocateDBT(z->keys[0].size);
						struct DBT *save_value = allocateDBT(z->values[0].size);
						dbtcpy(save_key, &z->keys[0]);
						dbtcpy(save_value, &z->values[0]);
						
						deleteKeyInside(db, z, save_key);
						
						dbtcpy(&x->keys[i], save_key);
						dbtcpy(&x->values[i], save_value);
						
						writeInFile(db, x);
						
						freeNode(db, y);
						freeNode(db, z);
						freeDBT(save_value);
						freeDBT(save_key);
						return 0;
					}
					
					// Both child nodes have t-1 keys
					size_t j;
					merge_nodes(db, y, &x->keys[i], &x->values[i], z);
					
					removeFromFile(db, z);
					freeNode(db, z);
					
					for(j = i; j < x->n - 1; j++) {
						dbtcpy(&x->keys[j], &x->keys[j+1]);
						dbtcpy(&x->values[j], &x->values[j+1]);
					}
					
					for(j = i+1; j < x->n; j++) {
						x->offsetsChildren[j] = x->offsetsChildren[j+1];
					}
					
					x->n--;
					
					deleteKeyInside(db, y, key);
					
					if (x == db->root && x->n == 0) {
						removeFromFile(db, y);
						removeFromFile(db, db->root);
						y->selfOffset = db->root->selfOffset;
						freeNode(db, db->root);
						db->root = y;
						writeInFile(db, y);
					} else {
						writeInFile(db, x);
						freeNode(db, y);
					}
					
					return 0;
				}
			} else {
				if (x->leaf) {
				return -1;
			}
				struct BTREE *y = readFromFile(db, x->offsetsChildren[i]);
				if (y->n == db->t - 1) {
					struct BTREE *z = readFromFile(db, x->offsetsChildren[i+1]);
					if (z->n >= db->t) {
						keysSwapDirect(y, i, x, z);
						writeInFile(db, x);
						writeInFile(db, z);
						freeNode(db, z);
						int ret = deleteKeyInside(db, y, key);
						freeNode(db, y);
						return ret;
					}
					merge_nodes(db, y, &x->keys[i], &x->values[i], z);
					removeFromFile(db, z);
					freeNode(db, z);
					
					size_t j;
					for(j = i-1; j < x->n - 1; j++) {
						dbtcpy(&x->keys[j], &x->keys[j+1]);
						dbtcpy(&x->values[j], &x->values[j+1]);
					}
					
					for(j = i; j < x->n; j++) {
						x->offsetsChildren[j] = x->offsetsChildren[j+1];
					}
					
					x->n--;
					
					int ret = deleteKeyInside(db, y, key);
					
					if (x == db->root && x->n == 0) {
						removeFromFile(db, y);
						y->selfOffset = db->root->selfOffset;
						removeFromFile(db, db->root);
						freeNode(db, db->root);
						db->root = y;
						writeInFile(db, y);
					} else {
						writeInFile(db, x);
						freeNode(db, y);
					}
					return ret; 
				} else {
					int ret = deleteKeyInside(db, y, key);
					freeNode(db, y);
					return ret;
				}
				
			}
		} 
		if (i == x->n) {
			if (x->leaf) {
				return -1;
			}
			struct BTREE *y = readFromFile(db, x->offsetsChildren[i]);
			if (y->n == db->t - 1) {
				struct BTREE *z = readFromFile(db, x->offsetsChildren[i-1]);
				if (z->n >= db->t) {
					keysSwapUndirect(z, i-1, x, y);
					writeInFile(db, x);
					writeInFile(db, z);
					freeNode(db, z);
					int ret = deleteKeyInside(db, y, key);
					freeNode(db, y);
					return ret;
				} 
				merge_nodes(db, z, &x->keys[i], &x->values[i], y);
				removeFromFile(db, y);
				freeNode(db, y);
				
				size_t j;
				for(j = i-1; j < x->n - 1; j++) {
					dbtcpy(&x->keys[j], &x->keys[j+1]);
					dbtcpy(&x->values[j], &x->values[j+1]);
				}
				
				for(j = i; j < x->n; j++) {
					x->offsetsChildren[j] = x->offsetsChildren[j+1];
				}
				
				x->n--;
				
				int ret = deleteKeyInside(db, z, key);
				
				if (x == db->root && x->n == 0) {
					removeFromFile(db, z);
					z->selfOffset = db->root->selfOffset;
					removeFromFile(db, db->root);
					freeNode(db, db->root);
					db->root = z;
					writeInFile(db, z);
				} else {
					writeInFile(db, x);
					freeNode(db, z);
				}
				return ret;
			} else {		
				int ret = deleteKeyInside(db, y, key);
				freeNode(db, y);
				return ret;
			}
		}
	}
}


int deleteKey(struct DB *aux_db, const struct DBT *key) {
	struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
	return deleteKeyInside(db, db->root, key);
}


int mainForDbopen(void) {
	
	struct DB_IMPL * db = (struct DB_IMPL *)dbopen("database.db");
	if (db == NULL) {
		fprintf(stderr, "Fatal error: imposible open database\n");
		return -1;
	}
	
	struct DBT key, value;
	size_t k[] = {1901578, 25982088, 44271580, 34989, 1872522};
	
	key.data = (size_t *)malloc(sizeof(size_t));
	key.size = sizeof(size_t);
	
	value.data = (byte *)malloc(MAX_SIZE_VALUE - sizeof(size_t));
	memset(value.data, 0 , MAX_SIZE_VALUE - sizeof(size_t));
	
	size_t i;
	int g;
	
	for (i = 0; i < 5; i++) {
		memcpy(key.data, &k[i], sizeof(size_t)); 
		g = deleteKey((struct DB *)db, &key);
	}
	
	printBTREE(db, db->root, 0, 0);
	
	if (g == -1) {
		printf("There isn't key in this database\n");
		dbclose((struct DB *)db);
		free(key.data);
		free(value.data);
		return 0;
	}
	
	//printf("---- VALUE OF KEY %lu IS %s -----\n", k, (char *)value.data);
	
	free(key.data);
	free(value.data);
	
	dbclose((struct DB *)db);
	return 0;
}
/*
int main (int argc, char **argv) {
	if (argc == 2 && strcmp(argv[1], "open") == 0) {
		mainForDbopen();
		return 0;
	} 
	
	printf("\n--------------------------------------------------\n");
	printf("--------------- NEW RUN OF PROGRAM ---------------\n");
	printf("--------------------------------------------------\n");
	
	struct DBC conf;
	conf.db_size = 512 * 1024 * 1024;
	conf.chunk_size = 4096;
	//conf.mem_size = 16 * 1024 * 1024;

	struct DB_IMPL * myDB = (struct DB_IMPL *)dbcreate("database.db", conf);
	
	if (myDB == NULL) {
		fprintf(stderr, "Fatal error: imposible create database\n");
		return -1;
	}
	srand(time(NULL));
	struct DBT key;
	struct DBT value;
	byte str[32];
	size_t i, r, y, x;
	
	key.data = (size_t *)malloc(sizeof(size_t));
	key.size = sizeof(size_t);
	value.data = (byte *)malloc(32);
	value.size = 19;
	
	for(i = 0; i < 1000; i++) {
		r = rand() % 100000000; 
		if (i == 12) {
			y = r;
		}
		if (i == 56) {
			x = r;
		}
		valueGen(str, 32);
		memcpy(value.data, str, value.size);
		memcpy(key.data, &r, key.size);
		insertNode((struct DB *)myDB, &key, &value);
	}
	
	//print_statistics(myDB);
	
	printBTREE(myDB, myDB->root, 0, 0);
	
	printf("\n--- KEY FOR DELETING: %lu ---\n", y);
	
	memcpy(key.data, &y, key.size);
	int h = myDB->del((struct DB *)myDB, &key);
	
	printBTREE(myDB, myDB->root, 0, 0);
	
	memcpy(key.data, &x, key.size);
	h = myDB->del((struct DB *)myDB, &key);
	
	printBTREE(myDB, myDB->root, 0, 0);
	
	if (h == -1) {
		printf("There isn't key in this database\n");
		dbclose((struct DB *)myDB);
		return 0;
	}
	
	int u = myDB->get((struct DB *)myDB, &key, &value);
	
	//printBTREE(myDB, myDB->root, 0, 0);
	
	if (u == -1) {
		printf("There isn't key in this database\n");
		dbclose((struct DB *)myDB);
		return 0;
	}
	
	printf("---- VALUE OF KEY %lu IS %s -----\n", y, (char *)value.data);

	free(key.data);
	free(value.data);
	
	dbclose((struct DB *)myDB);

	return 0;
}*/

