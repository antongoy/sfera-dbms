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

/*size_t binarySearch(const struct DBT *keys, size_t left_b,  size_t right_b, const DBT key) {
	while (1) {
		size_t mid = (left_b + right_b) / 2;
		if (memcmpwrapp2(&key, &keys[mid]) < 0) {
			right_b = mid - 1;
		} else if (memcmpwrapp2(&key, &keys[mid]) > 0) {
			left_b = mid + 1;
		} else {
			return mid;
		}
		if (left_b > right_b) {
			return -1;
		}
	}
}*/



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

int removeFromFile(struct DB_IMPL *db, struct BTREE *x) {
	int numOfBytesForMask = db->numOfBlocks / BYTE_SIZE;
	int m = myround(numOfBytesForMask / db->chunkSize);
	int pos_in_mask = (x->selfOffset - (m*db->chunkSize + db->chunkSize)) / db->chunkSize;
	int pos_in_byte = pos_in_mask;
	int num_of_byte = pos_in_mask;
	num_of_byte /= BYTE_SIZE;
	pos_in_byte %= BYTE_SIZE;
	pos_in_byte = BYTE_SIZE - pos_in_byte;
	setBitFalse(&db->mask[num_of_byte], pos_in_byte);
	if (lseek(db->fileDescriptor, 0, 0) < 0) {
		perror("lseek");
		return -1;
	}
	db->curNumOfBlocks--;
	if (write(db->fileDescriptor, &db->curNumOfBlocks, sizeof(size_t)) != sizeof(size_t)) {
		perror("write");
		return -1;
	}
	
	if (lseek(db->fileDescriptor, db->chunkSize + num_of_byte, 0) < 0) {
		perror("lseek");
		return -1;
	}
	if (write(db->fileDescriptor, &db->mask[num_of_byte], sizeof(byte)) != sizeof(byte)) {
		perror("write");
		return -1;
	}
	
	return 0;
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
	int i, numColKeys = 4, numColValue = 2, numColChildren = 3;
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
		printf("%2d->(%lu, %lu)  ", i, *((size_t *)x->keys[i].data), x->keys[i].size);
	}
	
	j=0;
	printf("\n\t\t Values: \n\t\t");
	
	for(i = 0; i < x->n; i++, j++) {
		if (j == numColValue) {
			printf("\n\t\t");
			j = 0;
		}
		printf("%2d--->%s  ", i, (char *)(x->values[i].data));
	}
	printf("\n");
	
	j=0;
	printf("\n\t\t Children: \n\t\t");
	
	for(i = 0; i <= x->n; i++, j++) {
		if (j == numColChildren) {
			printf("\n\t\t");
			j = 0;
		}
		printf("%2d--->%lu  ", i, x->offsetsChildren[i]);
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
			size_t j;
			for (j = 0; memcmpwrapp2(key, &(x->keys[j])) > 0; j++) {
				if (j == x->n) {
					break;
				}
				
			}
			if (j != x->n)
				if (memcmpwrapp2(key, &(x->keys[j])) == 0) {
					dbtcpy(&(x->values[j]), value);
					writeInFile(db, x);
					return;
				} 
			
			for (i = x->n; i > j; i--) {
				dbtcpy(&(x->keys[i]), &(x->keys[i-1]));
				dbtcpy(&(x->values[i]), &(x->values[i-1]));
			}
			
			dbtcpy(&(x->keys[j]), key);
			dbtcpy(&(x->values[j]), value);
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
		struct BTREE *child = readFromFile(db, x->offsetsChildren[i]);
		if (child->n == 2*db->t - 1) {
			struct BTREE *newChild = splitChild(db, x, child, i);
			if (memcmpwrapp2(key, &x->keys[i]) > 0) {
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
	while (memcmpwrapp2(key, &x->keys[i]) > 0) {
		i++;
		if (i == x->n) {
			break;
		}
		
	}
	if (i != x->n) {
		if (memcmpwrapp2(key, &x->keys[i]) == 0) {
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


int deleteFromBtreeLeaf (struct BTREE *x, size_t key_pos) {
    size_t i;
    
    for (i = key_pos; i < x->n - 1 ; i++) {
        dbtcpy(&x->keys[i], &x->keys[i+1]);
        dbtcpy(&x->values[i], &x->values[i+1]);
    }
    
    x->n--;
    
    return 0;
}

int mergeNodes (struct DB_IMPL *db, struct BTREE *y, struct DBT *key, struct DBT *value, struct BTREE *z) {
    size_t j;

    dbtcpy(&y->keys[db->t-1], key);
    dbtcpy(&y->values[db->t-1], value);
    
    for(j = 1; j < db->t; j++) {
        dbtcpy(&y->keys[j+db->t-1], &z->keys[j-1]);
        dbtcpy(&y->values[j+db->t-1], &z->values[j-1]);
    }
    
    if (!y->leaf) {
        for (j = 0; j < db->t + 1; j++) {
            y->offsetsChildren[db->t+j] = z->offsetsChildren[j];
        }
    }
    
    y->n = 2*db->t - 1;
    
    return 0;
}

void getPredecessorKey (struct DB_IMPL *db, struct BTREE *x,
                        struct DBT *pred_key, struct DBT *pred_value) {
    if (x->leaf) {
        dbtcpy(pred_key, &x->keys[x->n - 1]);
        dbtcpy(pred_value, &x->values[x->n - 1]);
    } else {
        struct BTREE *y = readFromFile(db, x->offsetsChildren[x->n]);
        getPredecessorKey(db, y, pred_key, pred_value);
        freeNode(db, y);
    }
    
}

void getSuccessorKey (struct DB_IMPL *db, struct BTREE *x,
                      struct DBT *succ_key, struct DBT *succ_value) {
    if (x->leaf) {
        dbtcpy(succ_key, &x->keys[0]);
        dbtcpy(succ_value, &x->values[0]);
    } else {
        struct BTREE *y = readFromFile(db, x->offsetsChildren[0]);
        getSuccessorKey(db, y, succ_key, succ_value);
        freeNode(db, y);
    }
}

int keysSwapRL(struct BTREE *left_child, struct BTREE *parent, size_t separator_pos, struct BTREE *right_child) {
    
    dbtcpy(&left_child->keys[left_child->n], &parent->keys[separator_pos]);
    dbtcpy(&left_child->values[left_child->n], &parent->values[separator_pos]);
    
    if (!left_child->leaf) {
        left_child->offsetsChildren[left_child->n + 1] = right_child->offsetsChildren[0];
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
            right_child->offsetsChildren[i] = right_child->offsetsChildren[i+1];
        }
    }
    
    right_child->n--;
    
    return 0;
}

int keysSwapLR(struct BTREE *left_child, struct BTREE *parent, size_t separator_pos, struct BTREE *right_child) {
    size_t i;
    
    for (i = right_child->n; i >= 1; i--) {
        dbtcpy(&right_child->keys[i], &right_child->keys[i-1]);
        dbtcpy(&right_child->values[i], &right_child->values[i-1]);
    }
    
    if (!right_child->leaf) {
        for (i = right_child->n + 1; i >= 1; i--) {
            right_child->offsetsChildren[i] = right_child->offsetsChildren[i-1];
        }
    }
    
    dbtcpy(&right_child->keys[0], &parent->keys[separator_pos]);
    dbtcpy(&right_child->values[0], &parent->values[separator_pos]);
    
    right_child->n++;
    
    dbtcpy(&parent->keys[separator_pos], &left_child->keys[left_child->n - 1]);
    dbtcpy(&parent->values[separator_pos], &left_child->values[left_child->n - 1]);
    
    if (!right_child->leaf) {
        right_child->offsetsChildren[0] = left_child->offsetsChildren[left_child->n];
    }
    
    left_child->n--;
    
    return 0;
}

int isInNode (struct BTREE *x, const struct DBT *key, size_t *pos) {
    size_t i = 0;
	for(i = 0; i < x->n && memcmpwrapp2(key, &x->keys[i]) > 0; i++);
    
    *pos = i;
    
    if (i < x->n && memcmpwrapp2(key, &x->keys[i]) == 0) {
        return 1;
    } else {
        return 0;
    }
}

void shiftKeys(struct BTREE *x, long pos) {
   long j;
    
    for(j = pos; j < x->n - 1; j++) {
        dbtcpy(&x->keys[j], &x->keys[j+1]);
        dbtcpy(&x->values[j], &x->values[j+1]);
    }
    
    
    if (!x->leaf) {
        for(j = pos+1; j < x->n ; j++) {
            x->offsetsChildren[j] = x->offsetsChildren[j+1];
        }
    }
    
    x->n--;
}

int deleteFromBtree (struct DB_IMPL *db, struct BTREE *x, const struct DBT *key) {
    size_t pos;
    
    if (isInNode(x, key, &pos)) {
        size_t key_index = pos;
        
        if (x->leaf) {
            deleteFromBtreeLeaf(x, key_index);
            writeInFile(db, x);
            return 0;
        } else {
            struct BTREE *y = readFromFile(db, x->offsetsChildren[key_index]);
            
            if (y->n >= db->t) {
                struct DBT *pred_key = allocateDBT((size_t)MAX_SIZE_KEY);
                struct DBT *pred_value = allocateDBT((size_t)MAX_SIZE_VALUE);
                
                getPredecessorKey(db, y, pred_key, pred_value);
                
                deleteFromBtree(db, y, pred_key);
                
                dbtcpy(&x->keys[key_index], pred_key);
                dbtcpy(&x->values[key_index], pred_value);
                
                writeInFile(db, x);
                
                freeDBT(pred_key);
                freeDBT(pred_value);
                freeNode(db, y);
                return 0;
            }
            
            struct BTREE *z = readFromFile(db, x->offsetsChildren[key_index + 1]);
            
            if (z->n >= db->t) {
                struct DBT *succ_key = allocateDBT((size_t)MAX_SIZE_KEY);
                struct DBT *succ_value = allocateDBT((size_t)MAX_SIZE_VALUE);
                
                getSuccessorKey(db, z, succ_key, succ_value);
                
                deleteFromBtree(db, z, succ_key);
                
                dbtcpy(&x->keys[key_index], succ_key);
                dbtcpy(&x->values[key_index], succ_value);
                
                writeInFile(db, x);
                
                freeDBT(succ_key);
                freeDBT(succ_value);
                freeNode(db, y);
                freeNode(db, z);
                return 0;
            }
            
            mergeNodes(db, y, &x->keys[key_index], &x->values[key_index], z);
            
            removeFromFile(db, z);
            freeNode(db, z);
            
            shiftKeys(x, key_index);
            
            if (x->n == 0) {
                removeFromFile(db, db->root);
                removeFromFile(db, y);
                db->curNumOfBlocks++;
                freeNode(db, db->root);
                db->root = y;
                y->selfOffset = -1;
                writeInFile(db, y);
            } else {
                writeInFile(db, x);
            }
            
            deleteFromBtree(db, y, key);
            
            if (db->root != y) {
                freeNode(db, y);
            } 
            
            return 0;
        }
    } else {
        size_t subtree_index = pos;
        if (x->leaf) {
            return -1;
        } else {
            struct BTREE *y = readFromFile(db, x->offsetsChildren[subtree_index]);
            
            if (y->n > db->t - 1) {
                int ret = deleteFromBtree(db, y, key);
                freeNode(db, y);
                return ret;
            }
            

            
            struct BTREE *z = NULL;
            struct BTREE *w = NULL;
            
            if (subtree_index != x->n) {
                z =  readFromFile(db, x->offsetsChildren[subtree_index + 1]);
                if (z->n >= db->t) {
                    keysSwapRL(y, x, subtree_index, z);
                    
                    writeInFile(db, x);
                    writeInFile(db, z);
                    writeInFile(db, y);
                    freeNode(db, z);
                    
                    int ret  = deleteFromBtree(db, y, key);
                    
                    freeNode(db, y);
                    
                    return ret;
                }
            }
            
            if (subtree_index != 0) {
                w = readFromFile(db, x->offsetsChildren[subtree_index - 1]);
                if (w->n >= db->t) {
                    keysSwapLR(w, x, subtree_index - 1, y);
                    
                    writeInFile(db, x);
                    writeInFile(db, w);
                    writeInFile(db, y);
                    
                    freeNode(db, w);
                    if (z != NULL) {
                        freeNode(db, z);
                    }
                    
                    int ret = deleteFromBtree(db, y, key);
                    
                    freeNode(db, y);
                    
                    return ret;
                }
            }
            
            if (subtree_index == x->n) {
                mergeNodes(db, w, &x->keys[x->n-1], &x->values[x->n-1], y);
                
                removeFromFile(db, y);
                freeNode(db, y);
                
                shiftKeys(x, x->n);
                
                if (x->n == 0) {
                    removeFromFile(db, db->root);
                    removeFromFile(db, w);
                    db->curNumOfBlocks++;
                    freeNode(db, db->root);
                    db->root = w;
                    w->selfOffset = -1;
                    writeInFile(db, w);
                } else {
                    writeInFile(db, x);
                    writeInFile(db, w);
                }
                
                int ret = deleteFromBtree(db, w, key);
                
                if (db->root != w) {
                    freeNode(db, w);
                } 
                
                return ret;
                
            } else {
                
                if (w != NULL) {
                    freeNode(db, w);
                }
                
                mergeNodes(db, y, &x->keys[subtree_index], &x->values[subtree_index], z);
                
                removeFromFile(db, z);
                freeNode(db, z);
                
                shiftKeys(x, subtree_index);
                
                if (x->n == 0) {
                    removeFromFile(db, db->root);
                    removeFromFile(db, y);
                    db->curNumOfBlocks++;
                    freeNode(db, db->root);
                    db->root = y;
                    y->selfOffset = -1;
                    writeInFile(db, y);
                } else {
                    writeInFile(db, x);
                    writeInFile(db, y);
                }
                
                int ret = deleteFromBtree(db, y, key);
                
                if (db->root != y) {
                    freeNode(db, y);
                } 
                
                return ret;
            }
        }
        
    }
    
}


int numKeys(struct DB_IMPL *db, struct BTREE *x) {
	int sum = 0, temp;
	size_t i;
	if (x->n == 8 && x != db->root) {
		printf("GTERRE\n");
	}
	if(x->leaf) {
		return x->n;
	}
	
	for (i = 0; i <=x->n ; i++) {
		struct BTREE *y = readFromFile(db, x->offsetsChildren[i]);
		temp = numKeys(db, y);
		sum += temp;
		freeNode(db, y);
	}
	sum += x->n;
	
	
	return  sum;
}



int deleteKey(struct DB *aux_db, const struct DBT *key) {
	struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
	printf("CUR NUM OF BLOCKS:  %d\n", numKeys(db, db->root));
	int ret =  deleteFromBtree(db, db->root, key);
	return ret;
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
	value.size = 32;
	size_t del_k[1000];
	FILE *fp = fopen("data.txt", "r");
	for(i = 0; i < 1000; i++) {
		r = rand() % 1000000000; 
		del_k[i] = r;
		fprintf(fp, "%lu ", del_k[i]);
		valueGen(str, 32);
		memcpy(value.data, str, value.size);
		memcpy(key.data, &del_k[i], key.size);
		insertNode((struct DB *)myDB, &key, &value);
	}
	fclose(fp);
	
	for (i = 0; i < 1000; i++) {
		memcpy(key.data, &del_k[i], key.size);
		if (i == 230 || i == 231) {
			//printf("sdsdsd\n");
			//printBTREE(myDB, myDB->root, 0 ,0);
		}
		if (deleteKey((struct DB *)myDB, &key) < 0) {
			//int n = searchInTree((struct DB *)myDB, &key, &value);
			//fprintf(stderr, "N= %d\n", n);
			printf("Problem in deleteKey: key[%lu]--> %lu\n",i, del_k[i]);
		}
	}
	
	printBTREE(myDB, myDB->root, 0, 0);
	print_statistics(myDB);

	free(key.data);
	free(value.data);
	
	dbclose((struct DB *)myDB);

	return 0;
}

