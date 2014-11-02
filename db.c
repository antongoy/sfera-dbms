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

void set_bit_true(byte *b, int pos) {
	byte mask = 1;
	mask <<= pos - 1;
	*b |= mask;
}

void set_bit_false(byte *b, int pos) {
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

int find_true_bit(byte *b) {
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

struct BTREE* read_from_file(struct DB_IMPL *db, size_t offset) {
	size_t i;
	struct BTREE *newNode = allocate_node(db->t, 1);

	//Move to start position of node
	if (lseek(db->file_desc, offset, 0) < 0) {
        perror("In read_from_file function (lseek)");
		exit(1);
	}

	//Read to buffer
	if(read(db->file_desc, db->buf, db->chunkSize) != db->chunkSize) {
        perror("In read_from_file function (read)");
        exit(1);
	}

	//Parse the buffer. Metadata
	byte * ptr = db->buf;
	memcpy(&newNode->self_offset, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newNode->leaf, ptr, sizeof(size_t));
	ptr += sizeof(size_t);
	memcpy(&newNode->n, ptr, sizeof(size_t));
	ptr += sizeof(size_t);

	//Parse the buffer. Offsets of children
	memcpy(newNode->offsets_children, ptr, sizeof(size_t)*(2*db->t));
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

int my_round(double f) {
	int r = (int)f;
	return r + 1;
}

int remove_from_file(struct DB_IMPL *db, struct BTREE *x) {
	int numOfBytesForMask = db->n_blocks / BYTE_SIZE;
	int m = my_round(numOfBytesForMask / db->chunkSize);
	int pos_in_mask = (x->self_offset - (m*db->chunkSize + db->chunkSize)) / db->chunkSize;
	int pos_in_byte = pos_in_mask;
	int num_of_byte = pos_in_mask;
	num_of_byte /= BYTE_SIZE;
	pos_in_byte %= BYTE_SIZE;
	pos_in_byte = BYTE_SIZE - pos_in_byte;
	set_bit_false(&db->mask[num_of_byte], pos_in_byte);
	if (lseek(db->file_desc, 0, 0) < 0) {
		perror("In remove_from_file function (lseek)");
		exit(1);
	}
	db->cur_n_blocks--;
	if (write(db->file_desc, &db->cur_n_blocks, sizeof(size_t)) != sizeof(size_t)) {
        perror("In remove_from_file function (write)");
        exit(1);
	}
	
	if (lseek(db->file_desc, db->chunkSize + num_of_byte, 0) < 0) {
        perror("In remove_from_file function (lseek)");
        exit(1);
	}
	if (write(db->file_desc, &db->mask[num_of_byte], sizeof(byte)) != sizeof(byte)) {
        perror("In remove_from_file function (write)");
        exit(1);
	}
	
	return 0;
}

int write_in_file (struct DB_IMPL *db, struct BTREE *node) {
	size_t i, j;

	// If self_offset = -1 then node hasn't written yet
	if (node->self_offset == -1) {

		//Find free space in file - first bit that is set to zero
		for (j = 0; (i = find_true_bit(&(db->mask[j]))) == 0 && j <  db->n_blocks; j++);
		
		//We can exceed size of mask. Then database is full
		if (j == db->n_blocks) {
            fprintf(stderr, "In write_in_file function: Fatal error - no free memory in database\n");
			exit(1);
		}
		
		int numOfBytesForMask = db->n_blocks / BYTE_SIZE;

		//Calculate new self_offset
		node->self_offset = (j * BYTE_SIZE + 8 - i) * db->chunkSize + my_round(numOfBytesForMask / db->chunkSize) * db->chunkSize + db->chunkSize;

		//Move to start of file and change curNumBlocks
		if (lseek(db->file_desc, 0, 0) == -1) {
            perror( "In function write_in_file (lseek)");
			exit(1);
		}

		if (write(db->file_desc, &db->cur_n_blocks, sizeof(size_t)) != sizeof(size_t)) {
            perror( "In function write_in_file (write)");
            exit(1);
		}

		//Move to bit which is responsible for new node
		int offsetInMetadata = db->chunkSize + j;
		if (lseek(db->file_desc, offsetInMetadata, 0) == -1) {
            perror( "In function write_in_file (lseek)");
            exit(1);
		}

		set_bit_true(&(db->mask[j]), i);
		
		if (write(db->file_desc, &(db->mask[j]), sizeof(byte)) == -1) {
            perror( "In function write_in_file (write)");
            exit(1);
		}
		
	}

	size_t k = 0;
	memset(db->buf, 0, db->chunkSize);

	//Copy header with block metadata
	memcpy(db->buf + k, &(node->self_offset), sizeof(size_t));
	k += sizeof(size_t);
	memcpy(db->buf + k, &(node->leaf), sizeof(size_t));
	k += sizeof(size_t);
	memcpy(db->buf + k, &(node->n), sizeof(size_t));
	k += sizeof(size_t);

	//Copy 'offsets_children'
	int n = 2*db->t;
	memcpy(db->buf + k, node->offsets_children, sizeof(size_t)*n);
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
	if (lseek(db->file_desc, node->self_offset, 0) == -1) {
        perror( "In function write_in_file (lseek)");
        exit(1);
	}

	if(write(db->file_desc, db->buf, db->chunkSize) != db->chunkSize) {
        perror( "In function write_in_file (write)");
        exit(1);
	}

	return 0;
}

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

struct DB* dbcreate(const char *file, const struct DBC conf) {
	long fd; 
	size_t i;

	if ((fd = open(file, O_CREAT|O_RDWR|O_TRUNC,  S_IRWXU)) < 0) {
        perror("In dbcreate function (open)");
        exit(1);
	}

	//Allocate memory for new Database
	struct DB_IMPL * newDB = (struct DB_IMPL *)malloc(sizeof(struct DB_IMPL));
	if (newDB == NULL) {
		fprintf(stderr, "In dbcreate function: Memory wasn't allocated for newDB\n");
		return NULL;
	}
	
	newDB->put = &insert_key;
	newDB->get = &search_key;
	newDB->close = &dbclose;
	newDB->del = &delete_key;
	
	//Fill simple fields in newDB structure
	newDB->file_desc = fd;
	newDB->cur_n_blocks = 0;
	newDB->chunkSize = conf.chunk_size;
	newDB->dbSize = conf.db_size;

	//Calculating auxiliary variable 'temp', which is needed for the evaluating 't' - degree of B-tree
	int temp = (conf.chunk_size - 3 * sizeof(size_t)) / (sizeof(size_t) + MAX_SIZE_KEY + MAX_SIZE_VALUE);
	temp = temp % 2 == 0 ? temp - 1 : temp;
	newDB->t = (temp + 1) / 2;

	//Calculating auxiliary variable 'm', which is needed for the evaluating 'n_blocks'is max number of nodes that stored in file
	double m = ((conf.db_size / conf.chunk_size) - 1) / ((double)(conf.chunk_size + 1));
	newDB->n_blocks = m * conf.chunk_size;
	//Allocate memory for bit mask - it shows free spaces in file
	newDB->mask = (byte *)malloc(newDB->n_blocks / 8);
	memset(newDB->mask, 0, newDB->n_blocks / 8);
	//newDB->mask = (byte *)malloc(sizeof(byte)*newDB->n_blocks);
	//memset(newDB->mask, 0, newDB->n_blocks);
	
	//Memory for auxiliary buffer. It is needed for writing in file
	newDB->buf = (byte *)malloc(sizeof(byte) * newDB->chunkSize);
	memset(newDB->buf, 0, newDB->chunkSize);
	//Move to start of file
	if (lseek(newDB->file_desc, 0, 0) == -1) {
		fprintf(stderr, "In dbcreate function: system call lseek throws the error\n");
		return NULL;
	}

	//'metaData' is auxiliary buffer for database metadata
	size_t *metaData = (size_t *)malloc(DB_HEADER_SIZE);
	//size_t *p = metaData;

	//Copy to buffer database metadata
	size_t k = 0;
	memcpy(metaData + k, &(newDB->cur_n_blocks), sizeof(size_t));
	k ++;
	memcpy(metaData + k, &(newDB->t), sizeof(size_t));
	k ++;    
	memcpy(metaData + k, &(newDB->chunkSize), sizeof(size_t));
	k ++; 
	memcpy(metaData + k, &(newDB->n_blocks), sizeof(size_t));
	
	
	//Write in file database metadata
	if(write(newDB->file_desc, metaData, DB_HEADER_SIZE) != DB_HEADER_SIZE) {
		fprintf(stderr, "In dbcreate function: system call write() throws the error\n");
		return NULL;
	}

	//Allocate memory for root node
	newDB->root = allocate_node(newDB->t, 1);
	if (newDB->root == NULL) {
		fprintf(stderr, "In dbcreate function: Don't create new node\n");
		return NULL;
	}
	

	// -1 is mean that this node hasn't written in file yet
	newDB->root->self_offset = -1;

	//We have new node!
	newDB->cur_n_blocks++;

	//No comments
	if(write_in_file(newDB, newDB->root) < 0) {
		fprintf(stderr, "In dbcreate function: Error in the write_in_file()\n");
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
	
	newDB->put = &insert_key;
	newDB->get = &search_key;
	newDB->close = &dbclose;
	

	newDB->file_desc = fd;

	//Metadata buffer for database header
	size_t *metaData = (size_t *)malloc(DB_HEADER_SIZE);
	if (metaData == NULL) {
		fprintf(stderr, "In dbopen function: Memory wasn't allocated for metaData\n");
		return NULL;
	}
	
	if (lseek(newDB->file_desc, 0, 0) < 0) {
		fprintf(stderr, "In dbopen function: lseek() error\n");
		return NULL;
	}
	
	if (read(newDB->file_desc, metaData, DB_HEADER_SIZE) !=  DB_HEADER_SIZE) {
		fprintf(stderr, "In dbopen function: imposible read data\n");
		return NULL;
	}

	//Parse metadata
	size_t k = 0;
	memcpy(&(newDB->cur_n_blocks), metaData + k, sizeof(size_t));
	k++;
	memcpy(&(newDB->t), metaData + k, sizeof(size_t));
	k++;
	memcpy(&(newDB->chunkSize), metaData + k, sizeof(size_t));
	k++;
	memcpy(&(newDB->n_blocks), metaData + k, sizeof(size_t));

	//Allocate memory
	newDB->buf = (byte *)malloc(sizeof(byte)*newDB->chunkSize);
	memset(newDB->buf, 0, newDB->chunkSize);
	if (newDB->buf == NULL) {
		fprintf(stderr, "In dbopen function: : Memory wasn't allocated for newDB->buf\n");
		return NULL;
	}
	
	newDB->mask = (byte *)malloc(newDB->n_blocks / BYTE_SIZE);

	if (newDB->mask == NULL) {
		fprintf(stderr, "In dbopen function: : Memory wasn't allocated for newDB->mask\n");
		return NULL;
	}
	
	memset(newDB->mask, 0, newDB->n_blocks / BYTE_SIZE); 
	
	//Read bit mask from file
	byte * p = newDB->mask;

	if (lseek(newDB->file_desc, newDB->chunkSize, 0) == -1) {
		fprintf(stderr, "In dbopen function: system call lseek() throws the error\n");
		return NULL;
	}
	
	long m = my_round((newDB->n_blocks / BYTE_SIZE)/newDB->chunkSize);
	long l = (newDB->n_blocks / BYTE_SIZE) - (m - 1) * newDB->chunkSize;
	
	if (read(newDB->file_desc, p, newDB->chunkSize*(m - 1) + l) == -1) {
		fprintf(stderr, "In dbopen function: imposible read from database \n");
		return NULL;
	}

	newDB->root = read_from_file(newDB, m*newDB->chunkSize + newDB->chunkSize);

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
	printf("--- Current number of blocks: %lu\n", db->cur_n_blocks);
	printf("--- Maximum number of blocks: %lu\n", db->n_blocks);
	printf("--- Chunk size: %lu\n", db->chunkSize);
	printf("--- Database size: %lu\n", db->dbSize);
	printf("--- Degree of tree: %lu\n", db->t);
	printf("--- Bitmask (first 10 elements): ");
	for(i = 0; i < 10; i++) {
		printf("%hhu ", db->mask[i]);
	}
	printf("\n");
	printf("--- Offset of root node in file: %lu\n", db->root->self_offset);
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
	printf("\n\t\t Offset of node in file: %lu\n", x->self_offset);
	printf("\t\t Number of keys in  node: %lu\n", x->n);
    printf( "\t\t Leaf flag: %lu\n", x->leaf);
    printf( "\t\t Keys in node: \n\t\t");
	
	int j = 0;
	for(i = 0; i < x->n; i++, j++) {
		if (j == numColKeys) {
            printf( "\n\t\t");
		 j = 0;
		}
		*((char *)x->keys[i].data + x->keys[i].size) = '\0';
        printf( "%2d->(%s, %lu)  ", i, (char *)x->keys[i].data, x->keys[i].size);
	}
	
	j=0;
    printf( "\n\t\t Values: \n\t\t");
	
	for(i = 0; i < x->n; i++, j++) {
		if (j == numColValue) {
            printf( "\n\t\t");
			j = 0;
		}
		printf( "%2d--->%s  ", i, (char *)(x->values[i].data));
	}
	printf( "\n");
	
	j=0;
	printf( "\n\t\t Children: \n\t\t");
	
	for(i = 0; i <= x->n; i++, j++) {
		if (j == numColChildren) {
			printf( "\n\t\t");
			j = 0;
		}
		printf( "%2d--->%lu  ", i, x->offsets_children[i]);
	}
    printf( "\n");
}

void dbtcpy(struct DBT *data1, const struct DBT *data2) {
	data1->size = data2->size;
	memcpy(data1->data, data2->data, data1->size);
}

struct BTREE* split_child(struct DB_IMPL *db, struct BTREE *x,  struct BTREE *y, size_t i) {
	long j;
	//Create new node
	struct BTREE *z = allocate_node(db->t, 1);
	if (z == NULL) {
		fprintf(stderr, "In split_child function: Don't create new node\n");
		return NULL;
	}
	
	//Copy simple information to new node 'z'
	z->leaf = y->leaf;
	z->n = db->t - 1;
	z->self_offset = -1;

	//Copy second part of y to z ++++++++++++
	for(j = 0; j < db->t-1; j++) {
		dbtcpy(&(z->keys[j]), &(y->keys[j+db->t]));
		dbtcpy(&(z->values[j]), &(y->values[j+db->t]));
	}
	
	//If not leaf then copy offsets_children ++++++++++
	if(!y->leaf) {
		for(j = 0; j < db->t; j++) {
			z->offsets_children[j] = y->offsets_children[j+db->t];
		}
	}
	
	//Set new size of y node
	y->n = db->t - 1;

	//Make place for new children in parent node +++++++
	for (j = x->n+1; j > i + 1; j--) {
		x->offsets_children[j] = x->offsets_children[j-1];
	}

	//Write
	db->cur_n_blocks++;
	if (write_in_file(db, z) < 0) {
		fprintf(stderr, "In split_child function: error in the write_in_file()\n");
		return NULL;
	}
	
	x->offsets_children[i+1] = z->self_offset;
	
	//Make place for new key and value ++++++
	for (j = x->n; j > i; j--) {
		dbtcpy(&x->keys[j], &x->keys[j-1]);
		dbtcpy(&x->values[j], &x->values[j-1]);
	}

	//Copy to free place node from son +++++++
	dbtcpy(&x->keys[i], &y->keys[db->t-1]);
	dbtcpy(&x->values[i], &y->values[db->t-1]);

	x->n++;
	if (write_in_file(db, y) < 0) {
		fprintf(stderr, "In split_child function: error in the write_in_file()\n");
		return NULL;
	}
	
	if (write_in_file(db, x) < 0) {
		fprintf(stderr, "In split_child function: error in the write_in_file()\n");
		return NULL;
	}
	
	return z;
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

int memcmp_wrapper (const struct DBT *value1, const struct DBT *value2) {
    int memcmp_res = memcmp(value1->data, value2->data, MIN(value1->size, value2->size));
    if (value1->size != value2->size && memcmp_res == 0) {
        return value1->size > value2->size ? 1 : -1;
    }
	return memcmp_res;
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

void insert_key_nonfull (struct DB_IMPL *db, struct BTREE *x, 
                       struct DBT *key, const struct DBT *value){
	long i = x->n;
    size_t j;
    int flag = is_in_node(x, key, &j);
	
	if (x->leaf) {
		if (x->n == 0) {
			dbtcpy(&(x->keys[0]), key);
			dbtcpy(&(x->values[0]), value);
		} else {

            if (flag) {
                dbtcpy(&(x->values[j]), value);
                write_in_file(db, x);
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
		write_in_file(db, x);
	} else {
        if (flag) {
            dbtcpy(&(x->values[j]), value);
            write_in_file(db, x);
            return;
        }
		struct BTREE *child = read_from_file(db, x->offsets_children[j]);
		if (child->n == 2*db->t - 1) {
			struct BTREE *newChild = split_child(db, x, child, j);
			if (memcmp_wrapper(key, &x->keys[j]) > 0) {
				insert_key_nonfull(db, newChild, key, value);
			} else {
				insert_key_nonfull(db, child, key, value);
			}
			free_node(db, newChild);
		} else {
			insert_key_nonfull(db, child, key, value);
		}
		free_node(db,child);
	}
}


void printBTREE (FILE *f, struct DB_IMPL *db, struct BTREE *x, int depth, int n) {
	struct BTREE *child;
	size_t i;

    printf( "\n=============================================================================================\n");
    printf( "================================= START ROOT OF SUBTREE ======================================\n");
    printf( "=================================== DEPTH: %d NUM: %d ==========================================\n", depth, n);
	print_node(x);
    printf( "\n=============================================================================================\n");
    printf( "=================================== START ITS CHILDREN =======================================\n");
    printf( "=============================================================================================\n");
	for(i = 0; i < x->n + 1; i++) {
		if (x->n == 0) {
			break;
		}
		
		child = read_from_file(db, x->offsets_children[i]);
		if(x->leaf == 0) {
            printf( "\n========================================= IT'S %lu CHILD =======================================\n", i);
			print_node(child);
            free_node(db, child);
		}
	}
	
	for (i = 0; i <x->n+1; i++) {
		if (x->n == 0) {
			break;
		}
		child = read_from_file(db, x->offsets_children[i]);
		if (child->leaf == 0) {
            printBTREE(f, db, child, depth + 1, i);
			free_node(db,child);
		}
	}
    printf( "\n=================================================================================================\n");
    printf( "============================================= END OF TREE =======================================\n");
    printf( "=================================================================================================\n\n");
}


int insert_key(struct DB *aux_db, struct DBT *key, const struct DBT *value) {
	struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
	struct BTREE *temp = db->root;
	
	if (temp->n == 2*db->t - 1) {
		struct BTREE *s = allocate_node(db->t, 1);
		db->root = s;
		db->root->leaf = 0;
		db->root->n = 0;
		db->root->self_offset = temp->self_offset;
		temp->self_offset = -1;
		write_in_file(db, temp);
		db->root->offsets_children[0] = temp->self_offset;
		db->cur_n_blocks++;
		write_in_file(db, db->root);
		struct BTREE *t = split_child(db, db->root, temp, 0);
		free_node(db, temp);
		insert_key_nonfull(db, s, key, value);
		free_node(db,t);
		return 0;
	} else {
		insert_key_nonfull(db, temp, key, value);
		return 0;
	}
}

int search_key_inside(struct DB_IMPL *db, struct BTREE *x, struct DBT *key, struct DBT *value) {
    long i = 0;

    int flag = is_in_node(x, key, &i);

    if (flag == 1) {
        dbtcpy(value, &(x->values[i]));
        return 0;
    } else {
        if (x->leaf) {
            return -1;
        }
        struct BTREE *new_node = read_from_file(db, x->offsets_children[i]);
        if (new_node == NULL) {
            perror("Error in search_key_inside function (read_from_file)");
            exit(1);
        }
        int  res =  search_key_inside(db, new_node, key, value);
        free_node(db, new_node);
        return res;
    }
}

int search_key(struct DB *aux_db, struct DBT *key, struct DBT *value) {
	struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
    value->data = (byte *)malloc(MAX_SIZE_VALUE);
	return search_key_inside(db, db->root, key, value);
}

int dbclose (struct DB *close_db) {
	struct DB_IMPL *db = (struct DB_IMPL *)close_db;
	free_node(db, db->root);
	free(db->mask);
	free(db->buf);
	close(db->file_desc);
	free(db);
	return 0;
}

void value_gen(char * str, size_t size) {
	size_t i;
	
	for (i = 0; i < size; i++) {
		str[i] = 'a' + rand() % 26;
	}
	
}

struct DBT * allocate_dbt(size_t size) {
	struct DBT *dbt = (struct DBT *)malloc(sizeof(struct DBT));
    if (dbt == NULL) {
        perror("In function allocate_dbt function (malloc)");
        return NULL;
    }
	dbt->size = size;
	dbt->data = (size_t *)malloc(size);
    if (dbt->size == NULL)  {
        perror("In function allocate_dbt function (malloc)");
        return NULL;
    }
	return dbt;
}

void free_dbt (struct DBT *ptr) {
	free(ptr->data);
	free(ptr);
}


int delete_from_btree_leaf (struct BTREE *x, size_t key_pos) {
    size_t i;
    
    for (i = key_pos; i < x->n - 1 ; i++) {
        dbtcpy(&x->keys[i], &x->keys[i+1]);
        dbtcpy(&x->values[i], &x->values[i+1]);
    }
    
    x->n--;
    
    return 0;
}

int merge_nodes (struct DB_IMPL *db, struct BTREE *y, struct DBT *key, 
                                                struct DBT *value, struct BTREE *z) {
    size_t j;

    dbtcpy(&y->keys[db->t-1], key);
    dbtcpy(&y->values[db->t-1], value);
    
    for(j = 1; j < db->t; j++) {
        dbtcpy(&y->keys[j+db->t-1], &z->keys[j-1]);
        dbtcpy(&y->values[j+db->t-1], &z->values[j-1]);
    }
    
    if (!y->leaf) {
        for (j = 0; j < db->t + 1; j++) {
            y->offsets_children[db->t+j] = z->offsets_children[j];
        }
    }
    
    y->n = 2*db->t - 1;
    
    return 0;
}

void get_predecessor_key (struct DB_IMPL *db, struct BTREE *x,
                        struct DBT *pred_key, struct DBT *pred_value) {
    if (x->leaf) {
        dbtcpy(pred_key, &x->keys[x->n - 1]);
        dbtcpy(pred_value, &x->values[x->n - 1]);
    } else {
        struct BTREE *y = read_from_file(db, x->offsets_children[x->n]);
        if (y == NULL) {
            perror("In get_predcessor_key function (read_from_file): ");
            exit(1);
        }
        get_predecessor_key(db, y, pred_key, pred_value);
        free_node(db, y);
    }
    
}

void get_successor_key (struct DB_IMPL *db, struct BTREE *x,
                      struct DBT *succ_key, struct DBT *succ_value) {
    if (x->leaf) {
        dbtcpy(succ_key, &x->keys[0]);
        dbtcpy(succ_value, &x->values[0]);
    } else {
        struct BTREE *y = read_from_file(db, x->offsets_children[0]);
        get_successor_key(db, y, succ_key, succ_value);
        free_node(db, y);
    }
}

int keys_swap_rl(struct BTREE *left_child, struct BTREE *parent, size_t separator_pos, struct BTREE *right_child) {
    
    dbtcpy(&left_child->keys[left_child->n], &parent->keys[separator_pos]);
    dbtcpy(&left_child->values[left_child->n], &parent->values[separator_pos]);
    
    if (!left_child->leaf) {
        left_child->offsets_children[left_child->n + 1] = right_child->offsets_children[0];
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
            right_child->offsets_children[i] = right_child->offsets_children[i+1];
        }
    }
    
    right_child->n--;
    
    return 0;
}

int keys_swap_lr(struct BTREE *left_child, struct BTREE *parent, size_t separator_pos, struct BTREE *right_child) {
    size_t i;
    
    for (i = right_child->n; i >= 1; i--) {
        dbtcpy(&right_child->keys[i], &right_child->keys[i-1]);
        dbtcpy(&right_child->values[i], &right_child->values[i-1]);
    }
    
    if (!right_child->leaf) {
        for (i = right_child->n + 1; i >= 1; i--) {
            right_child->offsets_children[i] = right_child->offsets_children[i-1];
        }
    }
    
    dbtcpy(&right_child->keys[0], &parent->keys[separator_pos]);
    dbtcpy(&right_child->values[0], &parent->values[separator_pos]);
    
    right_child->n++;
    
    dbtcpy(&parent->keys[separator_pos], &left_child->keys[left_child->n - 1]);
    dbtcpy(&parent->values[separator_pos], &left_child->values[left_child->n - 1]);
    
    if (!right_child->leaf) {
        right_child->offsets_children[0] = left_child->offsets_children[left_child->n];
    }
    
    left_child->n--;
    
    return 0;
}

int is_in_node (struct BTREE *x, const struct DBT *key, size_t *pos) {
    size_t i = 0;
	for(i = 0; i < x->n && memcmp_wrapper(key, &x->keys[i]) > 0; i++);
    
    *pos = i;
    
    if (i < x->n && memcmp_wrapper(key, &x->keys[i]) == 0) {
        return 1;
    } else {
        return 0;
    }
}

void shift_keys(struct BTREE *x, long pos) {
   long j;
    
    for(j = pos; j < x->n - 1; j++) {
        dbtcpy(&x->keys[j], &x->keys[j+1]);
        dbtcpy(&x->values[j], &x->values[j+1]);
    }
    
    
    if (!x->leaf) {
        for(j = pos+1; j < x->n ; j++) {
            x->offsets_children[j] = x->offsets_children[j+1];
        }
    }
    
    x->n--;
}

int delete_from_btree (struct DB_IMPL *db, struct BTREE *x, const struct DBT *key) {
    size_t pos;
    
    if (is_in_node(x, key, &pos)) {
        size_t key_index = pos;
        
        if (x->leaf) {
            delete_from_btree_leaf(x, key_index);
            write_in_file(db, x);
            return 0;
        } else {
            struct BTREE *y = read_from_file(db, x->offsets_children[key_index]);
            
            if (y->n >= db->t) {
                struct DBT *pred_key = allocate_dbt((size_t)MAX_SIZE_KEY);
                struct DBT *pred_value = allocate_dbt((size_t)MAX_SIZE_VALUE);
                
                get_predecessor_key(db, y, pred_key, pred_value);
                
                delete_from_btree(db, y, pred_key);
                
                dbtcpy(&x->keys[key_index], pred_key);
                dbtcpy(&x->values[key_index], pred_value);
                
                write_in_file(db, x);
                
                free_dbt(pred_key);
                free_dbt(pred_value);
                free_node(db, y);
                return 0;
            }
            
            struct BTREE *z = read_from_file(db, x->offsets_children[key_index + 1]);
            
            if (z->n >= db->t) {
                struct DBT *succ_key = allocate_dbt((size_t)MAX_SIZE_KEY);
                struct DBT *succ_value = allocate_dbt((size_t)MAX_SIZE_VALUE);
                
                get_successor_key(db, z, succ_key, succ_value);
                
                delete_from_btree(db, z, succ_key);
                
                dbtcpy(&x->keys[key_index], succ_key);
                dbtcpy(&x->values[key_index], succ_value);
                
                write_in_file(db, x);
                
                free_dbt(succ_key);
                free_dbt(succ_value);
                free_node(db, y);
                free_node(db, z);
                return 0;
            }
            
            merge_nodes(db, y, &x->keys[key_index], &x->values[key_index], z);
            
            remove_from_file(db, z);
            free_node(db, z);
            
            shift_keys(x, key_index);
            
            if (x->n == 0) {
                remove_from_file(db, db->root);
                remove_from_file(db, y);
                db->cur_n_blocks++;
                free_node(db, db->root);
                db->root = y;
                y->self_offset = -1;
                write_in_file(db, y);
            } else {
                write_in_file(db, x);
            }
            
            delete_from_btree(db, y, key);
            
            if (db->root != y) {
                free_node(db, y);
            } 
            
            return 0;
        }
    } else {
        size_t subtree_index = pos;
        if (x->leaf) {
            return 0;
        } else {
            struct BTREE *y = read_from_file(db, x->offsets_children[subtree_index]);
            
            if (y->n > db->t - 1) {
                int ret = delete_from_btree(db, y, key);
                free_node(db, y);
                return ret;
            }
            

            
            struct BTREE *z = NULL;
            struct BTREE *w = NULL;
            
            if (subtree_index != x->n) {
                z =  read_from_file(db, x->offsets_children[subtree_index + 1]);
                if (z->n >= db->t) {
                    keys_swap_rl(y, x, subtree_index, z);
                    
                    write_in_file(db, x);
                    write_in_file(db, z);
                    write_in_file(db, y);
                    free_node(db, z);
                    
                    int ret  = delete_from_btree(db, y, key);
                    
                    free_node(db, y);
                    
                    return ret;
                }
            }
            
            if (subtree_index != 0) {
                w = read_from_file(db, x->offsets_children[subtree_index - 1]);
                if (w->n >= db->t) {
                    keys_swap_lr(w, x, subtree_index - 1, y);
                    
                    write_in_file(db, x);
                    write_in_file(db, w);
                    write_in_file(db, y);
                    
                    free_node(db, w);
                    if (z != NULL) {
                        free_node(db, z);
                    }
                    
                    int ret = delete_from_btree(db, y, key);
                    
                    free_node(db, y);
                    
                    return ret;
                }
            }
            
            if (subtree_index == x->n) {
                merge_nodes(db, w, &x->keys[x->n-1], &x->values[x->n-1], y);
                
                remove_from_file(db, y);
                free_node(db, y);
                
                shift_keys(x, x->n);
                
                if (x->n == 0) {
                    remove_from_file(db, db->root);
                    remove_from_file(db, w);
                    db->cur_n_blocks++;
                    free_node(db, db->root);
                    db->root = w;
                    w->self_offset = -1;
                    write_in_file(db, w);
                } else {
                    write_in_file(db, x);
                    write_in_file(db, w);
                }
                
                int ret = delete_from_btree(db, w, key);
                
                if (db->root != w) {
                    free_node(db, w);
                } 
                
                return ret;
                
            } else {
                
                if (w != NULL) {
                    free_node(db, w);
                }
                
                merge_nodes(db, y, &x->keys[subtree_index], &x->values[subtree_index], z);
                
                remove_from_file(db, z);
                free_node(db, z);
                
                shift_keys(x, subtree_index);
                
                if (x->n == 0) {
                    remove_from_file(db, db->root);
                    remove_from_file(db, y);
                    db->cur_n_blocks++;
                    free_node(db, db->root);
                    db->root = y;
                    y->self_offset = -1;
                    write_in_file(db, y);
                } else {
                    write_in_file(db, x);
                    write_in_file(db, y);
                }
                
                int ret = delete_from_btree(db, y, key);
                
                if (db->root != y) {
                    free_node(db, y);
                } 
                
                return ret;
            }
        }
        
    }
    
}


int num_keys_in_btree(struct DB_IMPL *db, struct BTREE *x) {
	int sum = 0, temp;
	size_t i;
	if(x->leaf) {
		return x->n;
	}
	
	for (i = 0; i <=x->n ; i++) {
		struct BTREE *y = read_from_file(db, x->offsets_children[i]);
		temp = num_keys_in_btree(db, y);
		sum += temp;
		free_node(db, y);
	}
	sum += x->n;
	
	return  sum;
}



int delete_key(struct DB *aux_db, const struct DBT *key) {
	struct DB_IMPL *db = (struct DB_IMPL *)aux_db;
	int ret =  delete_from_btree(db, db->root, key);
	return ret;
}

/*
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
		g = delete_key((struct DB *)db, &key);
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
}*/

int main (int argc, char **argv) {
	/*if (argc == 2 && strcmp(argv[1], "open") == 0) {
		mainForDbopen();
		return 0;
	} */
	
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
	value.data = (byte *)malloc(15);
	value.size = 15;
	size_t del_k[100000];
	FILE *fp = fopen("data.txt", "w");

	for(i = 0; i < 100000; i++) {
		r = rand() % 1000000000; 
		del_k[i] = r;
		fscanf(fp, "%lu", &del_k[i]);
		value_gen(str, 15);
		memcpy(value.data, str, value.size);
		memcpy(key.data, &del_k[i], key.size);
		insert_key((struct DB *)myDB, &key, &value);
	}
	fclose(fp);

	for (i = 0; i < 100000; i++) {
		memcpy(key.data, &del_k[i], key.size);
		if (delete_key((struct DB *)myDB, &key) < 0) {
			printf("Problem in delete_key: key[%lu]--> %lu\n",i, del_k[i]);
		}
	}
	
	//printBTREE(myDB, myDB->root, 0, 0);
	print_statistics(myDB);

	free(key.data);
	free(value.data);
	
	dbclose((struct DB *)myDB);

	return 0;
}


