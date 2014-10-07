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
	
	DB_IMPL * newDB = (DB_IMPL *)malloc(sizeof(DB_IMPL));

	newDB->fileDescriptor = fd;

    //Metadata buffer for database header
	size_t * metaData = (size_t *)malloc(2 * sizeof(size_t) + sizeof(int));
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
	newDB->mask = (byte *)malloc(sizeof(byte)*newDB->numOfBlocks);	

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
        fprintf(stderr, "In dbopen function: imposible reading node from file\n");
        return NULL;
    }
    free(metaData);

	return (DB *)newDB;	
}

void print_statistics(DB_IMPL *db) {
    int i;
	printf("\nDatabase information:\n");
	printf("---------------------------------------------\n");
	printf("CurNumOfBlocks = %lu\n", db->curNumOfBlocks);
	printf("NumOfBlocks = %lu\n", db->numOfBlocks);
	printf("ChunkSize = %lu\n", db->chunkSize);
	printf("t = %lu\n", db->t);
    printf("Bitmask = ");
    for(i = 0; i < 10; i++) {
        printf("%d", db->mask[i]);
    }
	printf("\n");
    printf("Keys in root = ");
    for(i = 0; i < db->root->n; i++) {
        printf("%lu ", *((size_t *)db->root->keys[i].data));
    }
    printf("\n");
}

void print_node (BTREE *x) {
    int i = 0;
    for(i = 0; i < x->n; i++) {
        printf("%lu ", *((size_t *)x->keys[i].data));
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

void insertNonfullNode(DB_IMPL *db, BTREE *x, DBT *key, DBT *value) {
	long i = x->n;

    if (x->leaf) {
        if (x->n == 0) {
            dbtcpy(&x->keys[0], key);
            dbtcpy(&x->values[0], value);
        } else {

            while (memcmpwrapp(key, &x->keys[i-1]) < 0) {
                dbtcpy(&x->keys[i], &x->keys[i-1]);
                dbtcpy(&x->values[i], &x->values[i-1]);
                i--;
                if (!i) {
                    i++;
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
        } while (i >= 0 && memcmpwrapp(key, &x->keys[i]) < 0);

        i++;
        BTREE *child = readFromFile(db, x->offsetsChildren[i]);
        if (child->n == 2*db->t - 1) {
            BTREE *newChild = splitChild(db, x, child, i);
            if (memcmpwrapp(key, &x->keys[i]) > 0) {
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
    size_t i = 0;
    while (i <= x->n-1 && memcmpwrapp(key, &x->keys[i]) > 0)
        i++;
    if (i <= x->n-1 && memcmpwrapp(key, &x->keys[i]) == 0) {
        BTREE_POS *result = (BTREE_POS *)malloc(sizeof(BTREE_POS));
        result->node = x;
        result->pos = i;
        return result;
    } else {
        if (x->leaf) {
            return NULL;
        } else {
            if (x != db->root) {
                freeNode(x);
            }
            BTREE *newNode = readFromFile(db, x->offsetsChildren[i]);
            return searchInTreeInside(db, newNode, key);
        }
    }
}

BTREE_POS* searchInTree(DB_IMPL *db, DBT *key) {
   return searchInTreeInside(db, db->root, key );
}

int main (void) {
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
    byte str[] = "abcdefgh";
    size_t i;
    key.data = (size_t *)malloc(sizeof(size_t));
    key.size = sizeof(size_t);
    value.data = (byte *)malloc(9);
    value.size = 9;

    for(i = 0; i < 32; i++) {
        memcpy(value.data, str, value.size);
        memcpy(key.data, &i, key.size);
        insertNode(myDB, &key, &value);
    }

    print_node(myDB->root);
    BTREE *nodeX = readFromFile(myDB, myDB->root->offsetsChildren[0]);
    BTREE *nodeY = readFromFile(myDB, myDB->root->offsetsChildren[1]);
    print_node(nodeX);
    print_node(nodeY);
    print_statistics(myDB);
    printf("\n---------------------------------------------\n");
    i = 19;
    memcpy(key.data, &i, key.size);
    
    BTREE_POS *res = searchInTree(myDB, &key);
    print_node(res->node);
    printf("---- Position in node: %lu -----\n", res->pos);
    close(myDB->fileDescriptor);
    unlink("database.db");

}
