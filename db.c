#include "db.h"

typedef struct DB DB;
typedef struct DBC DBC;
typedef struct DBT DBT;
typedef struct BTREE BTREE;
typedef struct DB_IMPL DB_IMPL;

BTREE* readFromFile(DB_IMPL *db, size_t offset);
BTREE* allocateNode(size_t t, size_t leaf);
int writeInFile (DB_IMPL *db, BTREE *node);

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
		memcpy(newNode->keys[i].data, ptr, MAX_SIZE_KEY/2);
		ptr += MAX_SIZE_KEY/2;
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
		lseek(db->fileDescriptor, 0, 0);
		write(db->fileDescriptor, db->curNumOfBlocks, sizeof(size_t));

        //Move to bit which is responsible for new node
        int offsetInMetadata = db->chunkSize + i;
		lseek(db->fileDescriptor, offsetInMetadata, 0);

        //Set 1 in file and in 'db' structure
		byte temp = 1;
		byte *tempPtr = &temp;  
		write(db->fileDescriptor, tempPtr, sizeof(byte));
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
	//if (db->curNumOfBlocks > 0) {
		memcpy(ptr, node->offsetsChildren, sizeof(size_t)*(2*db->t));
		ptr += sizeof(size_t)*(2*db->t);
	//}

    //Copy 'keys' and 'values'
	int n = 2*db->t - 1;
	for(i = 0; i < n; i++) {
		memcpy(ptr, &node->keys[i].size, sizeof(size_t));
		ptr += sizeof(size_t);
		memcpy(ptr, node->keys[i].data, MAX_SIZE_KEY/2);
		ptr += MAX_SIZE_KEY/2;
		memcpy(ptr, &node->values[i].size, sizeof(size_t));
		ptr += sizeof(size_t);
		memcpy(ptr, node->values[i].data, MAX_SIZE_VALUE - sizeof(size_t));
	}

    //Write in file node
	lseek(db->fileDescriptor, node->selfOffset, 0);
	write(db->fileDescriptor, db->buf, db->chunkSize);		

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
		root->values[i].data = (byte *)malloc(sizeof(byte)*MAX_SIZE_VALUE - sizeof(size_t));
		root->keys[i].data = (byte *)malloc(sizeof(byte)*MAX_SIZE_KEY/2);
	}
	return root;
} 

struct DB* dbcreate(const char *file, const struct DBC *conf) {
	size_t fd, i;
	unlink("database.db");
	fd = open(file, O_CREAT|O_RDWR|O_TRUNC,0);

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
	lseek(newDB->fileDescriptor, 0, 0);

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
	write(newDB->fileDescriptor, metaData, sizeof(size_t)*3);

    //Allocate memory for root node
	newDB->root = allocateNode(newDB->t, 1);

    // -1 is mean that this node hasn't written in file yet
    newDB->root->selfOffset = -1;

    //We have new node!
	newDB->curNumOfBlocks++;

    //No comments
	writeInFile(newDB, newDB->root);

    free(metaData);
	
	return (DB *)newDB;

}

DB* dbopen (const char *file) {
	size_t fd, i;
	
	if (fd = open(file, O_RDWR | O_APPEND, 0)) {
		return NULL;
	}
	
	DB_IMPL * newDB = (DB_IMPL *)malloc(sizeof(DB_IMPL));

	newDB->fileDescriptor = fd;

    //Metadata buffer for database header
	size_t * metaData = (size_t *)malloc(2 * sizeof(size_t) + sizeof(int));
	read(newDB->fileDescriptor, metaData,  2 * sizeof(size_t) + sizeof(int));

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
	lseek(newDB->fileDescriptor, newDB->chunkSize, 0);
	for(i = 1; i <= m; i++) {
		read(newDB->fileDescriptor, p, newDB->chunkSize);
		p += newDB->chunkSize;
	}

	newDB->root = readFromFile(newDB, newDB->chunkSize + newDB->numOfBlocks);

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
        z->keys[j].size = y->keys[j + db->t].size;
		memcpy(z->keys[j].data, y->keys[j + db->t].data, z->keys[j].size);
        z->values[j].size = y->values[j + db->t].size;
        memcpy(z->values[j].data, y->values[j + db->t].data, z->values[j].size);
	}

    //If not leaf then copy offsetsChildren
	if(!y->leaf) {
		for(j = 0; j < db->t; j++) {
			z->offsetsChildren[j] = y->offsetsChildren[j+db->t];
		}
	}

    //Set new size of y node
	y->n = db->t - 1;

    //Make place for new children in parent node
	for (j = x->n; j > i; j--) {
		x->offsetsChildren[j] = x->offsetsChildren[j-1];
	}

    //Write
	writeInFile(db, z);
    db->curNumOfBlocks++;
	x->offsetsChildren[i+1] = z->selfOffset;

    //Make place for new key and value
	for (j = x->n; j > i-1; j--) {
        x->keys[j].size = x->keys[j-1].size;
		memcpy(x->keys[j].data,x->keys[j-1].data, x->keys[j].size);
        x->values[j].size = x->values[j-1].size;
		memcpy(x->values[j].data,x->values[j-1].data, x->values[j].size);
	}
    //Cop to free place node from son
    x->keys[i].size = y->keys[db->t-1].size;
	memcpy(x->keys[i].data, y->keys[db->t-1].data, y->keys[db->t-1].size);
    x->values[i].size = y->values[db->t-1].size;
	memcpy(x->values[i].data, y->values[db->t-1].data, x->values[i].size);
    print_node(x);
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



void insertNonfullNode(DB_IMPL *db, BTREE *x, DBT *key, DBT *value) {
	long i = x->n;

    if (x->leaf) {
        if (x->n == 0) {
            x->keys[0].size = key->size;
            x->values[0].size = value->size;
            memcpy(x->keys[0].data, key->data, key->size);
            memcpy(x->values[0].data, value->data, value->size);
        } else {
            size_t newKey = *((size_t *) key->data);
            size_t keyInNode = *((size_t *) x->keys[i-1].data);
            while (i >= 1 && newKey < keyInNode) {
                size_t keyInNode = *((size_t *) x->keys[i-1].data);
                x->keys[i].size = x->keys[i-1].size;
                memcpy(x->keys[i].data, x->keys[i-1].data, x->keys[i].size);
                x->values[i].size = x->values[i-1].size;
                memcpy(x->values[i].data, x->values[i-1].data, x->values[i].size);
                i--;
            }
            x->keys[i].size = key->size;
            memcpy(x->keys[i].data, key->data, x->keys[i].size);
            x->values[i].size = value->size;
            memcpy(x->values[i].data, value->data, x->values[i].size-1);

        }

        x->n++;
        writeInFile(db, x);

    } else {
        size_t newKey = *((size_t *) key->data);
        size_t keyInNode = *((size_t *) x->keys[i-1].data);
        i--;
        while (i >= 0 && newKey < keyInNode) {
            keyInNode = *((size_t *) x->keys[i].data);
            i--;
        }
        i++;
        BTREE *child = readFromFile(db, x->offsetsChildren[i]);
        if (child->n == 2*db->t - 1) {
            BTREE *newChild = splitChild(db, x, child, i);
            if (*((size_t *)key->data) > *((size_t *)x->keys[i].data)) {
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
        //freeNode(s);
	} else {
		insertNonfullNode(db, temp, key, value);
	}
}

int main (void) {
	DBC conf;
	conf.db_size = 512 * 1024 * 1024;
	conf.chunk_size = 4096;
	conf.mem_size = 16 * 1024 * 1024;

	struct DB_IMPL * myDB = (DB_IMPL *)dbcreate("database.db", &conf);

    DBT key;
    DBT value;
    byte str[] = "abcdefgh";
    size_t i;
    key.data = (size_t *)malloc(sizeof(size_t));
    key.size = sizeof(size_t);
    value.data = (byte *)malloc(9);
    value.size = 9;

    for(i = 0; i < 30; i++) {
        memcpy(value.data, str, value.size);
        memcpy(key.data, &i, key.size);
        insertNode(myDB, &key, &value);
    }

    print_node(myDB->root);
    printf("OFFSET0 = %lu\n", myDB->root->offsetsChildren[0]);
    BTREE *nodeX = readFromFile(myDB, myDB->root->offsetsChildren[0]);
    BTREE *nodeY = readFromFile(myDB, myDB->root->offsetsChildren[1]);
    print_node(nodeX);
    print_node(nodeY);
    print_statistics(myDB);
    close(myDB->fileDescriptor);
    unlink("database.db");

}
