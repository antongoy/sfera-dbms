#include "db_create.h"

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
    newDB->chunk_size = conf.chunk_size;
    newDB->db_size = conf.db_size;

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

    size_t cache_size = conf.mem_size / conf.chunk_size;

    newDB->database_cache = create_cache(cache_size);

    //Memory for auxiliary buffer. It is needed for writing in file
    newDB->buf = (byte *)malloc(sizeof(byte) * newDB->chunk_size);
    memset(newDB->buf, 0, newDB->chunk_size);

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
    k++;
    memcpy(metaData + k, &(newDB->t), sizeof(size_t));
    k++;
    memcpy(metaData + k, &(newDB->chunk_size), sizeof(size_t));
    k++;
    memcpy(metaData + k, &(newDB->n_blocks), sizeof(size_t));
    k++;
    memcpy(metaData + k, &cache_size, sizeof(size_t));

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
    size_t cache_size;

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
    memcpy(&(newDB->chunk_size), metaData + k, sizeof(size_t));
    k++;
    memcpy(&(newDB->n_blocks), metaData + k, sizeof(size_t));
    k++;
    memcpy(&(cache_size), metaData + k, sizeof(size_t));

    newDB->database_cache = create_cache(cache_size);

    //Allocate memory
    newDB->buf = (byte *)malloc(sizeof(byte)*newDB->chunk_size);
    memset(newDB->buf, 0, newDB->chunk_size);
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

    if (lseek(newDB->file_desc, newDB->chunk_size, 0) == -1) {
        fprintf(stderr, "In dbopen function: system call lseek() throws the error\n");
        return NULL;
    }

    long m = my_round((newDB->n_blocks / BYTE_SIZE)/newDB->chunk_size);
    long l = (newDB->n_blocks / BYTE_SIZE) - (m - 1) * newDB->chunk_size;

    if (read(newDB->file_desc, p, newDB->chunk_size*(m - 1) + l) == -1) {
        fprintf(stderr, "In dbopen function: imposible read from database \n");
        return NULL;
    }

    newDB->root = read_from_file(newDB, m*newDB->chunk_size + newDB->chunk_size);

    if (newDB->root == NULL) {
        fprintf(stderr, "In dbopen function: imposible read the node from file\n");
        return NULL;
    }
    free(metaData);

    return (struct DB *)newDB;
}

int dbclose (struct DB *close_db) {
    struct DB_IMPL *db = (struct DB_IMPL *)close_db;
    struct HTABLE_NODE *htable_node, *tmp;
    struct LIST_NODE *list_node;

    free_node(db, db->root);
    free(db->mask);
    free(db->buf);
    close(db->file_desc);

    HASH_ITER(hh, db->database_cache->HTABLE, htable_node, tmp) {
        free_htable_node(db, htable_node);
    }


    free(db);
    return 0;
}