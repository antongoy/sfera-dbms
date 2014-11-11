#include "db_io.h"

struct BTREE* read_from_file(struct DB_IMPL *db, size_t offset) {
    size_t i;
    struct BTREE *new_node = allocate_node(db->t, 1);
#ifndef _CACHE_
    struct LIST_NODE *list_node;

    if ((list_node = find_node_in_cache(db, offset)) != NULL) {
        nodecpy(new_node, list_node->data->page);
        restructure_cache(db, list_node);

        return new_node;
    }
#endif
    //Move to start position of node
    if (lseek(db->file_desc, offset, 0) < 0) {
        perror("In read_from_file function (lseek)");
        exit(1);
    }

    //Read to buffer
    if(read(db->file_desc, db->buf, db->chunk_size) != db->chunk_size) {
        perror("In read_from_file function (read)");
        exit(1);
    }

    //Parse the buffer. Metadata
    byte * ptr = db->buf;
    memcpy(&new_node->self_offset, ptr, sizeof(size_t));
    ptr += sizeof(size_t);
    memcpy(&new_node->leaf, ptr, sizeof(size_t));
    ptr += sizeof(size_t);
    memcpy(&new_node->n, ptr, sizeof(size_t));
    ptr += sizeof(size_t);

    //Parse the buffer. Offsets of children
    memcpy(new_node->offsets_children, ptr, sizeof(size_t)*(2*db->t));
    ptr +=  sizeof(size_t)*(2*db->t);

    //Parse the buffer. Keys and values
    int n = 2*db->t - 1;
    for(i = 0; i < n; i++) {
        memcpy(&new_node->keys[i].size, ptr, sizeof(size_t));
        ptr += sizeof(size_t);
        memcpy(new_node->keys[i].data, ptr, MAX_SIZE_KEY- sizeof(size_t));
        ptr += MAX_SIZE_KEY- sizeof(size_t);
        memcpy(&new_node->values[i].size, ptr, sizeof(size_t));
        ptr += sizeof(size_t);
        memcpy(new_node->values[i].data, ptr, MAX_SIZE_VALUE - sizeof(size_t));
        ptr += MAX_SIZE_VALUE - sizeof(size_t);
    }
#ifndef _CACHE_
    insert_node_into_cache(db, new_node);
#endif
    return new_node;

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
        node->self_offset = (j * BYTE_SIZE + 8 - i) * db->chunk_size + my_round(numOfBytesForMask / db->chunk_size) * db->chunk_size + db->chunk_size;

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
        int offsetInMetadata = db->chunk_size + j;
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
#ifndef _CACHE_
    struct LIST_NODE *list_node;

    if ((list_node = find_node_in_cache(db, node->self_offset)) != NULL) {
        refresh_node_in_cache(db, list_node, node);
    }
#endif
    size_t k = 0;
    memset(db->buf, 0, db->chunk_size);

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

    if(write(db->file_desc, db->buf, db->chunk_size) != db->chunk_size) {
        perror( "In function write_in_file (write)");
        exit(1);
    }

    return 0;
}

int remove_from_file(struct DB_IMPL *db, struct BTREE *x) {
    int numOfBytesForMask = db->n_blocks / BYTE_SIZE;
    int m = my_round(numOfBytesForMask / db->chunk_size);
    int pos_in_mask = (x->self_offset - (m*db->chunk_size + db->chunk_size)) / db->chunk_size;
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

    if (lseek(db->file_desc, db->chunk_size + num_of_byte, 0) < 0) {
        perror("In remove_from_file function (lseek)");
        exit(1);
    }
    if (write(db->file_desc, &db->mask[num_of_byte], sizeof(byte)) != sizeof(byte)) {
        perror("In remove_from_file function (write)");
        exit(1);
    }

    struct LIST_NODE *list_node;
#ifndef _CACHE_
    if ((list_node = find_node_in_cache(db, x->self_offset)) != NULL) {
        delete_from_cache(db, x->self_offset);
    }
#endif
    return 0;
}