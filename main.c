#include "main.h"

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

void print_statistics(struct DB_IMPL *db) {
    int i;
    printf("\n\t------ Database information ------\n");
    printf("\n");
    printf("--- Current number of blocks: %lu\n", db->cur_n_blocks);
    printf("--- Maximum number of blocks: %lu\n", db->n_blocks);
    printf("--- Chunk size: %lu\n", db->chunk_size);
    printf("--- Database size: %lu\n", db->db_size);
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

void value_gen(char * str, size_t size) {
    size_t i;

    for (i = 0; i < size; i++) {
        str[i] = 'a' + rand() % 26;
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


