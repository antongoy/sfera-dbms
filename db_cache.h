#ifndef _DB_CACHE_H_
#define _DB_CACHE_H_


#include "db_data_structures.h"
#include "db_util.h"
#include "db_mem_util.h"
#include "main.h"

struct CACHE *create_cache(size_t cache_size);


// ******************************************
// ******** ONLY FOR READ OPERATIONS ********
// ******************************************

//Insert least recently used node into head
void restructure_cache(struct DB_IMPL *db, struct LIST_NODE *list_node);

//Insert new just read node into cache (assumption: there is no new_page in cache (i.e. in hashtable)).
void insert_node_into_cache(struct DB_IMPL *db, struct BTREE *new_page);






// ***********************************************
// ******** FOR READ AND WRITE OPERATIONS ********
// ***********************************************

struct LIST_NODE* find_node_in_cache(struct DB_IMPL *db, size_t key);




// *******************************************
// ******** ONLY FOR WRITE OPERATIONS ********
// *******************************************

void refresh_node_in_cache(struct DB_IMPL *db, struct LIST_NODE *list_node, struct BTREE *new_page);



void delete_from_cache(struct DB_IMPL *db, size_t key);

#endif // _DB_CACHE_H_