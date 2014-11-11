#include "db_cache.h"

struct CACHE * create_cache(size_t cache_size) {
    struct CACHE *new_cache = (struct CACHE *)malloc(sizeof(struct CACHE));

    if (new_cache == NULL) {
        perror("In init_cache function (malloc)");
        return NULL;
    }

    new_cache->cache_size = cache_size;
    new_cache->cur_cache_size = 0;

    TAILQ_INIT(&(new_cache->LRU));

    new_cache->HTABLE = NULL;

    return new_cache;
}

void restructure_cache(struct DB_IMPL *db, struct LIST_NODE *list_node) {
    struct CACHE *dbcache = db->database_cache;

    TAILQ_REMOVE(&(dbcache->LRU), list_node, link);

    TAILQ_INSERT_HEAD(&(dbcache->LRU), list_node, link);
}

void insert_node_into_cache(struct DB_IMPL *db, struct BTREE *new_page) {
    struct CACHE *dbcache = db->database_cache;

    if (dbcache->cur_cache_size < dbcache->cache_size) {
        struct CACHE_STORAGE_NODE *new_csn = (struct CACHE_STORAGE_NODE *)malloc(sizeof(struct CACHE_STORAGE_NODE));

        new_csn->page = allocate_node(db->t, 1);
        new_csn->state = NONE;

        nodecpy(new_csn->page, new_page);

        struct LIST_NODE *new_list_node = (struct LIST_NODE *)malloc(sizeof(struct LIST_NODE));
        new_list_node->data = new_csn;

        TAILQ_INSERT_HEAD(&(dbcache->LRU), new_list_node, link);

        dbcache->cur_cache_size++;

        struct HTABLE_NODE *new_htable_node = (struct HTABLE_NODE *)malloc(sizeof(struct HTABLE_NODE));
        new_htable_node->key = new_page->self_offset;
        new_htable_node->data = new_list_node;

        HASH_ADD_INT(dbcache->HTABLE, key, new_htable_node);

    } else {
        struct LIST_NODE *last_list_node;

        last_list_node = TAILQ_LAST(&(dbcache->LRU), CACHE_LIST);

        TAILQ_REMOVE(&(dbcache->LRU), last_list_node, link);
        size_t key = last_list_node->data->page->self_offset;

        struct HTABLE_NODE *new_htable_node;

        HASH_FIND_INT(dbcache->HTABLE, &key, new_htable_node);
        HASH_DEL(dbcache->HTABLE, new_htable_node);

        if (new_page == NULL) {
            printf("Error!");
            exit(1);
        }

        nodecpy(last_list_node->data->page, new_page);
        last_list_node->data->state = NONE;

        TAILQ_INSERT_HEAD(&(dbcache->LRU), last_list_node, link);

        new_htable_node->key = new_page->self_offset;
        new_htable_node->data = last_list_node;

        HASH_ADD_INT(dbcache->HTABLE, key, new_htable_node);

    }
}

struct LIST_NODE* find_node_in_cache(struct DB_IMPL *db, size_t key) {
    struct CACHE *dbcache = db->database_cache;

    struct HTABLE_NODE *new_htable_node;
    HASH_FIND_INT(dbcache->HTABLE, &key, new_htable_node);

    if (new_htable_node == NULL) {
        return NULL;
    }

    return new_htable_node->data;
}

void refresh_node_in_cache(struct DB_IMPL *db, struct LIST_NODE *list_node, struct BTREE *new_page) {
    struct CACHE *dbcache = db->database_cache;

    nodecpy(list_node->data->page, new_page);
    if (list_node->data->state == NONE) {
        list_node->data->state = CHANGE;
    }

}

void delete_from_cache(struct DB_IMPL *db, size_t key) {
    struct CACHE *dbcache = db->database_cache;

    struct HTABLE_NODE *htable_node;
    HASH_FIND_INT(dbcache->HTABLE, &key, htable_node);
    HASH_DEL(dbcache->HTABLE, htable_node);

    TAILQ_REMOVE(&(dbcache->LRU), htable_node->data, link);

    dbcache->cur_cache_size--;

    free_htable_node(db, htable_node);
}

int get_list_size(struct DB_IMPL *db) {
    struct CACHE *dbcache = db->database_cache;
    struct LIST_NODE *it;
    int i = 0;

    TAILQ_FOREACH(it, &(dbcache->LRU), link) i++;

    return i;

}