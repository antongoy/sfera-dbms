#include "db_util.h"

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

int my_round(double f) {
    int r = (int)f;
    return r + 1;
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

int memcmp_wrapper (const struct DBT *value1, const struct DBT *value2) {
    int memcmp_res = memcmp(value1->data, value2->data, MIN(value1->size, value2->size));
    if (value1->size != value2->size && memcmp_res == 0) {
        return value1->size > value2->size ? 1 : -1;
    }
    return memcmp_res;
}

void dbtcpy(struct DBT *data1, const struct DBT *data2) {
    data1->size = data2->size;
    memcpy(data1->data, data2->data, data1->size);
}