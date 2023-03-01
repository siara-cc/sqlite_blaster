#ifndef BP_TREE_H
#define BP_TREE_H
#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdint.h>
#include "lru_cache.h"

#define BPT_LEAF0_LVL 14
#define BPT_STAGING_LVL 15
#define BPT_PARENT0_LVL 16

#define BPT_BLK_TYPE_INTERIOR 0
#define BPT_BLK_TYPE_LEAF 128
#define BPT_BLK_TYPE_OVFL 255

#define DEFAULT_BLOCK_SIZE 4096

#define descendant static_cast<T*>(this)

class util {
  public:
    static int compare(const uint8_t *v1, int len1, const uint8_t *v2,
            int len2, int k = 0) {
        int lim = (len2 < len1 ? len2 : len1);
        while (k < lim) {
            uint8_t c1 = v1[k];
            uint8_t c2 = v2[k];
            k++;
            if (c1 < c2)
                return -k;
            else if (c1 > c2)
                return k;
        }
        if (len1 == len2)
            return 0;
        k++;
        return (len1 < len2 ? -k : k);
    }
};

union page_ptr {
    unsigned long page;
    uint8_t *ptr;
};

#define MAX_LVL_COUNT 10
class bptree_iter_ctx {
    public:
        page_ptr pages[MAX_LVL_COUNT];
        int found_page_pos;
        int8_t last_page_lvl;
        int8_t found_page_idx;
        void init(unsigned long page, uint8_t *ptr, int cache_size) {
            last_page_lvl = 0;
            found_page_idx = 0;
            found_page_pos = -1;
            set(page, ptr, cache_size);
        }
        void set(unsigned long page, uint8_t *ptr, int cache_size) {
            if (cache_size > 0)
                pages[last_page_lvl].page = page;
            else
                pages[last_page_lvl].ptr = ptr;
        }
};

template<class T> // CRTP
class btree_handler {

protected:
    long total_size;
    int num_levels;
    int max_key_count_node;
    int max_key_count_leaf;
    int block_count_node;
    int block_count_leaf;
    long count1, count2;
    int max_key_len;
    int is_block_given;
    int root_page_num;
    bool is_closed;

public:
    lru_cache *cache;
    uint8_t *root_block;
    uint8_t *current_block;
    unsigned long current_page;
    const uint8_t *key;
    int key_len;
    uint8_t *key_at;
    int key_at_len;
    const uint8_t *value;
    int value_len;
    bool is_btree;

    size_t block_size;
    int cache_size;
    const char *filename;
    btree_handler(uint32_t block_sz = DEFAULT_BLOCK_SIZE, int cache_sz_kb = 0,
            const char *fname = NULL, int start_page_num = 0, bool whether_btree = false) :
            block_size (block_sz), cache_size (cache_sz_kb), filename (fname) {
        descendant->init_derived();
        init_stats();
        is_closed = false;
        is_block_given = 0;
        root_page_num = start_page_num;
        is_btree = whether_btree;
        if (cache_size > 0) {
            cache = new lru_cache(block_size, cache_size, filename,
                    &descendant->is_block_changed, &descendant->set_block_changed,
                    start_page_num);
            root_block = current_block = cache->get_disk_page_in_cache(start_page_num);
            if (cache->is_empty()) {
                descendant->set_leaf(1);
                descendant->init_current_block();
            }
        }
        // } else {
        //     root_block = current_block = (uint8_t *) util::aligned_alloc(leaf_block_size);
        //     descendant->set_leaf(1);
        //     descendant->set_current_block(root_block);
        //     descendant->init_current_block();
        // }
    }

    btree_handler(uint32_t block_sz, uint8_t *block, bool is_leaf, bool should_init = true) :
            block_size (block_sz), cache_size (0), filename (NULL) {
        is_block_given = 1;
        is_closed = false;
        root_block = current_block = block;
        if (should_init) {
            descendant->set_leaf(is_leaf ? 1 : 0);
            descendant->set_current_block(block);
            descendant->init_current_block();
        } else
            descendant->set_current_block(block);
    }

    ~btree_handler() {
        if (!is_closed)
            close();
    }

    void close() {
        descendant->cleanup();
        if (cache_size > 0)
            delete cache;
        else if (!is_block_given)
            free(root_block);
        is_closed = true;
    }

    void init_current_block() {
        //memset(current_block, '\0', BFOS_NODE_SIZE);
        //cout << "Tree init block" << endl;
        if (!is_block_given) {
            descendant->set_filled_size(0);
            descendant->set_kv_last_pos(block_size);
        }
    }

    void init_stats() {
        total_size = max_key_count_leaf = max_key_count_node = block_count_node = 0;
        num_levels = block_count_leaf = 1;
        count1 = count2 = 0;
        max_key_len = 0;
    }

    uint8_t *get_current_block() {
        return current_block;
    }

    std::string get_string(std::string key, std::string not_found_value) {
        bool ret = get(key.c_str(), key.length(), NULL, NULL);
        if (ret) {
            uint8_t *val = (uint8_t *) malloc(key_at_len);
            int val_len;
            descendant->copy_value(val, &val_len);
            return std::string((const char *) val, val_len);
        }
        return not_found_value;
    }

    bool get(const char *key, int key_len, int *in_size_out_val_len = NULL,
                 char *val = NULL, bptree_iter_ctx *ctx = NULL) {
        return get((uint8_t *) key, key_len, in_size_out_val_len, (uint8_t *) val, ctx);
    }
    bool get(const uint8_t *key, int key_len, int *in_size_out_val_len = NULL,
                 uint8_t *val = NULL, bptree_iter_ctx *ctx = NULL) {
        descendant->set_current_block_root();
        current_page = root_page_num;
        if (ctx)
            ctx->init(current_page, current_block, cache_size);
        this->key = key;
        this->key_len = key_len;
        int search_result = traverse_to_leaf(ctx);
        if (search_result < 0)
            return false;
        if (in_size_out_val_len != NULL)
            *in_size_out_val_len = key_at_len;
        if (val != NULL)
            descendant->copy_value(val, in_size_out_val_len);
        return true;
    }

    int traverse_to_leaf(bptree_iter_ctx *ctx = NULL) {
        uint8_t prev_lvl_split_count = 0;
        while (!descendant->is_leaf()) {
            if (ctx) {
                ctx->set(current_page, current_block, cache_size);
                ctx->last_page_lvl++;
            }
            int search_result = descendant->search_current_block(ctx);
            if (search_result >= 0 && is_btree)
                return search_result;
            uint8_t *child_ptr_loc = descendant->get_child_ptr_pos(search_result);
            uint8_t *child_ptr;
            if (cache_size > 0) {
                current_page = descendant->get_child_page(child_ptr_loc);
                child_ptr = cache->get_disk_page_in_cache(current_page);
            } else
                child_ptr = descendant->get_child_ptr(child_ptr_loc);
            descendant->set_current_block(child_ptr);
        }
        return descendant->search_current_block(ctx);
    }

    bool put_string(std::string key, std::string value) {
        return put(key.c_str(), key.length(), value.c_str(), value.length());
    }
    bool put(const char *key, int key_len, const char *value,
            int value_len, bptree_iter_ctx *ctx = NULL) {
        return put((const uint8_t *) key, key_len, (const uint8_t *) value, value_len, ctx);
    }
    bool put(const uint8_t *key, int key_len, const uint8_t *value,
            int value_len, bptree_iter_ctx *ctx = NULL, bool only_if_not_full = false) {
        descendant->set_current_block_root();
        this->key = key;
        this->key_len = key_len;
        if (max_key_len < key_len)
            max_key_len = key_len;
        this->value = value;
        this->value_len = value_len;
        current_page = root_page_num;
        bool is_ctx_given = true;
        if (ctx == NULL) {
            ctx = new bptree_iter_ctx();
            is_ctx_given = false;
        }
        ctx->init(current_page, current_block, cache_size);
        if (descendant->filled_size() == 0) {
            descendant->add_first_data();
            descendant->set_changed(1);
        } else {
            int search_result = traverse_to_leaf(ctx);
            num_levels = ctx->last_page_lvl + 1;
            if (only_if_not_full) {
                if (descendant->is_full(~search_result)) {
                    if (is_ctx_given)
                        delete ctx;
                    return false;
                }
            }
            recursive_update(search_result, ctx, ctx->last_page_lvl);
            if (search_result >= 0) {
                if (is_ctx_given)
                    delete ctx;
                descendant->set_changed(1);
                return true;
            }
        }
        total_size++;
        if (is_ctx_given)
            delete ctx;
        return false;
    }

    void recursive_update(int search_result, bptree_iter_ctx *ctx, int prev_level) {
        if (search_result < 0) {
            search_result = ~search_result;
            if (descendant->is_full(search_result)) {
                update_split_stats();
                uint8_t first_key[descendant->get_first_key_len()]; // is max_pfx_len sufficient?
                int first_len;
                uint8_t *old_block = current_block;
                uint8_t *new_block = descendant->split(first_key, &first_len);
                descendant->set_changed(1);
                int lvl = descendant->get_level(old_block, block_size);
                descendant->set_level(new_block, block_size, lvl);
                int new_page = 0;
                if (cache_size > 0)
                    new_page = cache->get_page_count() - 1;
                int cmp = descendant->compare_first_key(first_key, first_len, key, key_len);
                if (cmp <= 0)
                    descendant->set_current_block(new_block);
                search_result = ~descendant->search_current_block();
                descendant->add_data(search_result);
                //cout << "FK:" << level << ":" << first_key << endl;
                if (root_block == old_block) {
                    int new_lvl = lvl;
                    if (new_lvl == BPT_LEAF0_LVL)
                      new_lvl = BPT_PARENT0_LVL;
                    else if (new_lvl >= BPT_PARENT0_LVL)
                      new_lvl++;
                    block_count_node++;
                    int old_page = 0;
                    if (cache_size > 0) {
                        old_block = descendant->allocate_block(block_size, descendant->is_leaf(), new_lvl);
                        old_page = cache->get_page_count() - 1;
                        memcpy(old_block, root_block, block_size);
                        descendant->set_block_changed(old_block, block_size, true);
                    }
                    // } else
                    //     root_block = (uint8_t *) util::aligned_alloc(parent_block_size);
                    descendant->set_current_block(root_block);
                    descendant->set_leaf(0);
                    descendant->init_current_block();
                    descendant->set_changed(1);
                    descendant->set_level(current_block, block_size, new_lvl);
                    descendant->add_first_kv_to_root(first_key, first_len,
                        cache_size > 0 ? (unsigned long) old_page : (unsigned long) old_block,
                        cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block);
                    num_levels++;
                } else {
                    prev_level = prev_level - 1;
                    current_page = ctx->pages[prev_level].page;
                    uint8_t *parent_data = cache_size > 0 ? cache->get_disk_page_in_cache(current_page) : ctx->pages[prev_level].ptr;
                    descendant->set_current_block(parent_data);
                    uint8_t addr[9];
                    search_result = descendant->prepare_kv_to_add_to_parent(first_key, first_len, 
                                        cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block, addr);
                    recursive_update(search_result, ctx, prev_level);
                }
            } else {
                descendant->add_data(search_result);
                descendant->set_changed(1);
            }
        } else {
            descendant->update_data();
        }
    }

    inline void update_split_stats() {
        if (descendant->is_leaf()) {
            max_key_count_leaf += descendant->filled_size();
            block_count_leaf++;
        } else {
            max_key_count_node += descendant->filled_size();
            block_count_node++;
        }
    }
    int get_num_levels() {
        return num_levels;
    }

    void print_stats(long num_entries) {
        std::cout << "Block Count:";
        std::cout << (long) block_count_node;
        std::cout << ", ";
        std::cout << (long) block_count_leaf;
        std::cout << std::endl;
        std::cout << "Avg Block Count:";
        std::cout << (long) (num_entries / block_count_leaf);
        std::cout << " = ";
        std::cout << (long) num_entries;
        std::cout << " / ";
        std::cout << (long) block_count_leaf;
        std::cout << std::endl;
        std::cout << "Avg Max Count:";
        std::cout << (long) (max_key_count_node / (block_count_node ? block_count_node : 1));
        std::cout << ", ";
        std::cout << (long) (max_key_count_leaf / block_count_leaf);
        std::cout << ", ";
        std::cout << (long) max_key_len;
        std::cout << std::endl;
    }
    void print_num_levels() {
        std::cout << "Level Count:";
        std::cout << (long) num_levels;
        std::cout << std::endl;
    }
    void print_counts() {
        std::cout << "Count1:";
        std::cout << count1;
        std::cout << ", Count2:";
        std::cout << count2;
        std::cout << "\n";
        std::cout << std::endl;
    }
    long size() {
        return total_size;
    }
    int get_max_key_len() {
        return max_key_len;
    }
    cache_stats get_cache_stats() {
        return cache->get_cache_stats();
    }

};

#endif
