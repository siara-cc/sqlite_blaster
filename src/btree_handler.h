#ifndef BP_TREE_H
#define BP_TREE_H
#ifdef ARDUINO
#include <Hardware_serial.h>
#include <string.h>
#else
#include <cstdio>
#include <cstring>
#include <iostream>
#endif
#include <stdint.h>
#include "lru_cache.h"

using namespace std;

#define BPT_LEAF0_LVL 14
#define BPT_STAGING_LVL 15
#define BPT_PARENT0_LVL 16

#define BPT_BLK_TYPE_INTERIOR 0
#define BPT_BLK_TYPE_LEAF 128
#define BPT_BLK_TYPE_OVFL 255

#define DEFAULT_BLOCK_SIZE 4096

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
#if BPT_9_BIT_PTR == 1
#if BPT_INT64MAP == 1
    uint64_t *bitmap;
#else
    uint32_t *bitmap1;
    uint32_t *bitmap2;
#endif
#endif

    size_t block_size;
    int cache_size;
    const char *filename;
    bool to_demote_blocks;
    btree_handler(uint32_t block_sz = DEFAULT_BLOCK_SIZE, int cache_sz_mb = 32,
            const char *fname = NULL, int start_page_num = 0, bool whether_btree = false) :
            block_size (block_sz), cache_size (cache_sz_mb & 0xFFFF), filename (fname) {
        static_cast<T*>(this)->init_derived();
        init_stats();
        is_block_given = 0;
        root_page_num = start_page_num;
        is_btree = whether_btree;
        to_demote_blocks = false;

            cache = new lru_cache(block_size, cache_size, filename,
                    &static_cast<T*>(this)->is_block_changed, &static_cast<T*>(this)->set_block_changed,
                    start_page_num);
            root_block = current_block = cache->get_disk_page_in_cache(start_page_num);
            if (cache->is_empty()) {
                static_cast<T*>(this)->set_leaf(1);
                static_cast<T*>(this)->init_current_block();
            }

    }

    btree_handler(uint32_t block_sz, uint8_t *block, bool is_leaf, bool should_init = true) :
            block_size (block_sz), cache_size (0), filename (NULL) {
        is_block_given = 1;
        root_block = current_block = block;
        if (should_init) {
            static_cast<T*>(this)->set_leaf(is_leaf ? 1 : 0);
            static_cast<T*>(this)->set_current_block(block);
            static_cast<T*>(this)->init_current_block();
        } else
            static_cast<T*>(this)->set_current_block(block);
    }

    ~btree_handler() {
        static_cast<T*>(this)->cleanup();
        if (cache_size > 0)
            delete cache;
        else if (!is_block_given)
            free(root_block);
    }

    void init_current_block() {
        //memset(current_block, '\0', BFOS_NODE_SIZE);
        //cout << "Tree init block" << endl;
        if (!is_block_given) {
            static_cast<T*>(this)->set_filled_size(0);
            static_cast<T*>(this)->set_kv_last_pos(block_size);
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

    char *get(const char *key, int key_len, int *out_value_len, char *val = NULL) {
        return (char *) get((uint8_t *) key, key_len, out_value_len, (uint8_t *) val);
    }
    uint8_t *get(const uint8_t *key, int key_len, int *out_value_len, uint8_t *val = NULL) {
        static_cast<T*>(this)->set_current_block_root();
        this->key = key;
        this->key_len = key_len;
        current_page = root_page_num;
        int search_result;
        if (static_cast<T*>(this)->is_leaf())
            search_result = static_cast<T*>(this)->search_current_block();
        else
            search_result = traverse_to_leaf();
        if (search_result < 0)
            return NULL;
        if (val != NULL)
            static_cast<T*>(this)->copy_value(val, out_value_len);
        return static_cast<T*>(this)->get_value_at(out_value_len);
    }

    int traverse_to_leaf(int8_t *plevel_count = NULL, uint8_t *node_paths[] = NULL) {
        current_page = root_page_num;
        uint8_t prev_lvl_split_count = 0;
        uint8_t *btree_rec_ptr = NULL;
        int btree_rec_at_len = 0;
        uint8_t *btree_found_blk = NULL;
        while (!static_cast<T*>(this)->is_leaf()) {
            if (node_paths) {
                *node_paths++ = cache_size > 0 ? (uint8_t *) current_page : current_block;
                (*plevel_count)++;
            }
            int search_result = static_cast<T*>(this)->search_current_block();
            if (search_result >= 0 && is_btree) {
                btree_rec_ptr = key_at;
                btree_rec_at_len = key_at_len;
                btree_found_blk = current_block;
            }
            uint8_t *child_ptr_loc = static_cast<T*>(this)->get_child_ptr_pos(search_result);
            if (to_demote_blocks) {
                prev_lvl_split_count = child_ptr_loc[*child_ptr_loc + 1 + child_ptr_loc[*child_ptr_loc+1]];
            }
            uint8_t *child_ptr;
            if (cache_size > 0) {
                current_page = static_cast<T*>(this)->get_child_page(child_ptr_loc);
                child_ptr = cache->get_disk_page_in_cache(current_page, btree_found_blk);
            } else
                child_ptr = static_cast<T*>(this)->get_child_ptr(child_ptr_loc);
            static_cast<T*>(this)->set_current_block(child_ptr);
            //if (to_demote_blocks && current_block[5] < 255 && prev_lvl_split_count != current_block[5] - 1) {
            //    cout << "Split count not matching: " << (int) prev_lvl_split_count << " " << (int) current_block[5] << " " << (int) (current_block[0] & 0x1F) << endl;
            //}
        }
        if (btree_found_blk != NULL) {
            key_at = btree_rec_ptr;
            key_at_len = btree_rec_at_len;
            return 0;
        }
        return static_cast<T*>(this)->search_current_block();
    }

    char *put(const char *key, int key_len, const char *value,
            int value_len, int *out_value_len = NULL) {
        return (char *) put((const uint8_t *) key, key_len, (const uint8_t *) value, value_len, out_value_len);
    }
    uint8_t *put(const uint8_t *key, int key_len, const uint8_t *value,
            int value_len, int *out_value_len = NULL, bool only_if_not_full = false) {
        static_cast<T*>(this)->set_current_block_root();
        this->key = key;
        this->key_len = key_len;
        if (max_key_len < key_len)
            max_key_len = key_len;
        this->value = value;
        this->value_len = value_len;
        if (static_cast<T*>(this)->filled_size() == 0) {
            static_cast<T*>(this)->add_first_data();
            static_cast<T*>(this)->set_changed(1);
        } else {
            current_page = root_page_num;
            uint8_t **node_paths = (uint8_t **) malloc(8 * sizeof(void *));
            int8_t level_count = 1;
            int search_result = static_cast<T*>(this)->is_leaf() ?
                    static_cast<T*>(this)->search_current_block() :
                    traverse_to_leaf(&level_count, node_paths);
            num_levels = level_count;
            if (only_if_not_full) {
                if (static_cast<T*>(this)->is_full(~search_result)) {
                    *out_value_len = 9999;
                    free(node_paths);
                    return NULL;
                }
            }
            recursive_update(search_result, node_paths, level_count - 1);
            if (search_result >= 0) {
                free(node_paths);
                static_cast<T*>(this)->set_changed(1);
                return static_cast<T*>(this)->get_value_at(out_value_len);
            }
            free(node_paths);
        }
        total_size++;
        return NULL;
    }

    void recursive_update(int search_result, uint8_t *node_paths[], uint8_t level) {
        if (search_result < 0) {
            search_result = ~search_result;
            if (static_cast<T*>(this)->is_full(search_result)) {
                update_split_stats();
                uint8_t first_key[static_cast<T*>(this)->get_first_key_len()]; // is max_pfx_len sufficient?
                int first_len;
                uint8_t *old_block = current_block;
                uint8_t *new_block = static_cast<T*>(this)->split(first_key, &first_len);
                static_cast<T*>(this)->set_changed(1);
                int lvl = static_cast<T*>(this)->get_level(old_block, block_size);
                static_cast<T*>(this)->set_level(new_block, block_size, lvl);
                int new_page = 0;
                if (cache_size > 0)
                    new_page = cache->get_page_count() - 1;
                int cmp = static_cast<T*>(this)->compare_first_key(first_key, first_len, key, key_len);
                if (cmp <= 0)
                    static_cast<T*>(this)->set_current_block(new_block);
                search_result = ~static_cast<T*>(this)->search_current_block();
                static_cast<T*>(this)->add_data(search_result);
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
                        old_block = static_cast<T*>(this)->allocate_block(block_size, static_cast<T*>(this)->is_leaf(), new_lvl);
                        old_page = cache->get_page_count() - 1;
                        memcpy(old_block, root_block, block_size);
                        static_cast<T*>(this)->set_block_changed(old_block, block_size, true);
                    }// else
                    //    root_block = (uint8_t *) util::aligned_alloc(block_size);
                    static_cast<T*>(this)->set_current_block(root_block);
                    static_cast<T*>(this)->set_leaf(0);
                    static_cast<T*>(this)->init_current_block();
                    static_cast<T*>(this)->set_changed(1);
                    static_cast<T*>(this)->set_level(current_block, block_size, new_lvl);
                    static_cast<T*>(this)->add_first_kv_to_root(first_key, first_len,
                        cache_size > 0 ? (unsigned long) old_page : (unsigned long) old_block,
                        cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block);
                    num_levels++;
                } else {
                    int prev_level = level - 1;
                    current_page = (unsigned long) node_paths[prev_level];
                    uint8_t *parent_data = cache_size > 0 ? cache->get_disk_page_in_cache(current_page) : node_paths[prev_level];
                    static_cast<T*>(this)->set_current_block(parent_data);
                    uint8_t addr[9];
                    search_result = static_cast<T*>(this)->prepare_kv_to_add_to_parent(first_key, first_len, 
                                        cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block, addr);
                    recursive_update(search_result, node_paths, prev_level);
                }
            } else {
                static_cast<T*>(this)->add_data(search_result);
                static_cast<T*>(this)->set_changed(1);
            }
        } else {
            static_cast<T*>(this)->update_data();
        }
    }

    inline void update_split_stats() {
        if (static_cast<T*>(this)->is_leaf()) {
            max_key_count_leaf += static_cast<T*>(this)->filled_size();
            block_count_leaf++;
        } else {
            max_key_count_node += static_cast<T*>(this)->filled_size();
            block_count_node++;
        }
    }

    int get_num_levels() {
        return num_levels;
    }

    void print_stats(long num_entries) {
        cout << "Block Count:";
        cout << (long) block_count_node;
        cout << ", ";
        cout << (long) block_count_leaf;
        cout << endl;
        cout << "Avg Block Count:";
        cout << (long) (num_entries / block_count_leaf);
        cout << " = ";
        cout << (long) num_entries;
        cout << " / ";
        cout << (long) block_count_leaf;
        cout << endl;
        cout << "Avg Max Count:";
        cout << (long) (max_key_count_node / (block_count_node ? block_count_node : 1));
        cout << ", ";
        cout << (long) (max_key_count_leaf / block_count_leaf);
        cout << ", ";
        cout << (long) max_key_len;
        cout << endl;
    }
    void print_num_levels() {
        cout << "Level Count:";
        cout << (long) num_levels;
        cout << endl;
    }
    void print_counts() {
        cout << "Count1:";
        cout << count1;
        cout << ", Count2:";
        cout << count2;
        cout << "\n";
        cout << endl;
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
