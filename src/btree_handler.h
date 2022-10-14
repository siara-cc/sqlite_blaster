#ifndef BP_TREE_H
#define BP_TREE_H
#ifdef ARDUINO
#include <HardwareSerial.h>
#include <string.h>
#else
#include <cstdio>
#include <cstring>
#include <iostream>
#endif
#include <stdint.h>
#include "lru_cache.h"

using namespace std;

#define BPT_IS_LEAF_BYTE current_block[0]
#define BPT_FILLED_SIZE current_block + 1
#define BPT_LAST_DATA_PTR current_block + 3
#define BPT_MAX_PFX_LEN current_block[8]

template<class T> // CRTP
class btree_handler {
protected:
    long total_size;
    int numLevels;
    int maxKeyCountNode;
    int maxKeyCountLeaf;
    int blockCountNode;
    int blockCountLeaf;
    long count1, count2;
    int max_key_len;
    lru_cache *cache;

public:
    byte *header_block;
    byte *root_block;
    byte *current_block;
    const char *key;
    byte key_len;
    byte *key_at;
    byte key_at_len;
    const char *value;
    int16_t value_len;

    const uint16_t page_size;
    const int cache_size;
    const char *filename;
    btree_handler(char *filename, int page_size, int cache_size) :
            filename (filename), page_size (page_size), cache_size (cache_size)) {
        init_stats();
        if (cache_size > 0) {
            cache = new lru_cache(page_size, cache_size, filename);
            root_block = current_block = cache->get_disk_page_in_cache(0);
            if (cache->is_empty()) {
                static_cast<T*>(this)->initCurrentBlock();
            }
        } else {
            root_block = current_block = (byte *) malloc(page_size);
            static_cast<T*>(this)->initCurrentBlock();
        }
    }

    btree_handler() {
        // this is just to avoid error saying No default constructor exists
        // the other constructor must be called from descendants of this class
    }

    ~btree_handler() {
        if (cache_size > 0)
            delete cache;
    }

    void initCurrentBlock() {
        //memset(current_block, '\0', BFOS_NODE_SIZE);
        //cout << "Tree init block" << endl;
        setLeaf(1);
        setFilledSize(0);
        BPT_MAX_KEY_LEN = 1;
        setKVLastPos(leaf_block_size);
    }

    void init_stats() {
        total_size = maxKeyCountLeaf = maxKeyCountNode = blockCountNode = 0;
        numLevels = blockCountLeaf = 1;
        count1 = count2 = 0;
        max_key_len = 0;
    }

    void setCurrentBlockRoot();
    void setCurrentBlock(byte *m);
    char *get(const char *key, uint8_t key_len, int16_t *pValueLen) {
        static_cast<T*>(this)->setCurrentBlockRoot();
        this->key = key;
        this->key_len = key_len;
        if ((isLeaf() ? static_cast<T*>(this)->searchCurrentBlock() : traverseToLeaf()) < 0)
            return null;
        return getValueAt(pValueLen);
    }

    inline bool isLeaf() {
        return current_block[0] == 10; // || current_block[0] == 13;
    }

    int16_t searchCurrentBlock();
    byte *getKey(byte *t, byte *plen);

    inline int getPtr(int16_t pos) {
#if BPT_9_BIT_PTR == 1
        uint16_t ptr = *(static_cast<T*>(this)->getPtrPos() + pos);
#if BPT_INT64MAP == 1
        if (*bitmap & MASK64(pos))
        ptr |= 256;
#else
        if (pos & 0xFFE0) {
            if (*bitmap2 & MASK32(pos - 32))
            ptr |= 256;
        } else {
            if (*bitmap1 & MASK32(pos))
            ptr |= 256;
        }
#endif
        return ptr;
#else
        return util::getInt(static_cast<T*>(this)->getPtrPos() + (pos << 1));
#endif
    }
    byte *getPtrPos();

    int16_t traverseToLeaf(int8_t *plevel_count = NULL, byte *node_paths[] = NULL) {
        while (!isLeaf()) {
            if (node_paths) {
                *node_paths++ = current_block;
                (*plevel_count)++;
            }
            int16_t search_result = static_cast<T*>(this)->searchCurrentBlock();
            byte *child_ptr_loc = static_cast<T*>(this)->getChildPtrPos(search_result);
            byte *child_ptr;
            if (cache_size > 0) {
                int child_page = getChildPage(child_ptr_loc);
                child_ptr = cache->get_disk_page_in_cache(child_page);
            } else
                child_ptr = getChildPtr(child_ptr_loc);
            static_cast<T*>(this)->setCurrentBlock(child_ptr);
        }
        return static_cast<T*>(this)->searchCurrentBlock();
    }

    byte *getLastPtr();
    byte *getChildPtrPos(int16_t search_result);
    inline byte *getChildPtr(byte *ptr) {
        ptr += (*ptr + 1);
        return (byte *) util::bytesToPtr(ptr);
    }

    inline int getChildPage(byte *ptr) {
        ptr += (*ptr + 1);
        return util::bytesToPtr(ptr);
    }

    inline int16_t filledSize() {
        return util::getInt(BPT_FILLED_SIZE);
    }

    inline uint16_t getKVLastPos() {
        return util::getInt(BPT_LAST_DATA_PTR);
    }

    byte *allocateBlock(int size) {
        if (cache_size > 0)
            return cache->writeNewPage(current_block);
        return (byte *) util::alignedAlloc(size);
    }

    void put(const char *key, uint8_t key_len, const char *value,
            int16_t value_len) {
        static_cast<T*>(this)->setCurrentBlockRoot();
        this->key = key;
        this->key_len = key_len;
        if (max_key_len < key_len)
            max_key_len = key_len;
        this->value = value;
        this->value_len = value_len;
        if (filledSize() == 0) {
            static_cast<T*>(this)->addFirstData();
        } else {
            byte *node_paths[7];
            int8_t level_count = 1;
            int16_t search_result = isLeaf() ?
                    static_cast<T*>(this)->searchCurrentBlock() :
                    traverseToLeaf(&level_count, node_paths);
            recursiveUpdate(search_result, node_paths, level_count - 1);
        }
        total_size++;
    }

    void recursiveUpdate(int16_t search_result, byte *node_paths[], byte level) {
        //int16_t search_result = pos; // lastSearchPos[level];
        if (search_result < 0) {
            search_result = ~search_result;
            if (static_cast<T*>(this)->isFull(search_result)) {
                updateSplitStats();
                byte first_key[BPT_MAX_KEY_LEN]; // is max_pfx_len sufficient?
                int16_t first_len;
                byte *old_block = current_block;
                byte *new_block = static_cast<T*>(this)->split(first_key, &first_len);
                int new_page = 0;
                if (cache_size > 0)
                    new_page = cache->get_page_count() - 1;
                int16_t cmp = util::compare((char *) first_key, first_len,
                        key, key_len);
                if (cmp <= 0)
                    static_cast<T*>(this)->setCurrentBlock(new_block);
                search_result = ~static_cast<T*>(this)->searchCurrentBlock();
                static_cast<T*>(this)->addData(search_result);
                //cout << "FK:" << level << ":" << first_key << endl;
                if (root_block == old_block) {
                    blockCountNode++;
                    int old_page = 0;
                    if (cache_size > 0) {
                        old_block = cache->writeNewPage(NULL);
                        memcpy(old_block, root_block, parent_block_size);
                        old_page = cache->get_page_count() - 1;
                    } else
                    root_block = (byte *) util::alignedAlloc(parent_block_size);
                    static_cast<T*>(this)->setCurrentBlock(root_block);
                    static_cast<T*>(this)->initCurrentBlock();
                    setLeaf(0);
                    if (getKVLastPos() == leaf_block_size)
                        setKVLastPos(parent_block_size);
                    byte addr[9];
                    key = "";
                    key_len = 1;
                    value = (char *) addr;
                    value_len = util::ptrToBytes(cache_size > 0 ? (unsigned long) old_page : (unsigned long) old_block, addr);
                    static_cast<T*>(this)->addFirstData();
                    key = (char *) first_key;
                    key_len = first_len;
                    value = (char *) addr;
                    value_len = util::ptrToBytes(cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block, addr);
                    search_result = ~static_cast<T*>(this)->searchCurrentBlock();
                    static_cast<T*>(this)->addData(search_result);
                    numLevels++;
                } else {
                    int16_t prev_level = level - 1;
                    byte *parent_data = node_paths[prev_level];
                    static_cast<T*>(this)->setCurrentBlock(parent_data);
                    byte addr[9];
                    key = (char *) first_key;
                    key_len = first_len;
                    value = (char *) addr;
                    value_len = util::ptrToBytes(cache_size > 0 ? (unsigned long) new_page : (unsigned long) new_block, addr);
                    search_result = static_cast<T*>(this)->searchCurrentBlock();
                    recursiveUpdate(search_result, node_paths, prev_level);
                }
            } else
                static_cast<T*>(this)->addData(search_result);
        } else {
            //if (isLeaf()) {
            //    this->key_at += this->key_at_len;
            //    if (*key_at == this->value_len) {
            //        memcpy((char *) key_at + 1, this->value, this->value_len);
            //}
        }
    }

    bool isFull(int16_t search_result);
    void addFirstData();
    void addData(int16_t search_result);
    void insertCurrent();

    inline void setFilledSize(int16_t filledSize) {
        util::setInt(BPT_FILLED_SIZE, filledSize);
    }

    inline void insPtr(int16_t pos, uint16_t kv_pos) {
        int16_t filledSz = filledSize();
#if BPT_9_BIT_PTR == 1
        byte *kvIdx = static_cast<T*>(this)->getPtrPos() + pos;
        memmove(kvIdx + 1, kvIdx, filledSz - pos);
        *kvIdx = kv_pos;
#if BPT_INT64MAP == 1
        insBit(bitmap, pos, kv_pos);
#else
        if (pos & 0xFFE0) {
            insBit(bitmap2, pos - 32, kv_pos);
        } else {
            byte last_bit = (*bitmap1 & 0x01);
            insBit(bitmap1, pos, kv_pos);
            *bitmap2 >>= 1;
            if (last_bit)
            *bitmap2 |= MASK32(0);
        }
#endif
#else
        byte *kvIdx = static_cast<T*>(this)->getPtrPos() + (pos << 1);
        memmove(kvIdx + 2, kvIdx, (filledSz - pos) * 2);
        util::setInt(kvIdx, kv_pos);
#endif
        setFilledSize(filledSz + 1);

    }

    inline void setPtr(int16_t pos, uint16_t ptr) {
#if BPT_9_BIT_PTR == 1
        *(static_cast<T*>(this)->getPtrPos() + pos) = ptr;
#if BPT_INT64MAP == 1
        if (ptr >= 256)
        *bitmap |= MASK64(pos);
        else
        *bitmap &= ~MASK64(pos);
#else
        if (pos & 0xFFE0) {
            pos -= 32;
            if (ptr >= 256)
            *bitmap2 |= MASK32(pos);
            else
            *bitmap2 &= ~MASK32(pos);
        } else {
            if (ptr >= 256)
            *bitmap1 |= MASK32(pos);
            else
            *bitmap1 &= ~MASK32(pos);
        }
#endif
#else
        byte *kvIdx = static_cast<T*>(this)->getPtrPos() + (pos << 1);
        return util::setInt(kvIdx, ptr);
#endif
    }

    inline void updateSplitStats() {
        if (isLeaf()) {
            maxKeyCountLeaf += filledSize();
            blockCountLeaf++;
        } else {
            maxKeyCountNode += filledSize();
            blockCountNode++;
        }
    }

    inline void setLeaf(char isLeaf) {
        BPT_IS_LEAF_BYTE = isLeaf;
    }

    inline void setKVLastPos(uint16_t val) {
        util::setInt(BPT_LAST_DATA_PTR, val);
    }

    inline void insBit(uint32_t *ui32, int pos, uint16_t kv_pos) {
        uint32_t ryte_part = (*ui32) & RYTE_MASK32(pos);
        ryte_part >>= 1;
        if (kv_pos >= 256)
            ryte_part |= MASK32(pos);
        (*ui32) = (ryte_part | ((*ui32) & LEFT_MASK32(pos)));

    }

    byte *split(byte *first_key, int16_t *first_len_ptr);
#if BPT_INT64MAP == 1
    inline void insBit(uint64_t *ui64, int pos, uint16_t kv_pos) {
        uint64_t ryte_part = (*ui64) & RYTE_MASK64(pos);
        ryte_part >>= 1;
        if (kv_pos >= 256)
            ryte_part |= MASK64(pos);
        (*ui64) = (ryte_part | ((*ui64) & LEFT_MASK64(pos)));

    }
#endif

    void printStats(long num_entries) {
        util::print("Block Count:");
        util::print((long) blockCountNode);
        util::print(", ");
        util::print((long) blockCountLeaf);
        util::endl();
        util::print("Avg Block Count:");
        util::print((long) (num_entries / blockCountLeaf));
        util::endl();
        util::print("Avg Max Count:");
        util::print((long) (maxKeyCountNode / (blockCountNode ? blockCountNode : 1)));
        util::print(", ");
        util::print((long) (maxKeyCountLeaf / blockCountLeaf));
        util::print(", ");
        util::print((long) max_key_len);
        util::endl();
    }
    void printNumLevels() {
        util::print("Level Count:");
        util::print((long) numLevels);
        util::endl();
    }
    void printCounts() {
        util::print("Count1:");
        util::print(count1);
        util::print(", Count2:");
        util::print(count2);
        util::print("\n");
        util::endl();
    }
    long size() {
        return total_size;
    }
    int get_max_key_len() {
        return max_key_len;
    }

};

#endif
