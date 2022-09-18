#ifndef LRUCACHE_H
#define LRUCACHE_H
#include <set>
#include <unordered_map>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

typedef unsigned char byte;

using namespace std;

typedef struct dbl_lnklst_st {
    int disk_page;
    int cache_loc;
    struct dbl_lnklst_st *prev;
    struct dbl_lnklst_st *next;
} dbl_lnklst;

typedef struct {
    int cache_misses_since;
    int total_cache_misses_since;
    int max_cache_misses_since;
    int total_cache_misses;
    int cache_flush_count;
} cache_stats;

class lru_cache {
protected:
    int page_size;
    byte *page_cache;
    byte *root_block;
    int cache_size_in_pages;
    int cache_occupied_size;
    int skip_page_count;
    dbl_lnklst *lnklst_first_entry;
    dbl_lnklst *lnklst_last_entry;
    unordered_map<int, dbl_lnklst*> disk_to_cache_map;
    dbl_lnklst *llarr;
    const char *filename;
    FILE *fp;
    int file_page_count;
    byte empty;
    cache_stats stats;
public:
    lru_cache(int pg_size, int page_count, const char *fname, int init_page_count = 0, void *(*alloc_fn)(size_t) = NULL) throw(std::exception) {
        if (alloc_fn == NULL)
            alloc_fn = malloc;
        page_size = pg_size;
        cache_size_in_pages = page_count;
        cache_occupied_size = 0;
        lnklst_first_entry = lnklst_last_entry = NULL;
        filename = fname;
        page_cache = (byte *) alloc_fn(pg_size * page_count);
        root_block = (byte *) alloc_fn(pg_size);
        llarr = (dbl_lnklst *) alloc_fn(page_count * sizeof(dbl_lnklst));
        disk_to_cache_map.reserve(page_count);
        skip_page_count = init_page_count;
        file_page_count = init_page_count;
        fp = fopen(fname, "rb+");
        if (fp == NULL) {
            fp = fopen(fname, "wb");
            fclose(fp);
            fp = fopen(fname, "rb+");
            if (fp == NULL)
              throw errno;
        }
        if (!fseek(fp, 0, SEEK_END)) {
            file_page_count = ftell(fp);
            if (file_page_count > 0)
                file_page_count /= page_size;
            fseek(fp, 0, SEEK_SET);
        }
        empty = 0;
        if (fread(root_block, 1, page_size, fp) != page_size) {
            file_page_count = 1;
            fseek(fp, 0, SEEK_SET);
            if (fwrite(root_block, 1, page_size, fp) != page_size)
                throw EIO;
            empty = 1;
        }
        memset(&stats, '\0', sizeof(stats));
    }
    ~lru_cache() throw(std::exception) {
        set<int> pages_to_write;
        for (unordered_map<int, dbl_lnklst*>::iterator it = disk_to_cache_map.begin(); it != disk_to_cache_map.end(); it++) {
            byte *block = &page_cache[page_size * it->second->cache_loc];
            if (block[0] & 0x02) // is it changed
                pages_to_write.insert(it->first);
        }
        for (int it : pages_to_write) {
            byte *block = &page_cache[page_size * disk_to_cache_map[it]->cache_loc];
            block[0] &= 0xFD; // unchange it
            fseek(fp, page_size * it, SEEK_SET);
            int write_count = fwrite(block, 1, page_size, fp);
            if (write_count != page_size)
                throw EIO;
        }
        free(page_cache);
        fseek(fp, 0, SEEK_SET);
        int write_count = fwrite(root_block, 1, page_size, fp);
        if (write_count != page_size)
            throw EIO;
        fclose(fp);
        free(root_block);
        free(llarr);
        cout << "max_cache_misses_since: " << " " << stats.max_cache_misses_since << endl;
        cout << "total_cache_misses: " << " " << stats.total_cache_misses << endl;
        cout << "cache_flush_count: " << " " << stats.cache_flush_count << endl;
        cout << "Avg. cache misses: " << " " 
            << stats.total_cache_misses_since / (stats.cache_flush_count ? stats.cache_flush_count : 1) << endl;
    }
    void move_to_front(dbl_lnklst *entry_to_move) {
        if (entry_to_move != lnklst_first_entry) {
            if (entry_to_move == lnklst_last_entry)
                lnklst_last_entry = lnklst_last_entry->prev;
            entry_to_move->prev->next = entry_to_move->next;
            if (entry_to_move->next != NULL)
                entry_to_move->next->prev = entry_to_move->prev;
            entry_to_move->next = lnklst_first_entry;
            entry_to_move->prev = NULL;
            lnklst_first_entry->prev = entry_to_move;
            lnklst_first_entry = entry_to_move;
        }
    }
    byte *get_disk_page_in_cache(int disk_page, byte *block_to_keep = NULL) {
        if (disk_page == skip_page_count)
            return root_block;
        int cache_pos = 0;
        int removed_disk_page = 0;
        if (disk_to_cache_map.find(disk_page) == disk_to_cache_map.end()) {
            if (cache_occupied_size < cache_size_in_pages) {
                dbl_lnklst *new_entry = &llarr[cache_occupied_size]; // new dbl_lnklst();
                new_entry->disk_page = disk_page;
                new_entry->cache_loc = cache_occupied_size;
                new_entry->prev = lnklst_last_entry;
                new_entry->next = NULL;
                if (lnklst_last_entry != NULL)
                    lnklst_last_entry->next = new_entry;
                lnklst_last_entry = new_entry;
                if (lnklst_first_entry == NULL)
                    lnklst_first_entry = new_entry;
                disk_to_cache_map[disk_page] = new_entry;
                cache_pos = cache_occupied_size++;
                if (!fseek(fp, page_size * disk_page, SEEK_SET)) {
                    int read_count = fread(&page_cache[page_size * cache_pos], 1, page_size, fp);
                    if (read_count != page_size) {} // ignore if unable to read the page - it may not exist
                        //cout << "2:Only " << read_count << " bytes read at position: " << page_size * disk_page << endl;
                }
            } else {
                dbl_lnklst *entry_to_move = lnklst_last_entry;
                if (block_to_keep == &page_cache[page_size * entry_to_move->cache_loc])
                    entry_to_move = lnklst_last_entry->prev;
                removed_disk_page = entry_to_move->disk_page;
                cache_pos = entry_to_move->cache_loc;
                byte *block = &page_cache[page_size * cache_pos];
                if (block[0] & 0x02) { // is it changed
                    block[0] &= 0xFD; // unchange it
                    fseek(fp, page_size * removed_disk_page, SEEK_SET);
                    int write_count = fwrite(block, 1, page_size, fp);
                    if (write_count != page_size)
                        throw EIO;
                }
                move_to_front(entry_to_move);
                entry_to_move->disk_page = disk_page;
                disk_to_cache_map.erase(removed_disk_page);
                disk_to_cache_map[disk_page] = entry_to_move;
                if (!fseek(fp, page_size * disk_page, SEEK_SET)) {
                    int read_count = fread(&page_cache[page_size * cache_pos], 1, page_size, fp);
                    if (read_count != page_size) {}
                        //throw EIO; // Attempting to read non-existent page
                }
                stats.cache_misses_since++;
                stats.total_cache_misses++;
            }
        } else {
            dbl_lnklst *current_entry = disk_to_cache_map[disk_page];
            move_to_front(current_entry);
            cache_pos = current_entry->cache_loc;
        }
        return &page_cache[page_size * cache_pos];
    }
    byte *writeNewPage(byte *block_to_keep) {
        byte *new_page = get_disk_page_in_cache(file_page_count, block_to_keep);
        if (block_to_keep != NULL && cache_occupied_size == cache_size_in_pages) {
            stats.cache_flush_count++;
            stats.total_cache_misses_since += stats.cache_misses_since;
            if (stats.cache_misses_since > stats.max_cache_misses_since)
                stats.max_cache_misses_since = stats.cache_misses_since;
            int pages_to_check = stats.total_cache_misses_since / stats.cache_flush_count;
            pages_to_check *= 2;
            if (pages_to_check > 200)
                pages_to_check = 200;
            if (pages_to_check < 30)
                pages_to_check = 30;
            set<int> pages_to_write;
            dbl_lnklst *cur_entry = lnklst_last_entry;
            pages_to_write.insert(file_page_count);
            do {
                byte *block = &page_cache[cur_entry->cache_loc * page_size];
                if (block[0] & 0x02) // is it changed
                    pages_to_write.insert(cur_entry->disk_page);
                cur_entry = cur_entry->prev;
            } while (--pages_to_check && cur_entry);
            for (int it : pages_to_write) {
                byte *block = &page_cache[page_size * disk_to_cache_map[it]->cache_loc];
                block[0] &= 0xFD; // unchange it
                fseek(fp, page_size * it, SEEK_SET);
                int write_count = fwrite(block, 1, page_size, fp);
                if (write_count != page_size)
                    throw EIO;
            }
        } else {
            if (fseek(fp, file_page_count * page_size, SEEK_SET))
                fseek(fp, 0, SEEK_END);
            int write_count = fwrite(new_page, 1, page_size, fp);
            if (write_count != page_size)
                throw EIO;
        }
        fflush(fp);
        stats.cache_misses_since = 0;
        file_page_count++;
        return new_page;
    }
    int get_page_count() {
        return file_page_count;
    }
    byte is_empty() {
        return empty;
    }
    cache_stats get_cache_stats() {
        return stats;
    }
};
#endif
