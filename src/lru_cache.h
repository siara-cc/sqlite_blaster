#ifndef LRUCACHE_H
#define LRUCACHE_H
#include <set>
#include <unordered_map>
#include <iostream>
#ifndef _FILE_OFFSET_BITS
#define _FILE_OFFSET_BITS 64
#endif
#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif
#include <stdio.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>

#define USE_FOPEN 1

typedef unsigned char byte;

using namespace std;

typedef struct dbl_lnklst_st {
    int disk_page;
    int cache_loc;
    struct dbl_lnklst_st *prev;
    struct dbl_lnklst_st *next;
} dbl_lnklst;

typedef struct {
    int total_cache_misses;
    int cache_flush_count;
    int pages_written;
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
    set<int> new_pages;
    const char *filename;
#if USE_FOPEN == 1
    FILE *fp;
#else
    int fd;
#endif
    size_t file_page_count;
    size_t last_pages_to_flush;
    byte empty;
    cache_stats stats;
    void write_page(byte *block, off64_t file_pos, size_t bytes, bool is_new = true) {
        //if (is_new)
        //  fseek(fp, 0, SEEK_END);
        //else
#if USE_FOPEN == 1
        if (fseeko64(fp, file_pos, SEEK_SET))
            fseeko64(fp, 0, SEEK_END);
        int write_count = fwrite(block, 1, bytes, fp);
#else
        if (lseek64(fd, file_pos, SEEK_SET) == -1)
            lseek64(fd, 0, SEEK_END);
        int write_count = write(fd, block, bytes);
#endif
        if (write_count != bytes) {
            printf("Short write: %d\n", write_count);
            throw EIO;
        }
    }
    void write_pages(set<int>& pages_to_write) {
        for (int it : pages_to_write) {
            byte *block = &page_cache[page_size * disk_to_cache_map[it]->cache_loc];
            block[0] &= 0xFD; // unchange it
            off64_t file_pos = page_size;
            file_pos *= it;
            write_page(block, file_pos, page_size);
            stats.pages_written++;
        }
    }
    void calc_flush_count() {
        last_pages_to_flush = 10;
        return;
        //file_page_count / cache_size_in_pages * 40;
        if (last_pages_to_flush < 10)
            last_pages_to_flush = 10;
        if (last_pages_to_flush > 50)
           last_pages_to_flush = 50;
    }
    void flush_pages_in_seq(byte *block_to_keep) {
        stats.cache_flush_count++;
        set<int> pages_to_write(new_pages);
        new_pages.clear();
        calc_flush_count();
        int pages_to_check = last_pages_to_flush * 3;
        dbl_lnklst *cur_entry = lnklst_last_entry;
        do {
            byte *block = &page_cache[cur_entry->cache_loc * page_size];
            if (block_to_keep != block) {
              if (block[0] & 0x02) // is it changed
                pages_to_write.insert(cur_entry->disk_page);
              if (pages_to_write.size() > (last_pages_to_flush * 2))
                break;
            }
            cur_entry = cur_entry->prev;
        } while (--pages_to_check && cur_entry);
        write_pages(pages_to_write);
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

public:
    lru_cache(int pg_size, int page_count, const char *fname, int init_page_count = 0, void *(*alloc_fn)(size_t) = NULL) {
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
        struct stat file_stat;
        memset(&file_stat, '\0', sizeof(file_stat));
#if USE_FOPEN == 1
        fp = fopen(fname, "r+b");
        if (fp == NULL) {
          fp = fopen(fname, "wb");
          if (fp == NULL)
            throw errno;
          fclose(fp);
          fp = fopen(fname, "r+b");
          if (fp == NULL)
            throw errno;
        }
        lstat(fname, &file_stat);
#else
        fd = open(fname, O_RDWR | O_CREAT | O_LARGEFILE, 0644);
        if (fd == -1)
          throw errno;
        fstat(fd, &file_stat);
#endif
        file_page_count = file_stat.st_size;
        if (file_page_count > 0)
           file_page_count /= page_size;
        cout << "File page count: " << file_page_count << endl;
        empty = 0;
#if USE_FOPEN == 1
        if (fread(root_block, 1, page_size, fp) != page_size) {
            file_page_count = 1;
            fseeko64(fp, 0, SEEK_SET);
            if (fwrite(root_block, 1, page_size, fp) != page_size)
              throw EIO;
            empty = 1;
        }
#else
        if (read(fd, root_block, page_size) != page_size) {
            file_page_count = 1;
            lseek64(fd, 0, SEEK_SET);
            if (write(fd, root_block, page_size) != page_size)
              throw EIO;
            empty = 1;
        }
#endif
        memset(&stats, '\0', sizeof(stats));
        calc_flush_count();
    }
    ~lru_cache() {
        set<int> pages_to_write;
        for (unordered_map<int, dbl_lnklst*>::iterator it = disk_to_cache_map.begin(); it != disk_to_cache_map.end(); it++) {
            byte *block = &page_cache[page_size * it->second->cache_loc];
            if (block[0] & 0x02) // is it changed
                pages_to_write.insert(it->first);
        }
        write_pages(pages_to_write);
        free(page_cache);
        write_page(root_block, 0, page_size);
#if USE_FOPEN == 1
        fclose(fp);
#else
        close(fd);
#endif
        free(root_block);
        free(llarr);
        cout << "total_cache_misses: " << " " << stats.total_cache_misses << endl;
        cout << "cache_flush_count: " << " " << stats.cache_flush_count << endl;
    }
    byte *get_disk_page_in_cache(int disk_page, byte *block_to_keep = NULL, bool is_new = false) {
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
            } else {
                dbl_lnklst *entry_to_move = lnklst_last_entry;
                    //byte *block;
                    //while (1) {
                    //  block = &page_cache[entry_to_move->cache_loc * page_size];
                    //  if (block != block_to_keep) {
                    //     break;
                    //  }
                    //  entry_to_move = entry_to_move->prev;
                    //}
                calc_flush_count();
                int check_count = last_pages_to_flush * 5;
                byte *block;
                while (check_count--) { // find block which is not changed
                    block = &page_cache[entry_to_move->cache_loc * page_size];
                    if ((block[0] & 0x02) == 0x00 && block_to_keep != block)
                      break;
                    entry_to_move = entry_to_move->prev;
                }
                if ((block[0] & 0x02) || new_pages.size() > last_pages_to_flush
                       || new_pages.find(disk_page) != new_pages.end())
                  flush_pages_in_seq(block_to_keep);
                if (block[0] & 0x02) {
                  check_count = last_pages_to_flush;
                  while (check_count--) { // find block which is not changed
                      block = &page_cache[entry_to_move->cache_loc * page_size];
                      if (block != block_to_keep) {
                        if ((block[0] & 0x02) == 0x00)
                          break;
                      }
                      entry_to_move = entry_to_move->prev;
                  }
                }

                    //if (block[0] & 0x02) {
                    //  block[0] &= 0xFD; // unchange it
                    //  write_page(block, entry_to_move->disk_page * page_size, page_size);
                    //  fflush(fp);
                    //}
                removed_disk_page = entry_to_move->disk_page;
                cache_pos = entry_to_move->cache_loc;
                if (!is_new)
                  move_to_front(entry_to_move);
                entry_to_move->disk_page = disk_page;
                disk_to_cache_map.erase(removed_disk_page);
                disk_to_cache_map[disk_page] = entry_to_move;
                stats.total_cache_misses++;
            }
            if (!is_new && new_pages.find(disk_page) == new_pages.end()) {
                off64_t file_pos = page_size;
                file_pos *= disk_page;
#if USE_FOPEN == 1
                if (!fseeko64(fp, file_pos, SEEK_SET)) {
                    int read_count = fread(&page_cache[page_size * cache_pos], 1, page_size, fp);
                    //printf("read_count: %d, %d, %d, %d, %ld\n", read_count, page_size, disk_page, (int) page_cache[page_size * cache_pos], ftell(fp));
                    if (read_count != page_size) {
                       perror("read");
                    }
                } else {
                  printf("disk_page: %d, %d\n", disk_page, errno);
                }
#else
                if (lseek64(fd, file_pos, SEEK_SET) != -1) {
                    int read_count = read(fd, &page_cache[page_size * cache_pos], page_size);
                    //printf("read_count: %d, %d, %d, %d, %ld\n", read_count, page_size, disk_page, (int) page_cache[page_size * cache_pos], ftell(fp));
                    if (read_count != page_size) {
                       perror("read");
                    }
                } else {
                  printf("disk_page: %d, %d\n", disk_page, errno);
                }
#endif
            }
        } else {
            dbl_lnklst *current_entry = disk_to_cache_map[disk_page];
            move_to_front(current_entry);
            cache_pos = current_entry->cache_loc;
        }
        return &page_cache[page_size * cache_pos];
    }
    byte *get_new_page(byte *block_to_keep) {
        if (new_pages.size() > last_pages_to_flush)
            flush_pages_in_seq(block_to_keep);
        byte *new_page = get_disk_page_in_cache(file_page_count, block_to_keep, true);
        new_pages.insert(file_page_count);
            //new_page[0] &= 0xFD; // unchange it
            //write_page(new_page, file_page_count * page_size, page_size, true);
            //fflush(fp);
            //new_page[0] |= 0x02; // change it
            //printf("file_page_count:%ld\n", file_page_count);
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
