#ifndef LRUCACHE_H
#define LRUCACHE_H
#include <set>
#include <iostream>
#define _FILE_OFFSET_BITS 64
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
#include <cstring>
#include <time.h>
#include <chrono>
#include <brotli/encode.h>
//#include <snappy.h>

#define USE_FOPEN 1

using namespace std;
using namespace chrono;

typedef struct dbl_lnklst_st {
    int disk_page;
    int cache_loc;
    struct dbl_lnklst_st *prev;
    struct dbl_lnklst_st *next;
} dbl_lnklst;

typedef struct {
    long total_cache_req;
    long total_cache_misses;
    long cache_flush_count;
    long pages_written;
    long pages_read;
    int last_pages_to_flush;
} cache_stats;

class lru_cache {
protected:
    int page_size;
    uint8_t *root_block;
    int cache_occupied_size;
    int skip_page_count;
    dbl_lnklst *lnklst_first_entry;
    dbl_lnklst *lnklst_last_entry;
    dbl_lnklst *lnklst_last_free;
    dbl_lnklst **disk_to_cache_map;
    size_t disk_to_cache_map_size;
    dbl_lnklst *llarr;
    set<int> new_pages;
    char filename[100];
#if USE_FOPEN == 1
    FILE *fp;
#else
    int fd;
#endif
    uint8_t empty;
    cache_stats stats;
    long max_pages_to_flush;
    void *(*malloc_fn)(size_t);
    bool (*const is_changed_fn)(uint8_t *, int);
    void (*const set_changed_fn)(uint8_t *, int, bool);
    void write_pages(set<int>& pages_to_write) {
        time_point<steady_clock> start;
        start = steady_clock::now();
        for (set<int>::iterator it = pages_to_write.begin(); it != pages_to_write.end(); it++) {
            uint8_t *block = &page_cache[page_size * disk_to_cache_map[*it]->cache_loc];
            set_changed_fn(block, page_size, false);
            //if (page_size < 65537 && block[5] < 255)
            //    block[5]++;
            off_t file_pos = page_size;
            file_pos *= *it;
            write_page(block, file_pos, page_size);
            stats.pages_written++;
        }
/*
if (page_size == 4096) {
        cout << "Block write: " << duration<double>(steady_clock::now()-start).count() << endl;
        char new_filename[strlen(filename) + 15];
        sprintf(new_filename, "%s.f%ld", filename, stats.cache_flush_count);
        start = steady_clock::now();
        uint8_t *append_buf = (uint8_t *) malloc(pages_to_write.size() * page_size);
        int block_loc = 0;
        string compressed_str;
        uint8_t c_out[page_size];
        size_t c_size;
        for (set<int>::iterator it = pages_to_write.begin(); it != pages_to_write.end(); it++) {
            uint8_t *block = &page_cache[page_size * disk_to_cache_map[*it]->cache_loc];
            int first_part_size = 6 + util::get_int(block + 1) * 2;
            util::set_int(append_buf + block_loc, first_part_size);
            memcpy(append_buf + block_loc + 2, block, first_part_size);
            int kv_last_pos = util::get_int(block + 3);
            int second_part_size = page_size - kv_last_pos;
            util::set_int(append_buf + block_loc + first_part_size + 2, second_part_size);
            memcpy(append_buf + block_loc + first_part_size + 4, block + kv_last_pos, second_part_size);
            //snappy::Compress((char *) append_buf + block_loc, first_part_size + second_part_size + 4, &compressed_str);
            //memcpy(append_buf + block_loc, compressed_str.c_str(), compressed_str.length());
            //block_loc += compressed_str.size();
            if (Brotli_encoder_compress(BROTLI_DEFAULT_QUALITY, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE, 
                first_part_size + second_part_size + 4, append_buf + block_loc, &c_size, c_out)) {
               memcpy(append_buf + block_loc, compressed_str.c_str(), compressed_str.length());
               block_loc += compressed_str.size();
            } else
                cout << "Compression failure" << endl;
            //block_loc += first_part_size + second_part_size + 4;
        }

 #if USE_FOPEN == 1
        FILE *file_appender = fopen(new_filename, "a");
 #else
        int file_appender = open(fname, O_APPEND | O_CREAT | O_LARGEFILE, 0644);
        if (file_appender == -1)
          throw errno;
 #endif
 #if USE_FOPEN == 1
            int write_count = fwrite(append_buf, 1, block_loc, file_appender);
 #else
            int write_count = write(file_appender, pages_to_write.size() * page_size, bytes);
 #endif
            if (write_count != block_loc) {
                printf("Short write a: %d\n", write_count);
                throw EIO;
            }
        fclose(file_appender);
        free(append_buf);
        cout << "Append write: " << duration<double>(steady_clock::now()-start).count() << ", orig: " << pages_to_write.size() * page_size << ", cmprsd: " << block_loc << endl;
}
*/
    }
    void calc_flush_count() {
        if (stats.total_cache_req == 0) {
          stats.last_pages_to_flush = max_pages_to_flush < 20 ? max_pages_to_flush : 20;
          return;
        }
        stats.last_pages_to_flush = max_pages_to_flush; //cache_size_in_pages * stats.total_cache_misses / stats.total_cache_req;
        if (stats.last_pages_to_flush > 500)
           stats.last_pages_to_flush = 500;
        if (stats.last_pages_to_flush < 2)
           stats.last_pages_to_flush = 2;
    }
    void flush_pages_in_seq(uint8_t *block_to_keep) {
        if (lnklst_last_entry == NULL)
            return;
        stats.cache_flush_count++;
        set<int> pages_to_write(new_pages);
        calc_flush_count();
        int pages_to_check = stats.last_pages_to_flush * 3;
        dbl_lnklst *cur_entry = lnklst_last_entry;
        do {
            uint8_t *block = &page_cache[cur_entry->cache_loc * page_size];
            if (block_to_keep != block) {
              if (is_changed_fn(block, page_size)) {
                pages_to_write.insert(cur_entry->disk_page);
                if (cur_entry->disk_page == 0 || !disk_to_cache_map[cur_entry->disk_page])
                    cout << "Disk cache map entry missing" << endl;
              }
              if (pages_to_write.size() > (stats.last_pages_to_flush + new_pages.size()))
                break;
            }
            cur_entry = cur_entry->prev;
        } while (--pages_to_check && cur_entry);
        write_pages(pages_to_write);
        new_pages.clear();
        lnklst_last_free = lnklst_last_entry;
    }
    void move_to_front(dbl_lnklst *entry_to_move) {
        //if (entry_to_move == lnklst_last_free)
        //  lnklst_last_free = lnklst_last_free->prev;
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
    void check_map_size() {
        if (disk_to_cache_map_size <= file_page_count) {
            //cout << "Expanding cache at: " << file_page_count << endl;
            dbl_lnklst **temp = disk_to_cache_map;
            size_t old_size = disk_to_cache_map_size;
            disk_to_cache_map_size = file_page_count + 1000;
            disk_to_cache_map = (dbl_lnklst **) malloc_fn(disk_to_cache_map_size * sizeof(dbl_lnklst *));
            memset(disk_to_cache_map, '\0', disk_to_cache_map_size * sizeof(dbl_lnklst *));
            memcpy(disk_to_cache_map, temp, old_size * sizeof(dbl_lnklst*));
            free(temp);
        }
    }

public:
    size_t file_page_count;
    int cache_size_in_pages;
    uint8_t *page_cache;
    lru_cache(int pg_size, int cache_size_kb, const char *fname,
            bool (*is_changed)(uint8_t *, int), void (*set_changed)(uint8_t *, int, bool),
            int init_page_count = 0, void *(*alloc_fn)(size_t) = NULL)
                : is_changed_fn (is_changed), set_changed_fn (set_changed) {
        if (alloc_fn == NULL)
            alloc_fn = malloc;
        malloc_fn = alloc_fn;
        page_size = pg_size;
        cache_size_in_pages = cache_size_kb * 1024 / page_size;
        cache_occupied_size = 0;
        lnklst_first_entry = lnklst_last_entry = NULL;
        strcpy(filename, fname);
        page_cache = (uint8_t *) alloc_fn(pg_size * cache_size_in_pages);
        root_block = (uint8_t *) alloc_fn(pg_size);
        llarr = (dbl_lnklst *) alloc_fn(cache_size_in_pages * sizeof(dbl_lnklst));
        memset(llarr, '\0', cache_size_in_pages * sizeof(dbl_lnklst));
        skip_page_count = init_page_count;
        file_page_count = init_page_count;
        max_pages_to_flush = cache_size_in_pages / 2;
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
        //cout << "File page count: " << file_page_count << endl;
        disk_to_cache_map_size = max(file_page_count + 1000, (size_t) cache_size_in_pages);
        disk_to_cache_map = (dbl_lnklst **) alloc_fn(disk_to_cache_map_size * sizeof(dbl_lnklst *));
        memset(disk_to_cache_map, '\0', disk_to_cache_map_size * sizeof(dbl_lnklst *));
        empty = 0;
#if USE_FOPEN == 1
        fseek(fp, skip_page_count * page_size, SEEK_SET);
        if (fread(root_block, 1, page_size, fp) != page_size) {
            // Just fill arbitrary data before root
            for (int i = file_page_count; i < skip_page_count; i++)
                write_page(root_block, i * page_size, page_size);
            file_page_count = skip_page_count + 1;
            // write root block
            write_page(root_block, skip_page_count * page_size, page_size);
            empty = 1;
        }
#else
        lseek(fd, skip_page_count * page_size, SEEK_SET);
        if (read(fd, root_block, page_size) != page_size) {
            file_page_count = skip_page_count + 1;
            if (write(fd, root_block, page_size) != page_size)
              throw EIO;
            empty = 1;
        }
#endif
        stats.pages_read++;
        lnklst_last_free = NULL;
        memset(&stats, '\0', sizeof(stats));
        calc_flush_count();
    }
    ~lru_cache() {
        flush_pages_in_seq(0);
        set<int> pages_to_write;
        for (size_t ll = 0; ll < cache_size_in_pages; ll++) {
            if (llarr[ll].disk_page == 0)
              continue;
            uint8_t *block = &page_cache[page_size * llarr[ll].cache_loc];
            if (is_changed_fn(block, page_size)) // is it changed
                pages_to_write.insert(llarr[ll].disk_page);
        }
        write_pages(pages_to_write);
        free(page_cache);
        write_page(root_block, skip_page_count * page_size, page_size);
#if USE_FOPEN == 1
        fclose(fp);
#else
        close(fd);
#endif
        free(root_block);
        free(llarr);
        free(disk_to_cache_map);
        // cout << "cache requests: " << " " << stats.total_cache_req 
        //      << ", Misses: " << stats.total_cache_misses
        //      << ", Flush#: " << stats.cache_flush_count << endl;
    }
    int read_page(uint8_t *block, off_t file_pos, size_t bytes) {
#if USE_FOPEN == 1
        if (!fseek(fp, file_pos, SEEK_SET)) {
            int read_count = fread(block, 1, bytes, fp);
            //printf("read_count: %d, %d, %d, %d, %ld\n", read_count, page_size, disk_page, (int) page_cache[page_size * cache_pos], ftell(fp));
            if (read_count != bytes) {
                perror("read");
            }
            return read_count;
        } else {
            cout << "file_pos: " << file_pos << errno << endl;
        }
#else
        if (lseek(fd, file_pos, SEEK_SET) != -1) {
            int read_count = read(fd, block, bytes);
            //printf("read_count: %d, %d, %d, %d, %ld\n", read_count, page_size, disk_page, (int) page_cache[page_size * cache_pos], ftell(fp));
            if (read_count != bytes) {
                perror("read");
            }
            return read_count;
        } else {
            cout << "file_pos: " << file_pos << errno << endl;
        }
#endif
        return 0;
    }
    void write_page(uint8_t *block, off_t file_pos, size_t bytes, bool is_new = true) {
        //if (is_new)
        //  fseek(fp, 0, SEEK_END);
        //else
#if USE_FOPEN == 1
        if (fseek(fp, file_pos, SEEK_SET))
            fseek(fp, 0, SEEK_END);
        int write_count = fwrite(block, 1, bytes, fp);
#else
        if (lseek(fd, file_pos, SEEK_SET) == -1)
            lseek(fd, 0, SEEK_END);
        int write_count = write(fd, block, bytes);
#endif
        if (write_count != bytes) {
            printf("Short write: %d\n", write_count);
            throw EIO;
        }
    }
    uint8_t *get_disk_page_in_cache(int disk_page, uint8_t *block_to_keep = NULL, bool is_new = false) {
        if (disk_page < skip_page_count)
            cout << "WARNING: asking disk_page: " << disk_page << endl;
        if (disk_page == skip_page_count)
            return root_block;
        int cache_pos = 0;
        int removed_disk_page = 0;
        if (disk_to_cache_map[disk_page] == NULL) {
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
                    //uint8_t *block;
                    //while (1) {
                    //  block = &page_cache[entry_to_move->cache_loc * page_size];
                    //  if (block != block_to_keep) {
                    //     break;
                    //  }
                    //  entry_to_move = entry_to_move->prev;
                    //}
                calc_flush_count();
                uint8_t *block;
                dbl_lnklst *entry_to_move;
                do {
                  entry_to_move = lnklst_last_free;
                  if (entry_to_move == NULL)
                    entry_to_move = lnklst_last_entry;
                  int check_count = 40; // last_pages_to_flush * 2;
                  while (check_count--) { // find block which is not changed
                    block = &page_cache[entry_to_move->cache_loc * page_size];
                    if (!is_changed_fn(block, page_size) && block_to_keep != block)
                      break;
                    if (entry_to_move->prev == NULL) {  // TODO: Review lru logic
                      lnklst_last_free = NULL;
                      break;
                    }
                    entry_to_move = entry_to_move->prev;
                  }
                  if (is_changed_fn(block, page_size) || new_pages.size() > stats.last_pages_to_flush
                             || new_pages.find(disk_page) != new_pages.end() || entry_to_move->prev == NULL) {
                      flush_pages_in_seq(block_to_keep);
                      entry_to_move = lnklst_last_entry;
                      break;
                  }
                } while (is_changed_fn(block, page_size));
                if (!is_changed_fn(block, page_size))
                    lnklst_last_free = entry_to_move->prev;
                /*entry_to_move = lnklst_last_entry;
                  int check_count = 40;
                  while (check_count-- && entry_to_move != NULL) { // find block which is not changed
                    block = &page_cache[entry_to_move->cache_loc * page_size];
                    if (block_to_keep != block) {
                      if (is_changed_fn(block, page_size)) {
                        set_changed_fn(block, page_size, false);
                        //if (page_size < 65537 && block[5] < 255)
                        //    block[5]++;
                        write_page(block, entry_to_move->disk_page * page_size, page_size);
                        //fflush(fp);
                      }
                      if (new_pages.size() > 0) {
                        write_pages(new_pages);
                        new_pages.clear();
                      }
                      break;
                    }
                    entry_to_move = entry_to_move->prev;
                  }
                  if (entry_to_move == NULL) {
                    cout << "Could not satisfy cache miss" << endl;
                    exit(1);
                  }*/
                removed_disk_page = entry_to_move->disk_page;
                cache_pos = entry_to_move->cache_loc;
                //if (!is_new)
                  move_to_front(entry_to_move);
                entry_to_move->disk_page = disk_page;
                disk_to_cache_map[removed_disk_page] = NULL;
                disk_to_cache_map[disk_page] = entry_to_move;
                stats.total_cache_misses++;
                stats.total_cache_req++;
            }
            if (!is_new && new_pages.find(disk_page) == new_pages.end()) {
                off_t file_pos = page_size;
                file_pos *= disk_page;
                if (read_page(&page_cache[page_size * cache_pos], file_pos, page_size) != page_size)
                    cout << "Unable to read: " << disk_page << endl;
                stats.pages_read++;
            }
        } else {
            if (is_new)
                cout << "WARNING: How was new page found in cache?" << endl;
            dbl_lnklst *current_entry = disk_to_cache_map[disk_page];
            if (lnklst_last_free == current_entry && lnklst_last_free != NULL
                    && lnklst_last_free->prev != NULL)
                lnklst_last_free = lnklst_last_free->prev;
            move_to_front(current_entry);
            cache_pos = current_entry->cache_loc;
            if (cache_occupied_size >= cache_size_in_pages)
              stats.total_cache_req++;
        }
        return &page_cache[page_size * cache_pos];
    }
    uint8_t *get_new_page(uint8_t *block_to_keep) {
        if (new_pages.size() > stats.last_pages_to_flush)
            flush_pages_in_seq(block_to_keep);
        check_map_size();
        uint8_t *new_page = get_disk_page_in_cache(file_page_count, block_to_keep, true);
        new_pages.insert(file_page_count);
            //set_changed_fn(new_page, page_size, false);
            //write_page(new_page, file_page_count * page_size, page_size, true);
            //fflush(fp);
            //set_changed(new_page, page_size, true); // change it
            //printf("file_page_count:%ld\n", file_page_count);
        file_page_count++;
        return new_page;
    }
    int get_page_count() {
        return file_page_count;
    }
    uint8_t is_empty() {
        return empty;
    }
    cache_stats get_cache_stats() {
        return stats;
    }
};
#endif
