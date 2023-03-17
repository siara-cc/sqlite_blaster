#ifndef SIARA_SQLITE_DUMPIX
#define SIARA_SQLITE_DUMPIX

#define _FILE_OFFSET_BITS 64
#ifndef _LARGEFILE64_SOURCE
#define _LARGEFILE64_SOURCE
#endif

#include <string>
#include <vector>
#include <iostream>
#include "sqlite_common.h"

class sqlite_dumpix {

    private:
        int U, X, M;
        std::string filename;
        std::string column_names;
        std::string table_name;
        int column_count;
        int pk_col_count;
        int block_size;
        int pg_resv_bytes;
        uint8_t *master_block;
        uint8_t *current_block;
        FILE *fp;
        std::vector<int> non_leaf_page_nos;
        std::vector<uint8_t[]> non_leaf_pages;
        int read_page(uint8_t *block, off_t file_pos, size_t bytes) {
            if (!fseek(fp, file_pos, SEEK_SET)) {
                int read_count = fread(block, 1, bytes, fp);
                //printf("read_count: %d, %d, %d, %d, %ld\n", read_count, page_size, disk_page, (int) page_cache[page_size * cache_pos], ftell(fp));
                if (read_count != bytes) {
                    perror("read");
                }
                return read_count;
            } else {
                std::cout << "file_pos: " << file_pos << errno << std::endl;
            }
            return 0;
        }
        void write_page(uint8_t block[], off_t file_pos, size_t bytes) {
            if (fseek(fp, file_pos, SEEK_SET))
                fseek(fp, 0, SEEK_END);
            int write_count = fwrite(block, 1, bytes, fp);
            if (write_count != bytes) {
                printf("Short write: %d\n", write_count);
                throw EIO;
            }
        }
        int open_file() {
            fp = fopen(filename.c_str(), "w+b");
            if (fp == NULL)
                throw errno;
        }
        int close_file() {
            if (fp != NULL)
                fclose(fp);
            fp = NULL;
        }

    public:
        sqlite_dumpix(std::string fname, int page_size, int resv_bytes,
                        int total_col_count, int pk_col_count,
                        std::string col_names, std::string tbl_name)
                    : filename (fname), block_size (page_size), pg_resv_bytes (resv_bytes),
                        column_count (total_col_count), pk_col_count (pk_col_count),
                        column_names (col_names), table_name (tbl_name) {
            U = page_size - pg_resv_bytes;
            X = ((U-12)*64/255)-23;
            M = ((U-12)*32/255)-23;
            open_file();
            master_block = new uint8_t[block_size];
            sqlite_common::fill_page0(master_block, column_count, pk_col_count,
                    block_size, resv_bytes, column_names, table_name);
            write_page(master_block, 0, block_size);
            current_block = new uint8_t[block_size];
            sqlite_common::init_bt_idx_leaf(current_block, block_size, resv_bytes);
        }
 
        ~sqlite_dumpix() {
            close_file();
        }

        int make_new_rec(uint8_t *ptr, int col_count, const void *values[], 
                const size_t value_lens[] = NULL, const uint8_t types[] = NULL) {
            return sqlite_common::write_new_rec(current_block, -1, 0, col_count, values, value_lens, types, ptr);
        }

        bool append(std::string rec) {
            
        }
        ~sqlite_dumpix() {
        }

}

#endif
