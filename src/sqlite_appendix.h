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

class sqlite_appendix : public sqlite_common {

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
        FILE *fp;
        std::vector<uint8_t*> cur_pages;
        uint32_t last_page_no;
        bool is_closed;
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
        void open_file() {
            fp = fopen(filename.c_str(), "w+b");
            if (fp == NULL)
                throw errno;
        }
        void close_file() {
            if (fp != NULL)
                fclose(fp);
            fp = NULL;
        }

        void write_completed_page(int page_idx, const void *values[], const size_t value_lens[] = NULL, const uint8_t types[] = NULL) {
            uint8_t *target_block = cur_pages[page_idx];
            uint32_t completed_page = ++last_page_no;
            write_page(cur_pages[page_idx], (completed_page - 1) * block_size, block_size);
            uint8_t *new_block = new uint8_t[block_size];
            cur_pages[page_idx] = new_block;
            if (target_block[0] == 2)
                init_bt_idx_interior(new_block, block_size, pg_resv_bytes);
            else
                init_bt_idx_leaf(new_block, block_size, pg_resv_bytes);
            page_idx++;
            if (page_idx < cur_pages.size()) {
                target_block = cur_pages[page_idx];
                int new_rec_no = util::read_uint16(target_block + 3);
                int res = write_new_rec(target_block, new_rec_no, 0, column_count, values, value_lens, types);
                if (res == SQLT_RES_NO_SPACE) {
                    util::write_uint32(target_block + 8, completed_page);
                    write_completed_page(page_idx, values, value_lens, types);
                } else
                    util::write_uint32(target_block + util::read_uint16(target_block + 5), completed_page);
            } else {
                uint8_t *new_root_block = new uint8_t[block_size];
                init_bt_idx_interior(new_root_block, block_size, pg_resv_bytes);
                write_new_rec(new_root_block, 0, 0, column_count, values, value_lens, types);
                util::write_uint32(new_root_block + util::read_uint16(new_root_block + 5), completed_page);
                cur_pages.push_back(new_root_block);
            }
        }

        void flush_last_blocks() {
            uint32_t rightmost_page_no = 0;
            for (int i = 0; i < cur_pages.size(); i++) {
                uint8_t *last_block = cur_pages[i];
                if (rightmost_page_no > 0)
                    util::write_uint32(last_block + 8, rightmost_page_no);
                rightmost_page_no = last_page_no++;
                write_page(last_block, rightmost_page_no * block_size, block_size);
                rightmost_page_no++;
            }
            int type_or_len, col_len, col_type;
            uint8_t *rec_ptr = master_block + util::read_uint16(master_block + 105);
            int8_t vlen;
            util::read_vint64(rec_ptr, &vlen);
            rec_ptr += vlen;
            util::read_vint32(rec_ptr, &vlen);
            rec_ptr += vlen;
            uint8_t *data_ptr = locate_col(3, rec_ptr, type_or_len, col_len, col_type);
            util::write_uint32(data_ptr, last_page_no);
            util::write_uint32(master_block + 28, last_page_no);
            write_page(master_block, 0, block_size);
        }

    public:
        sqlite_appendix(std::string fname, int page_size, int resv_bytes,
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
            fill_page0(master_block, column_count, pk_col_count,
                    block_size, resv_bytes, column_names, table_name);
            write_page(master_block, 0, block_size);
            last_page_no = 1;
            uint8_t *current_block = new uint8_t[block_size];
            cur_pages.push_back(current_block);
            sqlite_common::init_bt_idx_leaf(current_block, block_size, resv_bytes);
            is_closed = false;
        }
 
        int append_rec(const void *values[], const size_t value_lens[] = NULL, const uint8_t types[] = NULL) {
            if (is_closed)
                throw SQLT_RES_CLOSED;
            int res = SQLT_RES_NO_SPACE;
            uint8_t *target_block = cur_pages[0];
            uint32_t page_idx = 0;
            int new_rec_no = util::read_uint16(target_block + 3);
            res = sqlite_common::write_new_rec(target_block, new_rec_no, 0, column_count, values, value_lens, types);
            if (res == SQLT_RES_NO_SPACE)
                write_completed_page(page_idx, values, value_lens, types);
            return SQLT_RES_OK;
        }

        void close() {
            if (is_closed)
                return;
            flush_last_blocks();
            close_file();
            is_closed = true;
        }

        ~sqlite_appendix() {
            if (!is_closed)
                close();
        }

};

#endif
