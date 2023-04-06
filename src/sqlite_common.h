#ifndef SIARA_SQLITE3_COMMON
#define SIARA_SQLITE3_COMMON

#include <string>
#include <stdlib.h>
#include <stdint.h>

#include "util.h"

const int8_t col_data_lens[] = {0, 1, 2, 3, 4, 6, 8, 8, 0, 0};

enum {SQLT_TYPE_NULL = 0, SQLT_TYPE_INT8, SQLT_TYPE_INT16, SQLT_TYPE_INT24, SQLT_TYPE_INT32, SQLT_TYPE_INT48, SQLT_TYPE_INT64,
        SQLT_TYPE_REAL, SQLT_TYPE_INT0, SQLT_TYPE_INT1, SQLT_TYPE_BLOB = 12, SQLT_TYPE_TEXT = 13};

enum {SQLT_RES_OK = 0, SQLT_RES_ERR = -1, SQLT_RES_INV_PAGE_SZ = -2, 
  SQLT_RES_TOO_LONG = -3, SQLT_RES_WRITE_ERR = -4, SQLT_RES_FLUSH_ERR = -5};

enum {SQLT_RES_SEEK_ERR = -6, SQLT_RES_READ_ERR = -7,
  SQLT_RES_INVALID_SIG = -8, SQLT_RES_MALFORMED = -9,
  SQLT_RES_NOT_FOUND = -10, SQLT_RES_NOT_FINALIZED = -11,
  SQLT_RES_TYPE_MISMATCH = -12, SQLT_RES_INV_CHKSUM = -13,
  SQLT_RES_NEED_1_PK = -14, SQLT_RES_NO_SPACE = -15,
  SQLT_RES_CLOSED = -16};

class sqlite_common {

    public:
        // Writes given value at given pointer in Sqlite format
        static uint16_t write_data(uint8_t *data_ptr, int type, const void *val, uint16_t len) {
            if (val == NULL || type == SQLT_TYPE_NULL 
                    || type == SQLT_TYPE_INT0 || type == SQLT_TYPE_INT1)
                return 0;
            if (type >= SQLT_TYPE_INT8 && type <= SQLT_TYPE_INT64) {
                switch (type) {
                case SQLT_TYPE_INT8:
                    util::write_uint8(data_ptr, *((int8_t *) val));
                    break;
                case SQLT_TYPE_INT16:
                    util::write_uint16(data_ptr, *((int16_t *) val));
                    break;
                case SQLT_TYPE_INT24:
                    util::write_int24(data_ptr, *((int32_t *) val));
                    break;
                case SQLT_TYPE_INT32:
                    util::write_uint32(data_ptr, *((int32_t *) val));
                    break;
                case SQLT_TYPE_INT48:
                    util::write_int48(data_ptr, *((int64_t *) val));
                    break;
                case SQLT_TYPE_INT64:
                    util::write_uint64(data_ptr, *((int64_t *) val));
                    break;
                }
            } else
            if (type == SQLT_TYPE_REAL && len == 4) {
                // Assumes float is represented in IEEE-754 format
                uint64_t bytes64 = util::float_to_double(val);
                util::write_uint64(data_ptr, bytes64);
                len = 8;
            } else
            if (type == SQLT_TYPE_REAL && len == 8) {
                // TODO: Assumes double is represented in IEEE-754 format
                uint64_t bytes = *((uint64_t *) val);
                util::write_uint64(data_ptr, bytes);
            } else
                memcpy(data_ptr, val, len);
            return len;
        }

        uint8_t *locate_col(int which_col, uint8_t *rec, int& col_type_or_len, int& col_len, int& col_type) {
            int8_t vlen;
            int hdr_len = util::read_vint32(rec, &vlen);
            int hdr_pos = vlen;
            uint8_t *data_ptr = rec + hdr_len;
            col_len = vlen = 0;
            do {
                data_ptr += col_len;
                hdr_pos += vlen;
                if (hdr_pos >= hdr_len)
                    return NULL;
                col_type_or_len = util::read_vint32(rec + hdr_pos, &vlen);
                col_len = derive_data_len(col_type_or_len);
                col_type = derive_col_type(col_type_or_len);
            } while (which_col--);
            return data_ptr;
        }

        // See .h file for API description
        uint32_t derive_data_len(uint32_t col_type_or_len) {
            if (col_type_or_len >= 12) {
                if (col_type_or_len % 2)
                    return (col_type_or_len - 13)/2;
                return (col_type_or_len - 12)/2; 
            } else if (col_type_or_len < 10)
                return col_data_lens[col_type_or_len];
            return 0;
        }

        // See .h file for API description
        uint32_t derive_col_type(uint32_t col_type_or_len) {
            if (col_type_or_len >= 12) {
                if (col_type_or_len % 2)
                    return SQLT_TYPE_TEXT;
                return SQLT_TYPE_BLOB;
            } else if (col_type_or_len < 10)
                return col_type_or_len;
            return 0;
        }

        int read_col(int which_col, uint8_t *rec, int rec_len, void *out) {
            int col_type_or_len, col_len, col_type;
            uint8_t *data_ptr = locate_col(which_col, rec, col_type_or_len, col_len, col_type);
            if (data_ptr == NULL)
                return SQLT_RES_MALFORMED;
            switch (col_type) {
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    memcpy(out, data_ptr, col_len);
                    return col_len;
                case SQLT_TYPE_NULL:
                    return col_type_or_len;
                case SQLT_TYPE_INT0:
                    *((int8_t *) out) = 0;
                    return col_len;
                case SQLT_TYPE_INT1:
                    *((int8_t *) out) = 1;
                    return col_len;
                case SQLT_TYPE_INT8:
                    *((int8_t *) out) = *data_ptr;
                    return col_len;
                case SQLT_TYPE_INT16:
                    *((int16_t *) out) = util::read_uint16(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT24:
                    *((int32_t *) out) = util::read_int24(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT32:
                    *((int32_t *) out) = util::read_uint32(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT48:
                    *((int64_t *) out) = util::read_int48(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT64:
                    *((int64_t *) out) = util::read_uint64(data_ptr);
                    return col_len;
                case SQLT_TYPE_REAL:
                    *((double *) out) = util::read_double(data_ptr);
                    return col_len;
            }
            return SQLT_RES_MALFORMED;
        }

        // Initializes the buffer as a B-Tree Leaf Index
        static void init_bt_idx_interior(uint8_t *ptr, int block_size, int resv_bytes) {
            ptr[0] = 2; // Interior index b-tree page
            util::write_uint16(ptr + 1, 0); // No freeblocks
            util::write_uint16(ptr + 3, 0); // No records yet
            util::write_uint16(ptr + 5, (block_size - resv_bytes == 65536 ? 0 : block_size - resv_bytes)); // No records yet
            util::write_uint8(ptr + 7, 0); // Fragmented free bytes
            util::write_uint32(ptr + 8, 0); // right-most pointer
        }

        // Initializes the buffer as a B-Tree Leaf Index
        static void init_bt_idx_leaf(uint8_t *ptr, int block_size, int resv_bytes) {
            ptr[0] = 10; // Leaf index b-tree page
            util::write_uint16(ptr + 1, 0); // No freeblocks
            util::write_uint16(ptr + 3, 0); // No records yet
            util::write_uint16(ptr + 5, (block_size - resv_bytes == 65536 ? 0 : block_size - resv_bytes)); // No records yet
            util::write_uint8(ptr + 7, 0); // Fragmented free bytes
        }

        // Initializes the buffer as a B-Tree Leaf Table
        static void init_bt_tbl_leaf(uint8_t *ptr, int block_size, int resv_bytes) {
            ptr[0] = 13; // Leaf Table b-tree page
            util::write_uint16(ptr + 1, 0); // No freeblocks
            util::write_uint16(ptr + 3, 0); // No records yet
            util::write_uint16(ptr + 5, (block_size - resv_bytes == 65536 ? 0 : block_size - resv_bytes)); // No records yet
            util::write_uint8(ptr + 7, 0); // Fragmented free bytes
        }

        static int get_offset(uint8_t block[]) {
            return (block[0] == 2 || block[0] == 5 || block[0] == 10 || block[0] == 13 ? 0 : 100);
        }

        static int get_data_len(int i, const uint8_t types[], const void *values[]) {
            if (types == NULL || types[i] >= 10)
                return strlen((const char *) values[i]);
            return col_data_lens[types[i]];
        }

        static int write_new_rec(uint8_t block[], int pos, int64_t rowid_or_child_pageno, int col_count, const void *values[],
                const size_t value_lens[] = NULL, const uint8_t types[] = NULL, uint8_t *ptr = NULL) {

            int data_len = 0;
            for (int i = 0; i < col_count; i++)
                data_len += (value_lens == NULL ? get_data_len(i, types, values) : value_lens[i]);
            int hdr_len = 0;
            for (int i = 0; i < col_count; i++) {
                int val_len_hdr_len = (value_lens == NULL ? get_data_len(i, types, values) : value_lens[i]);
                if (types == NULL || types[i] == SQLT_TYPE_TEXT || types[i] == SQLT_TYPE_BLOB) {
                    val_len_hdr_len = val_len_hdr_len * 2 + (types == NULL ? 13 : types[i]);
                }
                hdr_len += util::get_vlen_of_uint16(val_len_hdr_len);
            }
            int offset = get_offset(block);
            int hdr_len_vlen = util::get_vlen_of_uint32(hdr_len);
            hdr_len += hdr_len_vlen;
            int rowid_len = 0;
            if (block[offset] == 5 || block[offset] == 13)
                rowid_len = util::get_vlen_of_uint64(rowid_or_child_pageno);
            else if (block[offset] == 2)
                rowid_len = 4;

            int rec_len = hdr_len + data_len;
            int rec_len_vlen = util::get_vlen_of_uint32(rec_len);

            int last_pos = 0;
            bool is_ptr_given = true;
            int blk_hdr_len = (block[offset] == 10 || block[offset] == 13 ? 8 : 12);
            if (ptr == NULL) {
                is_ptr_given = false;
                last_pos = util::read_uint16(block + offset + 5);
                if (last_pos == 0)
                    last_pos = 65536;
                int ptr_len = util::read_uint16(block + offset + 3) << 1;
                if (offset + blk_hdr_len + ptr_len + rowid_len + rec_len + rec_len_vlen + 1 >= last_pos)
                    return SQLT_RES_NO_SPACE;
                last_pos -= rec_len;
                last_pos -= rec_len_vlen;
                last_pos -= rowid_len;
                ptr = block + last_pos;
            }

            if (!is_ptr_given) {
                if (block[offset] == 2) {
                    util::write_uint32(ptr, rowid_or_child_pageno);
                    ptr += 4;
                }
                ptr += util::write_vint32(ptr, rec_len);
                if (block[offset] == 5 || block[offset] == 13)
                    ptr += util::write_vint64(ptr, rowid_or_child_pageno);
            }
            ptr += util::write_vint32(ptr, hdr_len);
            for (int i = 0; i < col_count; i++) {
                uint8_t type = (types == NULL ? SQLT_TYPE_TEXT : types[i]);
                int value_len = (value_lens == NULL ? get_data_len(i, types, values) : value_lens[i]);
                int col_len_in_hdr = (type == SQLT_TYPE_TEXT || type == SQLT_TYPE_BLOB)
                        ? value_len * 2 + type : type;
                ptr += util::write_vint32(ptr, col_len_in_hdr);
            }
            for (int i = 0; i < col_count; i++) {
                if (value_lens == NULL || value_lens[i] > 0) {
                    ptr += write_data(ptr, types == NULL ? SQLT_TYPE_TEXT : types[i],
                        values[i], value_lens == NULL ? get_data_len(i, types, values) : value_lens[i]);
                }
            }

            // if pos given, update record count, last data pos and insert record
            if (last_pos > 0 && pos >= 0) {
                int rec_count = util::read_uint16(block + offset + 3);
                util::write_uint16(block + offset + 3, rec_count + 1);
                util::write_uint16(block + offset + 5, last_pos);
                uint8_t *ins_ptr = block + offset + blk_hdr_len + pos * 2;
                memmove(ins_ptr + 2, ins_ptr, (rec_count - pos) * 2);
                util::write_uint16(ins_ptr, last_pos);
            }

            return is_ptr_given ? rec_len : last_pos;

        }

        // Writes data into buffer to form first page of Sqlite db
        static void fill_page0(uint8_t master_block[], int total_col_count, int pk_col_count, int block_size,
            int page_resv_bytes, const std::string& col_names, const std::string& table_name) {

            if (block_size % 512 || block_size < 512 || block_size > 65536)
                throw SQLT_RES_INV_PAGE_SZ;

            // 100 uint8_t header - refer https://www.sqlite.org/fileformat.html
            memcpy(master_block, "SQLite format 3\0", 16);
            util::write_uint16(master_block + 16, block_size == 65536 ? 1 : (uint16_t) block_size);
            master_block[18] = 1;
            master_block[19] = 1;
            master_block[20] = page_resv_bytes;
            master_block[21] = 64;
            master_block[22] = 32;
            master_block[23] = 32;
            //write_uint32(master_block + 24, 0);
            //write_uint32(master_block + 28, 0);
            //write_uint32(master_block + 32, 0);
            //write_uint32(master_block + 36, 0);
            //write_uint32(master_block + 40, 0);
            memset(master_block + 24, '\0', 20); // Set to zero, above 5
            util::write_uint32(master_block + 28, 2); // TODO: Update during finalize
            util::write_uint32(master_block + 44, 4);
            //write_uint16(master_block + 48, 0);
            //write_uint16(master_block + 52, 0);
            memset(master_block + 48, '\0', 8); // Set to zero, above 2
            util::write_uint32(master_block + 56, 1);
            // User version initially 0, set to table leaf count
            // used to locate last leaf page for binary search
            // and move to last page.
            util::write_uint32(master_block + 60, 0);
            util::write_uint32(master_block + 64, 0);
            // App ID - set to 0xA5xxxxxx where A5 is signature
            // till it is implemented
            util::write_uint32(master_block + 68, 0xA5000000);
            memset(master_block + 72, '\0', 20); // reserved space
            util::write_uint32(master_block + 92, 105);
            util::write_uint32(master_block + 96, 3016000);
            memset(master_block + 100, '\0', block_size - 100); // Set remaining page to zero

            // master table b-tree
            init_bt_tbl_leaf(master_block + 100, block_size, page_resv_bytes);

            // write table script record
            std::string tbl_name = "idx1";
            if (!table_name.empty())
                tbl_name = table_name;
            // write table script record
            int col_count = 5;
            // if (table_script) {
            //     uint16_t script_len = strlen(table_script);
            //     if (script_len > block_size - 100 - page_resv_bytes - 8 - 10)
            //         return SQLT_RES_TOO_LONG;
            //     set_col_val(4, SQLT_TYPE_TEXT, table_script, script_len);
            // } else {
                int table_name_len = tbl_name.length();
                // len("CREATE TABLE ") + table_name_len + len(" (")
                //    + len("PRIMARY KEY (") + len(") WITHOUT ROWID")
                size_t script_len = 13 + table_name_len + 2 + 13 + 15;
                script_len += col_names.length();
                script_len += 2; // len(", ")
                int pk_end_pos = 0;
                for (int i = 0, comma_count = 0; i < col_names.length(); i++) {
                    if (col_names[i] == ',')
                        comma_count++;
                    if (comma_count == pk_col_count) {
                        pk_end_pos = i;
                        break;
                    }
                }
                if (pk_end_pos == 0)
                    pk_end_pos = col_names.length();
                script_len += pk_end_pos;
                script_len++; // len(")")
                // 100 byte header, 2 byte ptr, 3 byte rec/hdr vlen, 1 byte rowid
                // 6 byte hdr len, 5 byte "table", twice table name, 4 byte uint32 root
                if (script_len > (block_size - 100 - page_resv_bytes - 8
                        - 2 - 3 - 1 - 6 - 5 - tbl_name.length() * 2 - 4))
                    throw SQLT_RES_TOO_LONG;
                uint8_t *script_loc = master_block + block_size - page_resv_bytes - script_len;
                uint8_t *script_pos = script_loc;
                memcpy(script_pos, "CREATE TABLE ", 13);
                script_pos += 13;
                memcpy(script_pos, tbl_name.c_str(), table_name_len);
                script_pos += table_name_len;
                *script_pos++ = ' ';
                *script_pos++ = '(';
                memcpy(script_pos, col_names.c_str(), col_names.length());
                script_pos += col_names.length();
                *script_pos++ = ',';
                *script_pos++ = ' ';
                memcpy(script_pos, "PRIMARY KEY (", 13);
                script_pos += 13;
                memcpy(script_pos, col_names.c_str(), pk_end_pos);
                script_pos += pk_end_pos;
                *script_pos++ = ')';
                memcpy(script_pos, ") WITHOUT ROWID", 15);
                script_pos += 15;
            // }
            int32_t root_page_no = 2;
            const void *master_rec_values[] = {"table", tbl_name.c_str(), tbl_name.c_str(), &root_page_no, script_loc};
            const size_t master_rec_col_lens[] = {5, table_name.length(), table_name.length(), sizeof(root_page_no), script_len};
            const uint8_t master_rec_col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT32, SQLT_TYPE_TEXT};
            int res = write_new_rec(master_block, 0, 1, 5, master_rec_values, master_rec_col_lens, master_rec_col_types);
            if (res < 0)
               throw res;

        }

};

#endif
