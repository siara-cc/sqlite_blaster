#ifndef SQLITE_H
#define SQLITE_H
#ifndef ARDUINO
#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>
#endif
#include "btree_handler.h"

// TODO: decide whether needed
#define page_resv_bytes 5

const int8_t col_data_lens[] = {0, 1, 2, 3, 4, 6, 8, 8, 0, 0};

enum {SQLT_TYPE_NULL = 0, SQLT_TYPE_INT8, SQLT_TYPE_INT16, SQLT_TYPE_INT24, SQLT_TYPE_INT32, SQLT_TYPE_INT48, SQLT_TYPE_INT64,
        SQLT_TYPE_REAL, SQLT_TYPE_INT0, SQLT_TYPE_INT1, SQLT_TYPE_BLOB = 12, SQLT_TYPE_TEXT = 13};

enum {SQLT_RES_OK = 0, SQLT_RES_ERR = -1, SQLT_RES_INV_PAGE_SZ = -2, 
  SQLT_RES_TOO_LONG = -3, SQLT_RES_WRITE_ERR = -4, SQLT_RES_FLUSH_ERR = -5};

enum {SQLT_RES_SEEK_ERR = -6, SQLT_RES_READ_ERR = -7,
  SQLT_RES_INVALID_SIG = -8, SQLT_RES_MALFORMED = -9,
  SQLT_RES_NOT_FOUND = -10, SQLT_RES_NOT_FINALIZED = -11,
  SQLT_RES_TYPE_MISMATCH = -12, SQLT_RES_INV_CHKSUM = -13,
  SQLT_RES_NEED_1_PK = -14, SQLT_RES_NO_SPACE = -15};

// CRTP see https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
class sqlite_index_blaster : public btree_handler<sqlite_index_blaster> {

    private:
        int U,X,M;
        // Returns how many bytes the given integer will
        // occupy if stored as a variable integer
        int8_t get_vlen_of_uint16(uint16_t vint) {
            return vint > 16383 ? 3 : (vint > 127 ? 2 : 1);
        }

        // Returns how many bytes the given integer will
        // occupy if stored as a variable integer
        int8_t get_vlen_of_uint32(uint32_t vint) {
            return vint > ((1 << 28) - 1) ? 5
                : (vint > ((1 << 21) - 1) ? 4 
                : (vint > ((1 << 14) - 1) ? 3
                : (vint > ((1 << 7) - 1) ? 2 : 1)));
        }

        // Returns how many bytes the given integer will
        // occupy if stored as a variable integer
        int8_t get_vlen_of_uint64(uint64_t vint) {
            return vint > ((1ULL << 56) - 1) ? 9
                : (vint > ((1ULL << 49) - 1) ? 8
                : (vint > ((1ULL << 42) - 1) ? 7
                : (vint > ((1ULL << 35) - 1) ? 6
                : (vint > ((1ULL << 28) - 1) ? 5
                : (vint > ((1ULL << 21) - 1) ? 4 
                : (vint > ((1ULL << 14) - 1) ? 3
                : (vint > ((1ULL <<  7) - 1) ? 2 : 1)))))));
        }

        // Stores the given uint8_t in the given location
        // in big-endian sequence
        void write_uint8(uint8_t *ptr, uint8_t input) {
            ptr[0] = input;
        }

        // Stores the given uint16_t in the given location
        // in big-endian sequence
        void write_uint16(uint8_t *ptr, uint16_t input) {
            ptr[0] = input >> 8;
            ptr[1] = input & 0xFF;
        }

        // Stores the given int24_t in the given location
        // in big-endian sequence
        void write_int24(uint8_t *ptr, uint32_t input) {
            int i = 3;
            ptr[1] = ptr[2] = 0;
            *ptr = (input >> 24) & 0x80;
            while (i--)
                *ptr++ |= ((input >> (8 * i)) & 0xFF);
        }

        // Stores the given uint32_t in the given location
        // in big-endian sequence
        void write_uint32(uint8_t *ptr, uint32_t input) {
            int i = 4;
            while (i--)
                *ptr++ = (input >> (8 * i)) & 0xFF;
        }

        // Stores the given int64_t in the given location
        // in big-endian sequence
        void write_int48(uint8_t *ptr, uint64_t input) {
            int i = 7;
            memset(ptr + 1, '\0', 7);
            *ptr = (input >> 56) & 0x80;
            while (i--)
                *ptr++ |= ((input >> (8 * i)) & 0xFF);
        }

        // Stores the given uint64_t in the given location
        // in big-endian sequence
        void write_uint64(uint8_t *ptr, uint64_t input) {
            int i = 8;
            while (i--)
                *ptr++ = (input >> (8 * i)) & 0xFF;
        }

        // Stores the given uint16_t in the given location
        // in variable integer format
        int write_vint16(uint8_t *ptr, uint16_t vint) {
            int len = get_vlen_of_uint16(vint);
            for (int i = len - 1; i > 0; i--)
                *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
            *ptr = vint & 0x7F;
            return len;
        }

        // Stores the given uint32_t in the given location
        // in variable integer format
        int write_vint32(uint8_t *ptr, uint32_t vint) {
            int len = get_vlen_of_uint32(vint);
            for (int i = len - 1; i > 0; i--)
                *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
            *ptr = vint & 0x7F;
            return len;
        }

        // Stores the given uint64_t in the given location
        // in variable integer format
        int write_vint64(uint8_t *ptr, uint64_t vint) {
            int len = get_vlen_of_uint64(vint);
            for (int i = len - 1; i > 0; i--) {
                if (i == 8)
                    *ptr++ = vint >> 56;
                else
                    *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
            }
            *ptr = vint & 0x7F;
            return len;
        }

        // Reads and returns big-endian uint8_t
        // at a given memory location
        uint8_t read_uint8(const uint8_t *ptr) {
            return *ptr;
        }

        // Returns type of column based on given value and length
        // See https://www.sqlite.org/fileformat.html#record_format
        uint32_t derive_col_type_or_len(int type, const void *val, int len) {
            uint32_t col_type_or_len = type;
            if (type > 11)
                col_type_or_len = len * 2 + type;
            return col_type_or_len;    
        }

        // Writes Record length, Row ID and Header length
        // at given location
        // No corruption checking because no unreliable source
        void write_rec_len_rowid_hdr_len(uint8_t *ptr, uint16_t rec_len,
                uint32_t rowid, uint16_t hdr_len) {
            // write record len
            ptr += write_vint32(ptr, rec_len);
            // write row id
            ptr += write_vint32(ptr, rowid);
            // write header len
            *ptr++ = 0x80 + (hdr_len >> 7);
            *ptr = hdr_len & 0x7F;
        }

        // Writes given value at given pointer in Sqlite format
        uint16_t write_data(uint8_t *data_ptr, int type, const void *val, uint16_t len) {
            if (val == NULL || type == SQLT_TYPE_NULL 
                    || type == SQLT_TYPE_INT0 || type == SQLT_TYPE_INT1)
                return 0;
            if (type >= SQLT_TYPE_INT8 && type <= SQLT_TYPE_INT64) {
                switch (type) {
                case SQLT_TYPE_INT8:
                    write_uint8(data_ptr, *((int8_t *) val));
                    break;
                case SQLT_TYPE_INT16:
                    write_uint16(data_ptr, *((int16_t *) val));
                    break;
                case SQLT_TYPE_INT24:
                    write_int24(data_ptr, *((int32_t *) val));
                    break;
                case SQLT_TYPE_INT32:
                    write_uint32(data_ptr, *((int32_t *) val));
                    break;
                case SQLT_TYPE_INT48:
                    write_int48(data_ptr, *((int64_t *) val));
                    break;
                case SQLT_TYPE_INT64:
                    write_uint64(data_ptr, *((int64_t *) val));
                    break;
                }
            } else
            if (type == SQLT_TYPE_REAL && len == 4) {
                // Assumes float is represented in IEEE-754 format
                uint64_t bytes64 = float_to_double(val);
                write_uint64(data_ptr, bytes64);
                len = 8;
            } else
            if (type == SQLT_TYPE_REAL && len == 8) {
                // TODO: Assumes double is represented in IEEE-754 format
                uint64_t bytes = *((uint64_t *) val);
                write_uint64(data_ptr, bytes);
            } else
                memcpy(data_ptr, val, len);
            return len;
        }

        // Initializes the buffer as a B-Tree Leaf Index
        void init_bt_idx_interior(uint8_t *ptr) {
            ptr[0] = 2; // Interior index b-tree page
            write_uint16(ptr + 1, 0); // No freeblocks
            write_uint16(ptr + 3, 0); // No records yet
            write_uint16(ptr + 5, (block_size - page_resv_bytes == 65536 ? 0 : block_size - page_resv_bytes)); // No records yet
            write_uint8(ptr + 7, 0); // Fragmented free bytes
            write_uint32(ptr + 8, 0); // right-most pointer
        }

        // Initializes the buffer as a B-Tree Leaf Index
        void init_bt_idx_leaf(uint8_t *ptr) {
            ptr[0] = 10; // Leaf index b-tree page
            write_uint16(ptr + 1, 0); // No freeblocks
            write_uint16(ptr + 3, 0); // No records yet
            write_uint16(ptr + 5, (block_size - page_resv_bytes == 65536 ? 0 : block_size - page_resv_bytes)); // No records yet
            write_uint8(ptr + 7, 0); // Fragmented free bytes
        }

        // Initializes the buffer as a B-Tree Leaf Table
        void init_bt_tbl_leaf(uint8_t *ptr) {
            ptr[0] = 13; // Leaf Table b-tree page
            write_uint16(ptr + 1, 0); // No freeblocks
            write_uint16(ptr + 3, 0); // No records yet
            write_uint16(ptr + 5, (block_size - page_resv_bytes == 65536 ? 0 : block_size - page_resv_bytes)); // No records yet
            write_uint8(ptr + 7, 0); // Fragmented free bytes
        }

        int get_offset() {
            return (current_block[0] == 2 || current_block[0] == 5 || current_block[0] == 10 || current_block[0] == 13 ? 0 : 100);
        }

        int get_data_len(int i, const uint8_t types[], const void *values[]) {
            if (types == NULL || types[i] >= 10)
                return strlen((const char *) values[i]);
            return col_data_lens[types[i]];
        }

        int write_new_rec(int pos, int64_t rowid, int col_count, const void *values[],
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
                hdr_len += get_vlen_of_uint16(val_len_hdr_len);
            }
            int offset = get_offset();
            int hdr_len_vlen = get_vlen_of_uint32(hdr_len);
            hdr_len += hdr_len_vlen;
            int rowid_len = 0;
            if (current_block[offset] == 10 || current_block[offset] == 13)
                rowid_len = get_vlen_of_uint64(rowid);
            int rec_len = hdr_len + data_len;
            int rec_len_vlen = get_vlen_of_uint32(rec_len);

            int last_pos = 0;
            bool is_ptr_given = true;
            if (ptr == NULL) {
                is_ptr_given = false;
                last_pos = read_uint16(current_block + offset + 5);
                if (last_pos == 0)
                    last_pos = 65536;
                int ptr_len = read_uint16(current_block + offset + 3) << 1;
                if (offset + blk_hdr_len + ptr_len + rec_len + rec_len_vlen >= last_pos)
                    return SQLT_RES_NO_SPACE;
                last_pos -= rec_len;
                last_pos -= rec_len_vlen;
                last_pos -= rowid_len;
                ptr = current_block + last_pos;
            }

            if (!is_ptr_given) {
                ptr += write_vint32(ptr, rec_len);
                if (current_block[offset] == 10 || current_block[offset] == 13)
                    ptr += write_vint64(ptr, rowid);
            }
            ptr += write_vint32(ptr, hdr_len);
            for (int i = 0; i < col_count; i++) {
                uint8_t type = (types == NULL ? SQLT_TYPE_TEXT : types[i]);
                int value_len = (value_lens == NULL ? get_data_len(i, types, values) : value_lens[i]);
                int col_len_in_hdr = (type == SQLT_TYPE_TEXT || type == SQLT_TYPE_BLOB)
                        ? value_len * 2 + type : type;
                ptr += write_vint32(ptr, col_len_in_hdr);
            }
            for (int i = 0; i < col_count; i++) {
                if (value_lens == NULL || value_lens[i] > 0) {
                    ptr += write_data(ptr, types == NULL ? SQLT_TYPE_TEXT : types[i],
                        values[i], value_lens == NULL ? get_data_len(i, types, values) : value_lens[i]);
                }
            }

            // if pos given, update record count, last data pos and insert record
            if (last_pos > 0 && pos >= 0) {
                int rec_count = read_uint16(current_block + offset + 3);
                write_uint16(current_block + offset + 3, rec_count + 1);
                write_uint16(current_block + offset + 5, last_pos);
                uint8_t *ins_ptr = current_block + offset + blk_hdr_len + pos * 2;
                memmove(ins_ptr + 2, ins_ptr, (rec_count - pos) * 2);
                write_uint16(ins_ptr, last_pos);
            }

            return is_ptr_given ? rec_len : last_pos;

        }

        // Writes data into buffer to form first page of Sqlite db
        int write_page0(int total_col_count, int pk_col_count,
            std::vector<std::string> col_names, const char *table_name = NULL) {

            if (block_size % 512 || block_size < 512 || block_size > 65536)
                throw SQLT_RES_INV_PAGE_SZ;

            master_block = (uint8_t *) malloc(block_size);
            current_block = master_block;

            // 100 uint8_t header - refer https://www.sqlite.org/fileformat.html
            memcpy(current_block, "SQLite format 3\0", 16);
            write_uint16(current_block + 16, block_size == 65536 ? 1 : (uint16_t) block_size);
            current_block[18] = 1;
            current_block[19] = 1;
            current_block[20] = page_resv_bytes;
            current_block[21] = 64;
            current_block[22] = 32;
            current_block[23] = 32;
            //write_uint32(current_block + 24, 0);
            //write_uint32(current_block + 28, 0);
            //write_uint32(current_block + 32, 0);
            //write_uint32(current_block + 36, 0);
            //write_uint32(current_block + 40, 0);
            memset(current_block + 24, '\0', 20); // Set to zero, above 5
            write_uint32(current_block + 28, 2); // TODO: Update during finalize
            write_uint32(current_block + 44, 4);
            //write_uint16(current_block + 48, 0);
            //write_uint16(current_block + 52, 0);
            memset(current_block + 48, '\0', 8); // Set to zero, above 2
            write_uint32(current_block + 56, 1);
            // User version initially 0, set to table leaf count
            // used to locate last leaf page for binary search
            // and move to last page.
            write_uint32(current_block + 60, 0);
            write_uint32(current_block + 64, 0);
            // App ID - set to 0xA5xxxxxx where A5 is signature
            // till it is implemented
            write_uint32(current_block + 68, 0xA5000000);
            memset(current_block + 72, '\0', 20); // reserved space
            write_uint32(current_block + 92, 105);
            write_uint32(current_block + 96, 3016000);
            memset(current_block + 100, '\0', block_size - 100); // Set remaining page to zero

            // master table b-tree
            init_bt_tbl_leaf(current_block + 100);

            // write table script record
            const char *default_table_name = "idx1";
            if (table_name == NULL)
                table_name = default_table_name;
            // write table script record
            int col_count = 5;
            // if (table_script) {
            //     uint16_t script_len = strlen(table_script);
            //     if (script_len > block_size - 100 - page_resv_bytes - 8 - 10)
            //         return SQLT_RES_TOO_LONG;
            //     set_col_val(4, SQLT_TYPE_TEXT, table_script, script_len);
            // } else {
                int table_name_len = strlen(table_name);
                size_t script_len = 13 + table_name_len + 2 + 13 + 15;
                for (int i = 0; i < total_col_count; i++)
                    script_len += col_names[i].length();
                script_len += total_col_count; // commas
                script_len += total_col_count; // spaces
                for (int i = 0; i < pk_col_count; i++)
                    script_len += col_names[i].length();
                script_len += pk_col_count; // commas
                // 100 byte header, 2 byte ptr, 3 byte rec/hdr vlen, 1 byte rowid
                // 6 byte hdr len, 5 byte "table", twice table name, 4 byte uint32 root
                if (script_len > (block_size - 100 - page_resv_bytes - blk_hdr_len
                        - 2 - 3 - 1 - 6 - 5 - strlen(table_name) * 2 - 4))
                    return SQLT_RES_TOO_LONG;
                uint8_t *script_loc = current_block + block_size - page_resv_bytes - script_len;
                uint8_t *script_pos = script_loc;
                memcpy(script_pos, "CREATE TABLE ", 13);
                script_pos += 13;
                memcpy(script_pos, table_name, table_name_len);
                script_pos += table_name_len;
                *script_pos++ = ' ';
                *script_pos++ = '(';
                for (int i = 0; i < total_col_count; i++) {
                    size_t str_len = col_names[i].length();
                    memcpy(script_pos, col_names[i].c_str(), str_len);
                    script_pos += str_len;
                    *script_pos++ = ',';
                    *script_pos++ = ' ';
                }
                memcpy(script_pos, "PRIMARY KEY (", 13);
                script_pos += 13;
                for (int i = 0; i < pk_col_count; ) {
                    size_t str_len = col_names[i].length();
                    memcpy(script_pos, col_names[i].c_str(), str_len);
                    script_pos += str_len;
                    i++;
                    *script_pos++ = (i == pk_col_count ? ')' : ',');
                }
                memcpy(script_pos, ") WITHOUT ROWID", 15);
                script_pos += 15;
            // }
            int32_t root_page_no = 2;
            const void *master_rec_values[] = {"table", table_name, table_name, &root_page_no, script_loc};
            const size_t master_rec_col_lens[] = {5, strlen(table_name), strlen(table_name), sizeof(root_page_no), script_len};
            const uint8_t master_rec_col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT32, SQLT_TYPE_TEXT};
            int res = write_new_rec(0, 1, 5, master_rec_values, master_rec_col_lens, master_rec_col_types);
            if (res < 0)
               return res;

            cache->write_page(master_block, 0, block_size);

            return SQLT_RES_OK;

        }

        // Converts float to Sqlite's Big-endian double
        int64_t float_to_double(const void *val) {
            uint32_t bytes = *((uint32_t *) val);
            uint8_t exp8 = (bytes >> 23) & 0xFF;
            uint16_t exp11 = exp8;
            if (exp11 != 0) {
                if (exp11 < 127)
                exp11 = 1023 - (127 - exp11);
                else
                exp11 = 1023 + (exp11 - 127);
            }
            return ((int64_t)(bytes >> 31) << 63) 
                | ((int64_t)exp11 << 52)
                | ((int64_t)(bytes & 0x7FFFFF) << (52-23) );
        }

        int64_t cvt_to_int64(const uint8_t *ptr, int type) {
            switch (type) {
                case SQLT_TYPE_NULL:
                case SQLT_TYPE_INT0:
                    return 0;
                case SQLT_TYPE_INT1:
                    return 1;
                case SQLT_TYPE_INT8:
                    return ptr[0];
                case SQLT_TYPE_INT16:
                    return read_uint16(ptr);
                case SQLT_TYPE_INT32:
                    return read_uint32(ptr);
                case SQLT_TYPE_INT48:
                    return read_uint48(ptr);
                case SQLT_TYPE_INT64:
                    return read_uint64(ptr);
                case SQLT_TYPE_REAL:
                    return read_double(ptr);
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    return 0; // TODO: do atol?
            }
            return -1; // should not reach here
        }

        // TODO: sqlite seems checking INT_MAX and INT_MIN when converting integers
        double cvt_to_dbl(const uint8_t *ptr, int type) {
            switch (type) {
                case SQLT_TYPE_REAL:
                    return read_double(ptr);
                case SQLT_TYPE_NULL:
                case SQLT_TYPE_INT0:
                    return 0;
                case SQLT_TYPE_INT1:
                    return 1;
                case SQLT_TYPE_INT8:
                    return ptr[0];
                case SQLT_TYPE_INT16:
                    return read_uint16(ptr);
                case SQLT_TYPE_INT32:
                    return read_uint32(ptr);
                case SQLT_TYPE_INT48:
                    return read_uint48(ptr);
                case SQLT_TYPE_INT64:
                    return read_uint64(ptr);
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    return 0; // TODO: do atol?
            }
            return -1; // should not reach here
        }

        int compare_col(const uint8_t *col1, int col_len1, int col_type1,
                            const uint8_t *col2, int col_len2, int col_type2) {
            switch (col_type1) {
                case SQLT_TYPE_BLOB:
                case SQLT_TYPE_TEXT:
                    if (col_type2 == SQLT_TYPE_TEXT || col_type2 == SQLT_TYPE_BLOB)
                        return util::compare(col1, col_len1, col2, col_len2);
                    if (col_type2 == SQLT_TYPE_NULL)
                        return 1;
                    return -1; // incompatible types
                case SQLT_TYPE_REAL: {
                    double col1_dbl = read_double(col1);
                    double col2_dbl = cvt_to_dbl(col2, col_type2);
                    return (col1_dbl < col2_dbl ? -1 : (col1_dbl > col2_dbl ? 1 : 0));
                    }
                case SQLT_TYPE_INT0:
                case SQLT_TYPE_INT1:
                case SQLT_TYPE_INT8:
                case SQLT_TYPE_INT16:
                case SQLT_TYPE_INT32:
                case SQLT_TYPE_INT48:
                case SQLT_TYPE_INT64: {
                    int64_t col1_int64 = cvt_to_int64(col1, col_type1);
                    int64_t col2_int64 = cvt_to_int64(col2, col_type2);
                    return (col1_int64 < col2_int64 ? -1 : (col1_int64 > col2_int64 ? 1 : 0));
                    }
                case SQLT_TYPE_NULL:
                    if (col_type2 == SQLT_TYPE_NULL)
                        return 0;
                    return -1; // NULL is less than any other type?
            }
            return -1; // should not be reached
        }

    public:
        uint8_t *master_block;
        int found_pos;
        unsigned long child_addr;
        int pk_count;
        int column_count;
        std::vector<std::string> column_names;
        const char *table_name;
        int blk_hdr_len;
        sqlite_index_blaster(int total_col_count, int pk_col_count, 
                std::vector<std::string> col_names, const char *tbl_name = NULL,
                int block_sz = DEFAULT_BLOCK_SIZE, int cache_sz = 0,
                const char *fname = NULL) : column_count (total_col_count), pk_count (pk_col_count),
                    column_names (col_names), table_name (tbl_name),
                    btree_handler<sqlite_index_blaster>(block_sz, cache_sz, fname, 1, true) {
            init();
        }

        void init() {
            U = block_size - page_resv_bytes;
            X = ((U-12)*64/255)-23;
            M = ((U-12)*32/255)-23;
            master_block = NULL;
            if (cache_size > 0) {
                if (cache->is_empty()) {
                    int res = write_page0(column_count, pk_count, column_names, table_name);
                    if (res != SQLT_RES_OK)
                        throw res;
                } else {
                    master_block = (uint8_t *) malloc(block_size);
                    if (cache->read_page(master_block, 0, block_size) != block_size)
                        throw 1;
                }
            }
            set_current_block_root();
        }

        void init_derived() {
        }

        sqlite_index_blaster(uint32_t block_sz, uint8_t *block, bool is_leaf, bool should_init)
                : btree_handler<sqlite_index_blaster>(block_sz, block, is_leaf, should_init) {
            U = block_size - page_resv_bytes;
            X = ((U-12)*64/255)-23;
            M = ((U-12)*32/255)-23;
            master_block = NULL;
        }

        ~sqlite_index_blaster() {
        }

        int make_new_rec(uint8_t *ptr, int col_count, const void *values[], 
                const size_t value_lens[] = NULL, const uint8_t types[] = NULL) {
            return write_new_rec(-1, 0, col_count, values, value_lens, types, ptr);
        }

        // Reads and returns big-endian uint16_t
        // at a given memory location
        uint16_t read_uint16(const uint8_t *ptr) {
            return (*ptr << 8) + ptr[1];
        }

        // Reads and returns big-endian int24_t
        // at a given memory location
        int32_t read_int24(const uint8_t *ptr) {
            uint32_t ret;
            ret = ((uint32_t)(*ptr & 0x80)) << 24;
            ret |= ((uint32_t)(*ptr++ & 0x7F)) << 16;
            ret |= ((uint32_t)*ptr++) << 8;
            ret += *ptr;
            return ret;
        }

        // Reads and returns big-endian uint24_t
        // at a given memory location
        uint32_t read_uint24(const uint8_t *ptr) {
            uint32_t ret;
            ret = ((uint32_t)*ptr++) << 16;
            ret += ((uint32_t)*ptr++) << 8;
            ret += *ptr;
            return ret;
        }

        // Reads and returns big-endian uint32_t
        // at a given memory location
        uint32_t read_uint32(const uint8_t *ptr) {
            uint32_t ret;
            ret = ((uint32_t)*ptr++) << 24;
            ret += ((uint32_t)*ptr++) << 16;
            ret += ((uint32_t)*ptr++) << 8;
            ret += *ptr;
            return ret;
        }

        // Reads and returns big-endian int48_t
        // at a given memory location
        int64_t read_int48(const uint8_t *ptr) {
            uint64_t ret;
            ret = ((uint64_t)(*ptr & 0x80)) << 56;
            ret |= ((uint64_t)(*ptr++ & 0x7F)) << 48;
            ret |= ((uint64_t)*ptr++) << 32;
            ret |= ((uint64_t)*ptr++) << 24;
            ret |= ((uint64_t)*ptr++) << 16;
            ret |= ((uint64_t)*ptr++) << 8;
            ret += *ptr;
            return ret;
        }

        // Reads and returns big-endian uint48_t :)
        // at a given memory location
        uint64_t read_uint48(const uint8_t *ptr) {
            uint64_t ret = 0;
            int len = 6;
            while (len--)
                ret += (*ptr++ << (8 * len));
            return ret;
        }

        // Reads and returns big-endian uint64_t
        // at a given memory location
        uint64_t read_uint64(const uint8_t *ptr) {
            uint64_t ret = 0;
            int len = 8;
            while (len--)
                ret += (*ptr++ << (8 * len));
            return ret;
        }

        // Reads and returns variable integer
        // from given location as uint16_t
        // Also returns the length of the varint
        uint16_t read_vint16(const uint8_t *ptr, int8_t *vlen) {
            uint16_t ret = 0;
            int8_t len = 3; // read max 3 bytes
            do {
                ret <<= 7;
                ret += *ptr & 0x7F;
                len--;
            } while ((*ptr++ & 0x80) == 0x80 && len);
            if (vlen)
                *vlen = 3 - len;
            return ret;
        }

        // Reads and returns variable integer
        // from given location as uint32_t
        // Also returns the length of the varint
        uint32_t read_vint32(const uint8_t *ptr, int8_t *vlen) {
            uint32_t ret = 0;
            int8_t len = 5; // read max 5 bytes
            do {
                ret <<= 7;
                ret += *ptr & 0x7F;
                len--;
            } while ((*ptr++ & 0x80) == 0x80 && len);
            if (vlen)
                *vlen = 5 - len;
            return ret;
        }

        double read_double(const uint8_t *data) {
            uint64_t value;
            std::memcpy(&value, data, sizeof(uint64_t)); // read 8 bytes from data pointer
            // SQLite stores 64-bit reals as big-endian integers
            value = ((value & 0xff00000000000000ull) >> 56) | // byte 1 -> byte 8
                    ((value & 0x00ff000000000000ull) >> 40) | // byte 2 -> byte 7
                    ((value & 0x0000ff0000000000ull) >> 24) | // byte 3 -> byte 6
                    ((value & 0x000000ff00000000ull) >> 8)  | // byte 4 -> byte 5
                    ((value & 0x00000000ff000000ull) << 8)  | // byte 5 -> byte 4
                    ((value & 0x0000000000ff0000ull) << 24) | // byte 6 -> byte 3
                    ((value & 0x000000000000ff00ull) << 40) | // byte 7 -> byte 2
                    ((value & 0x00000000000000ffull) << 56);  // byte 8 -> byte 1
            double result;
            std::memcpy(&result, &value, sizeof(double)); // convert the integer to a double
            return result;
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

        uint8_t *locate_col(int which_col, uint8_t *rec, int& col_type_or_len, int& col_len, int& col_type) {
            int8_t vlen;
            int hdr_len = read_vint32(rec, &vlen);
            int hdr_pos = vlen;
            uint8_t *data_ptr = rec + hdr_len;
            col_len = vlen = 0;
            do {
                data_ptr += col_len;
                hdr_pos += vlen;
                if (hdr_pos >= hdr_len)
                    return NULL;
                col_type_or_len = read_vint32(rec + hdr_pos, &vlen);
                col_len = derive_data_len(col_type_or_len);
                col_type = derive_col_type(col_type_or_len);
            } while (which_col--);
            return data_ptr;
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
                    *((int16_t *) out) = read_uint16(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT24:
                    *((int32_t *) out) = read_int24(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT32:
                    *((int32_t *) out) = read_uint32(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT48:
                    *((int64_t *) out) = read_int48(data_ptr);
                    return col_len;
                case SQLT_TYPE_INT64:
                    *((int64_t *) out) = read_uint64(data_ptr);
                    return col_len;
                case SQLT_TYPE_REAL:
                    *((double *) out) = read_double(data_ptr);
                    return col_len;
            }
            return SQLT_RES_MALFORMED;
        }

        void cleanup() {
            if (cache_size > 0 && master_block != NULL) {
                uint32_t file_size_in_pages = cache->file_page_count;
                write_uint32(master_block + 28, file_size_in_pages);
                cache->write_page(master_block, 0, block_size);
            }
            if (master_block != NULL)
                free(master_block);
        }

        inline void set_current_block_root() {
            set_current_block(root_block);
        }

        inline void set_current_block(uint8_t *m) {
            current_block = m;
            blk_hdr_len = (current_block[0] == 10 || current_block[0] == 13 ? 8 : 12);
        }

        int compare_keys(const uint8_t *rec1, int rec1_len, const uint8_t *rec2, int rec2_len) {
            int8_t vlen;
            const uint8_t *ptr1 = rec1;
            int hdr1_len = read_vint32(ptr1, &vlen);
            const uint8_t *data_ptr1 = ptr1 + hdr1_len;
            ptr1 += vlen;
            const uint8_t *ptr2 = rec2;
            int hdr2_len = read_vint32(ptr2, &vlen);
            const uint8_t *data_ptr2 = ptr2 + hdr2_len;
            ptr2 += vlen;
            int cols_compared = 0;
            int cmp = -1;
            for (int i = 0; i < hdr1_len; i++) {
                int col_len1 = read_vint32(ptr1, &vlen);
                int col_type1 = col_len1;
                if (col_len1 >= 12) {
                    if (col_len1 % 2) {
                        col_len1 = (col_len1 - 12) / 2;
                        col_type1 = SQLT_TYPE_BLOB;
                    } else {
                        col_len1 = (col_len1 - 13) / 2;
                        col_type1 = SQLT_TYPE_TEXT;
                    }
                } else
                    col_len1 = col_data_lens[col_len1];
                ptr1 += vlen;
                int col_len2 = read_vint32(ptr2, &vlen);
                int col_type2 = col_len2;
                if (col_len2 >= 12) {
                    if (col_len2 % 2) {
                        col_len2 = (col_len2 - 12) / 2;
                        col_type2 = SQLT_TYPE_BLOB;
                    } else {
                        col_len2 = (col_len2 - 13) / 2;
                        col_type2 = SQLT_TYPE_TEXT;
                    }
                } else
                    col_len2 = col_data_lens[col_len2];
                ptr2 += vlen;
                cmp = compare_col(data_ptr1, col_len1, col_type1,
                            data_ptr2, col_len2, col_type2);
                if (cmp != 0)
                    return cmp;
                cols_compared++;
                if (cols_compared == pk_count)
                    return 0;
                data_ptr1 += col_len1;
                data_ptr2 += col_len2;
            }
            return cmp;
        }

        int compare_first_key(const uint8_t *key1, int k_len1,
                            const uint8_t *key2, int k_len2) {
            if (k_len2 < 0) {
                return compare_keys(key1, abs(k_len1), key2, abs(k_len2));
            } else {
                int8_t vlen;
                k_len1 = (read_vint32(key1 + 1, &vlen) - 13) / 2;
                key1 += read_vint32(key1, &vlen);
                return util::compare(key1, k_len1, key2, k_len2);
            }
            return 0;
        }

        int get_first_key_len() {
            return ((block_size-page_resv_bytes-12)*64/255)-23+5;
        }

        int search_current_block(bptree_iter_ctx *ctx = NULL) {
            int middle, first, filled_sz, cmp;
            int8_t vlen;
            found_pos = -1;
            first = 0;
            filled_sz = read_uint16(current_block + 3);
            while (first < filled_sz) {
                middle = (first + filled_sz) >> 1;
                key_at = current_block + read_uint16(current_block + blk_hdr_len + middle * 2);
                if (!is_leaf())
                    key_at += 4;
                key_at_len = read_vint32(key_at, &vlen);
                key_at += vlen;
                if (key_len < 0) {
                    cmp = compare_keys(key_at, key_at_len, key, abs(key_len));
                } else {
                    uint8_t *raw_key_at = key_at + read_vint32(key_at, &vlen);
                    cmp = util::compare(raw_key_at, (read_vint32(key_at + vlen, NULL) - 13) / 2,
                                        key, key_len);
                }
                if (cmp < 0)
                    first = middle + 1;
                else if (cmp > 0)
                    filled_sz = middle;
                else {
                    if (ctx) {
                        ctx->found_page_idx = ctx->last_page_lvl;
                        ctx->found_page_pos = middle;
                    }
                    return middle;
                }
            }
            if (ctx) {
                ctx->found_page_idx = ctx->last_page_lvl;
                ctx->found_page_pos = ~filled_sz;
            }
            return ~filled_sz;
        }

        inline int get_header_size() {
            return blk_hdr_len;
        }

        void remove_entry(int pos) {
            del_ptr(pos);
            set_changed(1);
        }

        void remove_found_entry() {
            if (found_pos != -1) {
                del_ptr(found_pos);
                set_changed(1);
            }
            total_size--;
        }

        void del_ptr(int pos) {
            int filled_size = read_uint16(current_block + 3);
            uint8_t *kv_idx = current_block + blk_hdr_len + pos * 2;
            int8_t vlen;
            memmove(kv_idx, kv_idx + 2, (filled_size - pos) * 2);
            write_uint16(current_block + 3, filled_size - 1);
            // Remove the gaps instead of updating free blocks
            /*
            int rec_len = 0;
            if (!is_leaf())
                rec_len = 4;
            uint8_t *rec_ptr = current_block + read_uint16(kv_idx);
            rec_len += read_vint32(rec_ptr + rec_len, &vlen);
            rec_len += vlen;
            int kv_last_pos = get_kv_last_pos();
            if (rec_ptr != current_block + kv_last_pos)
                memmove(current_block + kv_last_pos + rec_len, current_block + kv_last_pos, rec_ptr - current_block + kv_last_pos);
            kv_last_pos += rec_len;
            set_kv_last_pos(kv_last_pos);*/
        }

        int get_ptr(int pos) {
            return read_uint16(current_block + blk_hdr_len + pos * 2);
        }

        void set_ptr(int pos, int ptr) {
            write_uint16(current_block + blk_hdr_len + pos * 2, ptr);
        }

        void make_space() {
            int lvl = current_block[0] & 0x1F;
            const int data_size = block_size - get_kv_last_pos();
            uint8_t data_buf[data_size];
            int new_data_len = 0;
            int new_idx;
            int orig_filled_size = filled_size();
            for (new_idx = 0; new_idx < orig_filled_size; new_idx++) {
                int src_idx = get_ptr(new_idx);
                int kv_len = current_block[src_idx];
                kv_len++;
                kv_len += current_block[src_idx + kv_len];
                kv_len++;
                new_data_len += kv_len;
                memcpy(data_buf + data_size - new_data_len, current_block + src_idx, kv_len);
                set_ptr(new_idx, block_size - new_data_len);
            }
            int new_kv_last_pos = block_size - new_data_len;
            memcpy(current_block + new_kv_last_pos, data_buf + data_size - new_data_len, new_data_len);
            //printf("%d, %d\n", data_size, new_data_len);
            set_kv_last_pos(new_kv_last_pos);
            search_current_block();
        }

        void add_first_kv_to_root(uint8_t *first_key, int first_len, 
                unsigned long old_block_addr, unsigned long new_block_addr) {
            uint32_t new_addr32 = (uint32_t) new_block_addr;
            write_uint32(current_block + 8, new_addr32 + 1);
            uint8_t addr[9];
            key = first_key;
            key_len = -first_len;
            value = NULL;
            value_len = 0;
            child_addr = old_block_addr + 1;
            //printf("value: %d, value_len1:%d\n", old_page, value_len);
            add_data(0);
        }

        int prepare_kv_to_add_to_parent(uint8_t *first_key, int first_len, unsigned long new_block_addr, uint8_t *addr) {
            key = (uint8_t *) first_key;
            key_len = -first_len;
            value = NULL;
            value_len = 0;
            child_addr = new_block_addr + 1;
            //printf("value: %d, value_len3:%d\n", new_page, value_len);
            int search_result = search_current_block();
            return search_result;
        }

        // TODO: updates only if length of existing record is same as what is updated
        void update_data() {
            int8_t vlen;
            if (key_len > 0) {
                int hdr_len = read_vint32(key_at, &vlen);
                uint8_t *raw_key_at = key_at + hdr_len;
                if (memcmp(raw_key_at, key, key_len) != 0)
                    std::cout << "Key not matching for update: " << key << ", len: " << key_len << std::endl;
                raw_key_at += key_len;
                if (hdr_len + key_len + value_len == key_at_len && key_at_len <= X)
                    memcpy(raw_key_at, value, value_len);
            } else {
                int rec_len = -key_len;
                if (rec_len == key_at_len && rec_len <= X)
                    memcpy(key_at, key, rec_len);
            }
        }

        bool is_full(int search_result) {
            int rec_len = abs(key_len) + value_len;
            if (key_len < 0) {
            } else {
                int key_len_vlen = get_vlen_of_uint32(key_len * 2 + 13);
                int value_len_vlen = get_vlen_of_uint32(value_len * 2 + 13);
                rec_len += key_len_vlen;
                rec_len += value_len_vlen;
                rec_len += get_vlen_of_uint32(key_len_vlen + value_len_vlen);
            }
            int on_page_len = (is_leaf() ? 0 : 4);
            int P = rec_len;
            int K = M+((P-M)%(U-4));
            on_page_len += get_vlen_of_uint32(rec_len);
            on_page_len += (P <= X ? P : (K <= X ? K : M));
            if (P > X)
                on_page_len += 4;
            int ptr_size = filled_size() + 1;
            ptr_size *= 2;
            if (get_kv_last_pos() <= (blk_hdr_len + ptr_size + on_page_len)) {
                //make_space();
                //if (get_kv_last_pos() <= (blk_hdr_len + ptr_size + rec_len))
                    return true;
            }
            return false;
        }

        uint8_t *allocate_block(int size, int type, int lvl) {
            uint8_t *new_page;
            if (cache_size > 0) {
                new_page = cache->get_new_page(current_block);
                set_block_changed(new_page, size, true);
                if ((cache->file_page_count - 1) * block_size == 1073741824UL) {
                    new_page = cache->get_new_page(current_block);
                    set_block_changed(new_page, size, true);
                }
            }// else
            //    new_page = (uint8_t *) util::aligned_alloc(size);
            if (type != BPT_BLK_TYPE_OVFL) {
                if (type == BPT_BLK_TYPE_INTERIOR)
                    init_bt_idx_interior(new_page);
                else
                    init_bt_idx_leaf(new_page);
            }
            return new_page;
        }

        uint8_t *split(uint8_t *first_key, int *first_len_ptr) {
            int orig_filled_size = filled_size();
            uint32_t SQLT_NODE_SIZE = block_size;
            int lvl = current_block[0] & 0x1F;
            uint8_t *b = allocate_block(SQLT_NODE_SIZE, is_leaf(), lvl);
            sqlite_index_blaster new_block(SQLT_NODE_SIZE, b, is_leaf(), true);
            set_changed(true);
            new_block.set_changed(true);
            SQLT_NODE_SIZE -= page_resv_bytes;
            int kv_last_pos = get_kv_last_pos();
            int half_kVLen = SQLT_NODE_SIZE - kv_last_pos + 1;
            half_kVLen /= 2;

            // Copy all data to new block in ascending order
            int brk_idx = -1;
            int brk_kv_pos;
            int brk_rec_len;
            uint32_t brk_child_addr;
            int tot_len;
            brk_kv_pos = tot_len = brk_rec_len = brk_child_addr = 0;
            int new_idx;
            for (new_idx = 0; new_idx < orig_filled_size; new_idx++) {
                int src_idx = get_ptr(new_idx);
                int8_t vlen;
                int on_page_len = (is_leaf() ? 0 : 4);
                int P = read_vint32(current_block + src_idx + on_page_len, &vlen);
                int K = M+((P-M)%(U-4));
                on_page_len += vlen;
                on_page_len += (P <= X ? P : (K <= X ? K : M));
                if (P > X)
                    on_page_len += 4;
                tot_len += on_page_len;
                if (brk_idx == -1) {
                    if (new_idx > 1 && (tot_len > half_kVLen || new_idx == (orig_filled_size / 2))) {
                        brk_idx = new_idx;
                        *first_len_ptr = P;
                        //if (*first_len_ptr > 2000)
                        //    cout << "GT 200: " << new_idx << ", rec_len: " << on_page_len << ", flp: " << *first_len_ptr << ", src_idx: " << src_idx << endl;
                        memcpy(first_key, current_block + src_idx + (is_leaf() ? 0 : 4) + vlen, on_page_len - (is_leaf() ? 0 : 4) - vlen);
                        brk_rec_len = on_page_len;
                        if (!is_leaf())
                            brk_child_addr = read_uint32(current_block + src_idx);
                        brk_kv_pos = kv_last_pos + brk_rec_len;
                    }
                }
                if (brk_idx != new_idx) { // don't copy the middle record, but promote it to prev level
                    memcpy(new_block.current_block + kv_last_pos, current_block + src_idx, on_page_len);
                    new_block.ins_ptr(brk_idx == -1 ? new_idx : new_idx - 1, kv_last_pos + brk_rec_len);
                    kv_last_pos += on_page_len;
                }
            }

            kv_last_pos = get_kv_last_pos();
            if (brk_rec_len) {
                memmove(new_block.current_block + kv_last_pos + brk_rec_len, new_block.current_block + kv_last_pos,
                            SQLT_NODE_SIZE - kv_last_pos - brk_rec_len);
                kv_last_pos += brk_rec_len;
            }
            {
                int diff = (SQLT_NODE_SIZE - brk_kv_pos + brk_rec_len);
                for (new_idx = 0; new_idx < brk_idx; new_idx++) {
                    set_ptr(new_idx, new_block.get_ptr(new_idx) + diff);
                } // Set index of copied first half in old block
                // Copy back first half to old block
                int old_blk_new_len = brk_kv_pos - kv_last_pos;
                memcpy(current_block + SQLT_NODE_SIZE - old_blk_new_len,
                    new_block.current_block + kv_last_pos, old_blk_new_len);
                set_kv_last_pos(SQLT_NODE_SIZE - old_blk_new_len);
                set_filled_size(brk_idx);
                if (!is_leaf()) {
                    uint32_t addr_to_write = brk_child_addr;
                    brk_child_addr = read_uint32(current_block + 8);
                    write_uint32(current_block + 8, addr_to_write);
                }
            }

            {
                int new_size = orig_filled_size - brk_idx - 1;
                uint8_t *block_ptrs = new_block.current_block + blk_hdr_len;
                memmove(block_ptrs, block_ptrs + (brk_idx << 1), new_size << 1);
                new_block.set_kv_last_pos(brk_kv_pos);
                new_block.set_filled_size(new_size);
                if (!is_leaf())
                    write_uint32(new_block.current_block + 8, brk_child_addr);
            }

            return b;
        }

        int write_child_page_addr(uint8_t *ptr, int search_result) {
            if (!is_leaf()) {
                if (search_result == filled_size() && search_result > 0) {
                    unsigned long prev_last_addr = read_uint32(current_block + 8);
                    write_uint32(current_block + 8, child_addr);
                    child_addr = prev_last_addr;
                }
                if (search_result < filled_size()) {
                    int cur_pos = read_uint16(current_block + blk_hdr_len + search_result * 2);
                    uint32_t old_addr = read_uint32(current_block + cur_pos);
                    write_uint32(current_block + cur_pos, child_addr);
                    child_addr = old_addr;
                }
                write_uint32(ptr, child_addr);
                return 4;
            }
            return 0;
        }

        void add_data(int search_result) {

            // P is length of payload or record length
            int P, hdr_len;
            if (key_len < 0) {
                P = -key_len;
                hdr_len = 0;
            } else {
                int key_len_vlen, value_len_vlen;
                P = key_len + value_len;
                key_len_vlen = get_vlen_of_uint32(key_len * 2 + 13);
                value_len_vlen = get_vlen_of_uint32(value_len * 2 + 13);
                hdr_len = key_len_vlen + value_len_vlen;
                hdr_len += get_vlen_of_uint32(hdr_len);
                P += hdr_len;
            }
            // See https://www.sqlite.org/fileformat.html
            int K = M+((P-M)%(U-4));
            int on_bt_page = (P <= X ? P : (K <= X ? K : M));
            int kv_last_pos = get_kv_last_pos();
            kv_last_pos -= on_bt_page;
            kv_last_pos -= get_vlen_of_uint32(P);
            if (!is_leaf())
                kv_last_pos -= 4;
            if (P > X)
                kv_last_pos -= 4;
            uint8_t *ptr = current_block + kv_last_pos;
            ptr += write_child_page_addr(ptr, search_result);
            copy_kv_with_overflow(ptr, P, on_bt_page, hdr_len);
            ins_ptr(search_result, kv_last_pos);
            int filled_size = read_uint16(current_block + 3);
            set_kv_last_pos(kv_last_pos);
            set_changed(true);

            // if (BPT_MAX_KEY_LEN < key_len)
            //     BPT_MAX_KEY_LEN = key_len;

        }

        void copy_kv_with_overflow(uint8_t *ptr, int P, int on_bt_page, int hdr_len) {
            int k_len, v_len;
            ptr += write_vint32(ptr, P);
            if (key_len < 0) {
                if (!is_leaf()) {
                    memcpy(ptr, key, on_bt_page + (P == on_bt_page ? 0 : 4));
                    return;
                }
                k_len = P;
                v_len = 0;
            } else {
                k_len = key_len;
                v_len = value_len;
                ptr += write_vint32(ptr, hdr_len);
                ptr += write_vint32(ptr, key_len * 2 + 13);
                ptr += write_vint32(ptr, value_len * 2 + 13);
            }
            int on_page_remaining = on_bt_page - hdr_len;
            bool copying_on_bt_page = true;
            int key_remaining = k_len;
            int val_remaining = v_len;
            uint8_t *ptr0 = ptr;
            do {
                if (key_remaining > 0) {
                    int to_copy = key_remaining > on_page_remaining ? on_page_remaining : key_remaining;
                    memcpy(ptr, key + k_len - key_remaining, to_copy);
                    ptr += to_copy;
                    key_remaining -= to_copy;
                    on_page_remaining -= to_copy;
                }
                if (val_remaining > 0 && key_remaining <= 0) {
                    int to_copy = val_remaining > on_page_remaining ? on_page_remaining : val_remaining;
                    memcpy(ptr, value + v_len - val_remaining, to_copy);
                    ptr += to_copy;
                    val_remaining -= to_copy;
                }
                if (key_remaining > 0 || val_remaining > 0) {
                    uint32_t new_page_no = cache->get_page_count() + 1;
                    if ((new_page_no - 1) * block_size == 1073741824UL)
                        new_page_no++;
                    write_uint32(copying_on_bt_page ? ptr : ptr0, new_page_no);
                    uint8_t *ovfl_ptr = allocate_block(block_size, BPT_BLK_TYPE_OVFL, 0);
                    ptr0 = ptr = ovfl_ptr;
                    ptr += 4;
                    on_page_remaining = U - 4;
                }
                if (!copying_on_bt_page)
                    write_uint32(ptr0, 0);
                copying_on_bt_page = false;
            } while (key_remaining > 0 || val_remaining > 0);
        }

        void ins_ptr(int pos, int data_loc) {
            int filled_size = read_uint16(current_block + 3);
            uint8_t *kv_idx = current_block + blk_hdr_len + pos * 2;
            memmove(kv_idx + 2, kv_idx, (filled_size - pos) * 2);
            write_uint16(current_block + 3, filled_size + 1);
            write_uint16(kv_idx, data_loc);
        }

        void add_first_data() {
            add_data(0);
        }

        int filled_size() {
            return read_uint16(current_block + 3);
        }

        void set_filled_size(int filled_size) {
            write_uint16(current_block + 3, filled_size);
        }

        int get_kv_last_pos() {
            return read_uint16(current_block + 5);
        }

        void set_kv_last_pos(int val) {
            if (val == 0)
                val = 65535;
            write_uint16(current_block + 5, val);
        }

        void set_changed(bool is_changed) {
            if (is_changed)
                current_block[block_size - page_resv_bytes] |= 0x40;
            else
                current_block[block_size - page_resv_bytes] &= 0xBF;
        }

        bool is_changed() {
            return current_block[block_size - page_resv_bytes] & 0x40;
        }

        static void set_block_changed(uint8_t *block, int block_size, bool is_changed) {
            if (is_changed)
                block[block_size - page_resv_bytes] |= 0x40;
            else
                block[block_size - page_resv_bytes] &= 0xBF;
        }

        static bool is_block_changed(uint8_t *block, int block_size) {
            return block[block_size - page_resv_bytes] & 0x40;
        }

        int get_level(uint8_t *block, int block_size) {
            return block[block_size - page_resv_bytes] & 0x1F;
        }

        void set_level(uint8_t *block, int block_size, int lvl) {
            block[block_size - page_resv_bytes] = (block[block_size - page_resv_bytes] & 0xE0) + lvl;
        }

        inline bool is_leaf() {
            return current_block[0] > 9;
        }

        void set_leaf(char is_leaf) {
            if (is_leaf)
                init_bt_idx_leaf(current_block);
            else
                init_bt_idx_interior(current_block);
            blk_hdr_len = (current_block[0] == 10 || current_block[0] == 13 ? 8 : 12);
        }

        void init_current_block() {
            //memset(current_block, '\0', BFOS_NODE_SIZE);
            //cout << "Tree init block" << endl;
            if (!is_block_given) {
            }
        }

        inline uint8_t *get_value_at(int *vlen) {
            int8_t vint_len;
            uint8_t *data_ptr = key_at + read_vint32(key_at, &vint_len);
            int hdr_vint_len = vint_len;
            int k_len = (read_vint32(key_at + hdr_vint_len, &vint_len) - 13) / 2;
            if (vlen != NULL)
                *vlen = (read_vint32(key_at + hdr_vint_len + vint_len, NULL) - 13) / 2;
            return (uint8_t *) data_ptr + k_len;
        }

        void copy_value(uint8_t *val, int *p_value_len) {
            int8_t vlen;
            int P = key_at_len;
            int K = M+((P-M)%(U-4));
            int on_bt_page = (P <= X ? P : (K <= X ? K : M));
            if (key_len < 0) {
                *p_value_len = key_at_len;
                memcpy(val, key_at, on_bt_page);
                if (P > on_bt_page)
                    copy_overflow(val + on_bt_page, 0, P - on_bt_page, read_uint32(key_at + on_bt_page));
            } else {
                int hdr_len = read_vint32(key_at, &vlen);
                uint8_t *raw_val_at = key_at + hdr_len;
                int8_t key_len_vlen;
                int k_len = (read_vint32(key_at + vlen, &key_len_vlen) - 13) / 2;
                *p_value_len = (read_vint32(key_at + vlen + key_len_vlen, NULL) - 13) / 2;
                if (hdr_len + k_len <= on_bt_page) {
                    raw_val_at += k_len;
                    int val_len_on_bt = on_bt_page - k_len - hdr_len;
                    memcpy(val, raw_val_at, val_len_on_bt);
                    if (*p_value_len - val_len_on_bt > 0)
                        copy_overflow(val + val_len_on_bt, 0, *p_value_len - val_len_on_bt, read_uint32(key_at + on_bt_page));
                } else {
                    if (*p_value_len > 0)
                        copy_overflow(val, hdr_len + k_len - on_bt_page, *p_value_len, read_uint32(key_at + on_bt_page));
                }
            }
        }

        void copy_overflow(uint8_t *val, int offset, int len, uint32_t ovfl_page) {
            do {
                uint8_t *ovfl_blk = cache->get_disk_page_in_cache(ovfl_page - 1, current_block);
                ovfl_page = read_uint32(ovfl_blk);
                int cur_pos = 4;
                int bytes_remaining = U - 4;
                if (offset > 0) {
                    if (offset > U - 4) {
                        offset -= (U - 4);
                        continue;
                    } else {
                        cur_pos += offset;
                        bytes_remaining -= offset;
                    }
                }
                if (bytes_remaining > 0) {
                    int to_copy = len > bytes_remaining ? bytes_remaining : len;
                    memcpy(val, ovfl_blk + cur_pos, to_copy);
                    val += to_copy;
                    len -= to_copy;
                }
            } while (len > 0 && ovfl_page > 0);
        }

        inline uint8_t *get_child_ptr_pos(int search_result) {
            if (search_result < 0)
                search_result = ~search_result;
            if (search_result == filled_size())
                return current_block + 8;
            return current_block + read_uint16(current_block + blk_hdr_len + search_result * 2);
        }

        uint8_t *get_ptr_pos() {
            return current_block + blk_hdr_len;
        }

        inline uint8_t *get_child_ptr(uint8_t *ptr) {
            uint64_t ret = read_uint32(ptr);
            return (uint8_t *) ret;
        }

        inline int get_child_page(uint8_t *ptr) {
            return read_uint32(ptr) - 1;
        }

        // bool next(bptree_iter_ctx *ctx, int *in_size_out_val_len = NULL, uint8_t *val = NULL) {
        //     ctx->found_page_idx;
        //     ctx->found_page_pos;
        //     ctx->last_page_lvl;
        //     ctx->pages;
        //     if (val != NULL)
        //         copy_value(val, in_size_out_val_len);
        //     return true;
        // }

        uint8_t *find_split_source(int search_result) {
            return NULL;
        }

};

#undef page_resv_bytes
#undef BPT_LEAF0_LVL
#undef BPT_STAGING_LVL
#undef BPT_PARENT0_LVL

#undef BPT_BLK_TYPE_INTERIOR
#undef BPT_BLK_TYPE_LEAF
#undef BPT_BLK_TYPE_OVFL

#undef DEFAULT_BLOCK_SIZE

#undef descendant
#undef MAX_LVL_COUNT

#endif
