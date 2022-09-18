/*
  Copyright @ 2022 Arundale Ramanathan, Siara Logics (cc)

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

/**
 * @file sqlite_index_blaster.c
 * @author Arundale Ramanathan
 * @brief Sqlite Index Blaster
 *
 * This file implements each function of the Sqlite Index Blaster API \n
 * defined in sqlite_index_blaster.h
 */

#include "sqlite_index_blaster.h"
#include <sys/types.h>
#include <sys/stat.h>

// Returns how many bytes the given integer will
// occupy if stored as a variable integer
int8_t get_vlen_of_uint16(uint16_t vint) {
  return vint > 16383 ? 3 : (vint > 127 ? 2 : 1);
}

// Returns how many bytes the given integer will
// occupy if stored as a variable integer
int8_t get_vlen_of_uint32(uint32_t vint) {
  return vint > 268435455 ? 5 : (vint > 2097151 ? 4 
           : (vint > 16383 ? 3 : (vint > 127 ? 2 : 1)));
}

// Stores the given byte in the given location
// in big-endian sequence
void write_uint8(byte *ptr, uint8_t input) {
  ptr[0] = input;
}

// Stores the given uint16_t in the given location
// in big-endian sequence
void write_uint16(byte *ptr, uint16_t input) {
  ptr[0] = input >> 8;
  ptr[1] = input & 0xFF;
}

// Stores the given uint32_t in the given location
// in big-endian sequence
void write_uint32(byte *ptr, uint32_t input) {
  int i = 4;
  while (i--)
    *ptr++ = (input >> (8 * i)) & 0xFF;
}

// Stores the given uint64_t in the given location
// in big-endian sequence
void write_uint64(byte *ptr, uint64_t input) {
  int i = 8;
  while (i--)
    *ptr++ = (input >> (8 * i)) & 0xFF;
}

// Stores the given uint16_t in the given location
// in variable integer format
int write_vint16(byte *ptr, uint16_t vint) {
  int len = get_vlen_of_uint16(vint);
  for (int i = len - 1; i > 0; i--)
    *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
  *ptr = vint & 0x7F;
  return len;
}

// Stores the given uint32_t in the given location
// in variable integer format
int write_vint32(byte *ptr, uint32_t vint) {
  int len = get_vlen_of_uint32(vint);
  for (int i = len - 1; i > 0; i--)
    *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
  *ptr = vint & 0x7F;
  return len;
}

// Reads and returns big-endian uint8_t
// at a given memory location
uint8_t read_uint8(byte *ptr) {
  return *ptr;
}

// Reads and returns big-endian uint16_t
// at a given memory location
uint16_t read_uint16(byte *ptr) {
  return (*ptr << 8) + ptr[1];
}

// Reads and returns big-endian uint32_t
// at a given memory location
uint32_t read_uint32(byte *ptr) {
  uint32_t ret;
  ret = ((uint32_t)*ptr++) << 24;
  ret += ((uint32_t)*ptr++) << 16;
  ret += ((uint32_t)*ptr++) << 8;
  ret += *ptr;
  return ret;
}

// Reads and returns big-endian uint32_t
// at a given memory location
uint64_t read_uint64(byte *ptr) {
  uint32_t ret = 0;
  int len = 8;
  while (len--)
    ret += (*ptr++ << (8 * len));
  return ret;
}

// Reads and returns variable integer
// from given location as uint16_t
// Also returns the length of the varint
uint16_t read_vint16(byte *ptr, int8_t *vlen) {
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
uint32_t read_vint32(byte *ptr, int8_t *vlen) {
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

// Returns type of column based on given value and length
// See https://www.sqlite.org/fileformat.html#record_format
uint32_t derive_col_type_or_len(int type, const void *val, int len) {
  uint32_t col_type_or_len = type;
  if (type > 11)
    col_type_or_len = len * 2 + type;
  return col_type_or_len;    
}

#define LEN_OF_REC_LEN 3
#define LEN_OF_HDR_LEN 2
int dblog_append_row_with_values(byte *buf, int page_size, int col_count,
      uint8_t types[], const void *values[], int lengths[]) {

  uint16_t len_of_rec_len_rowid = LEN_OF_REC_LEN + get_vlen_of_uint32(wctx->cur_write_rowid);
  uint16_t new_rec_len = 0;
  uint16_t hdr_len = LEN_OF_HDR_LEN;
  for (int i = 0; i < col_count; i++) {
    uint32_t col_type = derive_col_type_or_len(types[i], values[i], lengths[i]);
    new_rec_len += col_type;
    hdr_len += get_vlen_of_uint32(col_type);
  }
  new_rec_len += hdr_len;
  uint16_t last_pos = make_space_for_new_row(wctx, page_size,
                        len_of_rec_len_rowid, new_rec_len);
  if (!last_pos)
    return SQIB_RES_MALFORMED;
  byte *ptr = buf + (buf[0] == 13 ? 0 : 100);
  int rec_count = read_uint16(ptr + 3) + 1;
  if (rec_count * 2 + 8 >= last_pos)
    return DBLOG_RES_MALFORMED;

  write_rec_len_rowid_hdr_len(wctx->buf + last_pos, new_rec_len, 
                    wctx->cur_write_rowid, hdr_len);
  byte *rec_ptr = wctx->buf + last_pos + len_of_rec_len_rowid + LEN_OF_HDR_LEN;
  for (int i = 0; i < wctx->col_count; i++) {
    uint32_t col_type = derive_col_type_or_len(types[i], values[i], lengths[i]);
    int8_t vint_len = write_vint32(rec_ptr, col_type);
    rec_ptr += vint_len;
  }
  for (int i = 0; i < wctx->col_count; i++) {
    if (values[i] != NULL)
      rec_ptr += write_data(rec_ptr, types[i], values[i], lengths[i]);
  }
  write_uint16(ptr + 3, rec_count);
  write_uint16(ptr + 5, last_pos);
  write_uint16(ptr + 8 - 2 + (rec_count * 2), last_pos);
  wctx->state = DBLOG_ST_WRITE_PENDING;

  return DBLOG_RES_OK;
}

// Initializes the buffer as a B-Tree Leaf table
void init_bt_tbl_leaf(byte *ptr) {
  ptr[0] = 13; // Leaf table b-tree page
  write_uint16(ptr + 1, 0); // No freeblocks
  write_uint16(ptr + 3, 0); // No records yet
  write_uint16(ptr + 5, 0); // No records yet
  write_uint8(ptr + 7, 0); // Fragmented free bytes
}

// Writes data into buffer to form first page of Sqlite db
int write_page0(FILE *fp, byte *buf, int page_size, const char *table_name, int pk_col_count, 
    const char *pk_col_names[], int other_col_count, const char *other_col_names[]) {

  if (page_size % 512 || page_size < 512 || page_size > 65536)
    throw SQIB_RES_INV_PAGE_SZ;

  // 100 byte header - refer https://www.sqlite.org/fileformat.html
  memcpy(buf, "SQLite format 3\0", 16);
  write_uint16(buf + 16, page_size == 65536 ? 1 : (uint16_t) page_size);
  buf[18] = 1;
  buf[19] = 1;
  buf[20] = 0; // page_resv_bytes;
  buf[21] = 64;
  buf[22] = 32;
  buf[23] = 32;
  //write_uint32(buf + 24, 0);
  //write_uint32(buf + 28, 0);
  //write_uint32(buf + 32, 0);
  //write_uint32(buf + 36, 0);
  //write_uint32(buf + 40, 0);
  memset(buf + 24, '\0', 20); // Set to zero, above 5
  write_uint32(buf + 28, 2); // TODO: Update during finalize
  write_uint32(buf + 44, 4);
  //write_uint16(buf + 48, 0);
  //write_uint16(buf + 52, 0);
  memset(buf + 48, '\0', 8); // Set to zero, above 2
  write_uint32(buf + 56, 1);
  // User version initially 0, set to table leaf count
  // used to locate last leaf page for binary search
  // and move to last page.
  write_uint32(buf + 60, 0);
  write_uint32(buf + 64, 0);
  // App ID - set to 0xA5xxxxxx where A5 is signature
  // till it is implemented
  write_uint32(buf + 68, 0xA5000000);
  memset(buf + 72, '\0', 20); // reserved space
  write_uint32(buf + 92, 105);
  write_uint32(buf + 96, 3016000);
  memset(buf + 100, '\0', page_size - 100); // Set remaing page to zero

  // master table b-tree
  init_bt_tbl_leaf(buf + 100);

  // write table script record
  int orig_col_count = pk_col_count + other_col_count;
  dblog_append_empty_row(wctx);
  dblog_set_col_val(wctx, 0, DBLOG_TYPE_TEXT, "table", 5);
  char *default_table_name = "t1";
  if (table_name == NULL)
    table_name = default_table_name;
  dblog_set_col_val(wctx, 1, DBLOG_TYPE_TEXT, table_name, strlen(table_name));
  dblog_set_col_val(wctx, 2, DBLOG_TYPE_TEXT, table_name, strlen(table_name));
  int32_t root_page = 2;
  dblog_set_col_val(wctx, 3, DBLOG_TYPE_INT, &root_page, 4);
  if (table_script) {
    uint16_t script_len = strlen(table_script);
    if (script_len > page_size - 100 - wctx->page_resv_bytes - 8 - 10)
      return DBLOG_RES_TOO_LONG;
    dblog_set_col_val(wctx, 4, DBLOG_TYPE_TEXT, table_script, script_len);
  } else {
    int table_name_len = strlen(table_name);
    int script_len = (13 + table_name_len + 2 + 5 * orig_col_count);
    if (script_len > page_size - 100 - wctx->page_resv_bytes - 8 - 10)
      return DBLOG_RES_TOO_LONG;
    dblog_set_col_val(wctx, 4, DBLOG_TYPE_TEXT, buf + 110, script_len);
    byte *script_pos = buf + page_size - buf[20] - script_len;
    memcpy(script_pos, "CREATE TABLE ", 13);
    script_pos += 13;
    memcpy(script_pos, table_name, table_name_len);
    script_pos += table_name_len;
    *script_pos++ = ' ';
    *script_pos++ = '(';
    for (int i = 0; i < orig_col_count; ) {
      i++;
      *script_pos++ = 'c';
      *script_pos++ = '0' + (i < 100 ? 0 : (i / 100));
      *script_pos++ = '0' + (i < 10 ? 0 : ((i < 100 ? i : i - 100) / 10));
      *script_pos++ = '0' + (i % 10);
      *script_pos++ = (i == orig_col_count ? ')' : ',');
    }
  }
  int res = write_page(wctx, 0, page_size);
  if (res)
    return res;
  wctx->col_count = orig_col_count;
  wctx->cur_write_page = 1;
  wctx->cur_write_rowid = 0;
  init_bt_tbl_leaf(wctx->buf);
  wctx->state = DBLOG_ST_WRITE_PENDING;

  return DBLOG_RES_OK;

}

sqlite_index_blaster::sqlite_index_blaster(char *filename, int page_size, int cache_size,
      const char *table_name = NULL, int col_count = 0, int pk_col_count = 0,
      const char *col_names[] = NULL) {
  struct stat sb;
  if (stat(filename, &sb) == -1) {
    if (errno == 2) { // file not found
      FILE *fp = fopen(filename, "wb");
      if (fp == NULL)
        throw errno;
      write_page0(fp, page_size, col_count, pk_col_count, col_names);
      fclose(fp);
    } else {
      // check page size and signature
    }
  }
  cache = new lru_cache(page_size, cache_size, filename, 1);
}

byte *sqlite_index_blaster::get_row(const char *pk_values[], int pk_value_count) {
}

byte *sqlite_index_blaster::put_row(const char *pk_values[], int pk_value_count, const char *values[], int value_count) {
}

byte *sqlite_index_blaster::delete_row(const char *pk_values[], int pk_value_count) {

}

/** 
 * Flushes cache data into disk
 */
int sqlite_index_blaster::flush() {

}

/** 
 * Flushes cache data into disk, clears cache and closes file
 */
int sqlite_index_blaster::close() {
  delete cache;
  cache = NULL;
}

sqlite_index_blaster::~sqlite_index_blaster() {
  if (cache != NULL)
    delete cache;
}
