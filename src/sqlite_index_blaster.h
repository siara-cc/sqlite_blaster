/*
  Sqlite Index Blaster

  Blazing fast Sqlite index inserts and updates

  Blazing fast index operations in return for compromise
  on fault tolerance.

  https://github.com/siara-cc/sqlite_index_blaster

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
 * @file sqlite_index_blaster.h
 * @author Arundale Ramanathan
 * @brief API for Sqlite Index Blaster
 *
 * This file describes each function of the Sqlite Index Blaster API \n
 * For finding out how this API can be used in your program, \n
 * please see test_sqlite_index_blaster.c.
 */

#include <stdio.h>
#include <string.h>
#include "lru_cache.h"
#include "btree_handler.h"

typedef unsigned char uint8_t;

enum {SQIB_TYPE_NULL = 0, SQIB_TYPE_INT8, SQIB_TYPE_INT16, SQIB_TYPE_INT24, SQIB_TYPE_INT32, SQIB_TYPE_INT48, SQIB_TYPE_INT64,
        SQIB_TYPE_REAL, SQIB_TYPE_INT0, SQIB_TYPE_INT1, SQIB_TYPE_TEXT = 12, SQIB_TYPE_BLOB = 13};

enum {SQIB_RES_OK = 0, SQIB_RES_ERR = -1, SQIB_RES_INV_PAGE_SZ = -2, 
  SQIB_RES_TOO_LONG = -3, SQIB_RES_WRITE_ERR = -4, SQIB_RES_FLUSH_ERR = -5};

enum {SQIB_RES_SEEK_ERR = -6, SQIB_RES_READ_ERR = -7,
  SQIB_RES_INVALID_SIG = -8, SQIB_RES_MALFORMED = -9,
  SQIB_RES_NOT_FOUND = -10, SQIB_RES_NOT_FINALIZED = -11,
  SQIB_RES_TYPE_MISMATCH = -12, SQIB_RES_INV_CHKSUM = -13,
  SQIB_RES_NEED_1_PK};

/**
 * @brief Sqlite Index Blaster class for super fast index operations
 */
class sqlite_index_blaster : btree_handler<sqlite_index_blaster> {

  protected:
    uint16_t make_space_for_new_row(int32_t page_size, uint16_t len_of_rec_len_rowid, uint16_t new_rec_len);
    int sqib_append_row_with_values(uint8_t *buf, int page_size, int col_count,
          const uint8_t types[], const void *values[], int lengths[]);
    int write_page0(const char *table_name, int col_count,
          int pk_col_count, const char *col_names[]);
    void write_rec_len_rowid_hdr_len(uint8_t *ptr, uint16_t rec_len,
          uint32_t rowid, uint16_t hdr_len);
    int append_empty_row();
    uint16_t acquire_last_pos(uint8_t *ptr);

  public:
    /** 
     * Creates or opens given file and initializes the cache for index operations
     * @param[in] filename        Name of Sqlite database file.
     * @param[in] pg_sz           Database page size.
     * @param[in] cache_size      Cache size in number of pages.
     * @param[in] total_col_count Total number columns including primary keys.
     * @param[in] pk_col_count    Number of primary key columns.
     * @param[in] col_names       Names of columns. Default c1, c2, etc.
     * @param[in] table_name      Names of table. Default t1.
     * @return SQIB_RES_OK if no error
     */
    sqlite_index_blaster(char *filename, int pg_sz, int cache_size,
      int total_col_count, int pk_col_count, const char *col_names[] = NULL,
      uint8_t types[], const char *table_name = NULL);

    uint8_t *get_row(const char *pk_values[]);
    uint8_t *put_row(const char *values[]);
    uint8_t *delete_row(const char *pk_values[]);

    int set_col_val(int col_idx, int type, const void *val, uint16_t len);
    const void *get_col_val(int col_idx, uint32_t *out_col_type);

    /** 
     * Flushes cache data into disk
     */
    int flush();

    /** 
     * Flushes cache data into disk, clears cache and closes file
     */
    ~sqlite_index_blaster();

};
