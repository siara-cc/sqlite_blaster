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

typedef unsigned char byte;

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
    lru_cache *cache = NULL;

  public:
    /** 
     * Creates or opens given file and initializes the cache for index operations
     * @param[in] filename    Name of Sqlite database file.
     * @param[in] page_size   Database page size.
     * @param[in] cache_size  Cache size in number of pages.
     * @param[in] pk_col_count Number of primary key columns.
     * @param[in] pk_col_names Names of primary key columns
     * @param[in] other_col_count Number of columns other than primary keys.
     * @param[in] other_col_names Names of other columns.
     * @return SQIB_RES_OK if no error
     */
    sqlite_index_blaster(char *filename, int page_size, int cache_size, const char *table_name = NULL, 
          int col_count = 0, int pk_col_count = 0, const char *col_names[] = NULL)
            : btree_handler<sqlite_index_blaster>(filename, page_size, cache_size,
                table_name, col_count, pk_col_count, col_names);

    byte *get_row(int pk_value_count, const char *pk_values[], uint8_t types[]);

    byte *put_row(int col_count, int pk_col_count, const char *values[], int value_count);

    byte *delete_row(const char *pk_values[], int pk_value_count);

    /** 
     * Flushes cache data into disk
     */
    int flush();

    /** 
     * Flushes cache data into disk, clears cache and closes file
     */
    ~sqlite_index_blaster();

};

