/*
  Testing program for Sqlite Index Blaster

  Sqlite Index Blaster is a library that inserts records in Sqlite format 3
  much faster than the original SQLite library.
  This utility is intended for testing it.

  https://github.com/siara-in/sqlite_micro_logger

  Copyright @ 2019 Arundale Ramanathan, Siara Logics (cc)

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

#ifndef ARDUINO

#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <fstream>
#include <string>
#include <sstream>

#include "sqlite_index_blaster.h"

using namespace std;

void print_usage() {
  printf("\nTesting Sqlite Index Blaster\n");
  printf("---------------------------\n\n");
  printf("Sqlite Index Blaster is a library that inserts records in Sqlite format 3\n");
  printf("much faster than the original SQLite library.\n");
  printf("This utility is intended for testing it.\n\n");
  printf("Usage\n");
  printf("-----\n\n");
  printf("test_sqlite_blaster -c <db_name.db> <page_size> <tbl_name>\n");
  printf("             <total_col_count> <pk_col_count> <col_1>,<col_2>...<col_n>\n");
  printf("    Creates a Sqlite database with the given name and page size\n");
  printf("          and given Column names in CSV format.\n");
  printf("          Overwrites any existing file\n\n");
  printf("test_sqlite_blaster -i <db_name.db> <page_size>\n");
  printf("             <total_col_count> <pk_col_count> <csv_1> ... <csv_n>\n");
  printf("    Inserts to the Sqlite database created using -c above\n");
  printf("        with records in CSV format (page_size, total_col_count\n");
  printf("        and pk_col_count have to match)\n\n");
  printf("test_sqlite_blaster -r <db_name.db> <page_size> <total_col_count>\n");
  printf("             <pk_col_count> <pk_val_1>,<pk_val_2>...<pk_val_n>\n");
  printf("    Searches <db_name.db> for given keys and prints result\n\n");
  printf("test_sqlite_blaster -t\n");
  printf("    Runs pre-defined tests\n\n");
  printf("NOTE: -r and -i do not detect data types and consider columns as text\n");
  printf("      For using other datatypes, please use the API.\n\n");
}

bool validate_page_size(int32_t page_size) {
  if (page_size == 512 || page_size == 1024 || page_size == 2048
        || page_size == 4096 || page_size == 8192 || page_size == 16384
        || page_size == 32768 || page_size == 65536)
      return true;
  printf("Page size should be one of 512, 1024, 2048, 4096, 8192, 16384, 32768 or 65536\n");
  return false;
}

vector<string> read_csv_vector(char *csv, int col_count, bool is_name) {
  vector<string> out;
  uint8_t *p = (uint8_t *) csv;
  char name[100];
  int col_idx = 0;
  int col_len = 0;
  while (*p != '\0') {
    if (is_name && (*p == ' ' || *p == '-' || (col_len == 0 && *p >= '0' && *p <= '9'))) {
      p++;
      continue;
    }
    if ((*p >= ' ' && *p <= '~' && *p != ',') || *p > 127)
      name[col_len++] = *p;
    if (*p == ',') {
      if ((col_idx + 1) == col_count)
        break;
      out.push_back(string(name, col_len));
      col_idx++;
      col_len = 0;
    }
    p++;
  }
  out.push_back(string(name, col_len));
  return out;
}

void read_csv(char *out[], char *csv, int col_count, bool is_name) {
  uint8_t *p = (uint8_t *) csv;
  char name[100];
  int col_idx = 0;
  int col_len = 0;
  while (*p != '\0') {
    if (is_name && (*p == ' ' || *p == '-' || (col_len == 0 && *p >= '0' && *p <= '9'))) {
      p++;
      continue;
    }
    if ((*p >= ' ' && *p <= '~' && *p != ',') || *p > 127)
      name[col_len++] = *p;
    if (*p == ',') {
      if ((col_idx + 1) == col_count)
        break;
      out[col_idx] = new char[col_len + 1];
      memcpy(out[col_idx], name, col_len);
      out[col_idx][col_len] = 0;
      col_idx++;
      col_len = 0;
    }
    p++;
  }
  out[col_idx] = new char[col_len + 1];
  memcpy(out[col_idx], name, col_len);
  out[col_idx][col_len] = 0;
}

void release_parsed_csv(char *parsed_csv[], int col_count) {
  for (int i = 0; i < col_count; i++) {
     delete parsed_csv[i];
  }
}

int create_db(int argc, char *argv[]) {
  int page_size = atoi(argv[3]);
  if (!validate_page_size(page_size))
    return SQLT_RES_ERR;
  int col_count = atoi(argv[5]);
  int pk_col_count = atoi(argv[6]);
  unlink(argv[2]);
  cout << "Creating db " << argv[2] << ", table " << argv[4] << ", page size: " << page_size << endl;
  cout << "Col count: " << col_count << ", pk count: " << pk_col_count << ", Cols: " << argv[7] << endl;
  vector<string> col_names = read_csv_vector(argv[7], col_count, true);
  sqlite_index_blaster sqib(col_count, pk_col_count, col_names, argv[4], page_size, 400, argv[2]);
  return SQLT_RES_OK;
}

bool file_exists(const char *filename) {
  struct stat buffer;
  return (stat(filename, &buffer) == 0);
}

vector<string> empty_cols = {""};

int insert_db(int argc, char *argv[]) {
  if (!file_exists(argv[2]))
    cout << "File does not exist" << endl;
  int page_size = atoi(argv[3]);
  if (!validate_page_size(page_size))
    return SQLT_RES_ERR;
  int col_count = atoi(argv[4]);
  int pk_col_count = atoi(argv[5]);
  sqlite_index_blaster sqib(col_count, pk_col_count, empty_cols, "", page_size, 320, argv[2]);
  char *parsed_csv[col_count];
  for (int i = 0; i < argc-6; i++) {
    read_csv(parsed_csv, argv[6 + i], col_count, false);
    uint8_t rec[strlen(argv[6+i])+20];
    int rec_len = sqib.make_new_rec(rec, col_count, (const void **) parsed_csv);
    sqib.put(rec, -rec_len, NULL, 0);
    release_parsed_csv(parsed_csv, col_count);
  }
  return SQLT_RES_OK;
}

int read_db(int argc, char *argv[]) {
  if (!file_exists(argv[2]))
    cout << "File does not exist" << endl;
  int page_size = atoi(argv[3]);
  if (!validate_page_size(page_size))
    return SQLT_RES_ERR;
  int col_count = atoi(argv[4]);
  int pk_col_count = atoi(argv[5]);
  sqlite_index_blaster sqib(col_count, pk_col_count, empty_cols, "", page_size, 320, argv[2]);
  int rec_len = 10000;
  uint8_t *rec = (uint8_t *) malloc(rec_len);
  int val_len = 2000;
  uint8_t *val = (uint8_t *) malloc(val_len);
  char *parsed_csv[pk_col_count];
  read_csv(parsed_csv, argv[6], pk_col_count, false);
  uint8_t pk_rec[strlen(argv[6])+20];
  int pk_rec_len = sqib.make_new_rec(pk_rec, pk_col_count, (const void **) parsed_csv);
  if (sqib.get(pk_rec, -pk_rec_len, &rec_len, rec)) {
    for (int i = 0; i < col_count; i++) {
      val_len = sqib.read_col(i, rec, rec_len, val);
      printf("%.*s", val_len, val);
      if (i < col_count - 1)
        printf(",");
    }
    printf("\n");
  } else
    printf("Not found\n");
  free(val);
  free(rec);
  release_parsed_csv(parsed_csv, pk_col_count);
  return SQLT_RES_OK;
}

// Returns how many bytes the given integer will
// occupy if stored as a variable integer
int8_t get_vlen_of_uint32(uint32_t vint) {
    return vint > ((1 << 28) - 1) ? 5
        : (vint > ((1 << 21) - 1) ? 4 
        : (vint > ((1 << 14) - 1) ? 3
        : (vint > ((1 << 7) - 1) ? 2 : 1)));
}

int write_vint32(uint8_t *ptr, uint32_t vint) {
    int len = get_vlen_of_uint32(vint);
    for (int i = len - 1; i > 0; i--)
        *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
    *ptr = vint & 0x7F;
    return len;
}

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

#define CS_PRINTABLE 1
#define CS_ALPHA_ONLY 2
#define CS_NUMBER_ONLY 3
#define CS_ONE_PER_OCTET 4
#define CS_255_RANDOM 5
#define CS_255_DENSE 6
#define MIN_KEY_LEN 12
int64_t prepare_data(uint8_t **data_buf_ptr, int64_t data_sz, int KEY_LEN, int VALUE_LEN, int NUM_ENTRIES, int CHAR_SET, bool KEY_VALUE_VAR_LEN) {
    char k[KEY_LEN + 1];
    char v[VALUE_LEN + 1];
    int k_len = KEY_LEN;
    int v_len = VALUE_LEN;
    uint8_t *data_buf = *data_buf_ptr;
    int64_t ret = 0;
    srand(time(NULL));
    for (unsigned long l = 0; l < NUM_ENTRIES; l++) {

        if (CHAR_SET == CS_PRINTABLE) {
            for (int i = 0; i < KEY_LEN; i++)
                k[i] = 32 + (rand() % 95);
            k[KEY_LEN] = 0;
        } else if (CHAR_SET == CS_ALPHA_ONLY) {
            for (int i = 0; i < KEY_LEN; i++)
                k[i] = 97 + (rand() % 26);
            k[KEY_LEN] = 0;
        } else if (CHAR_SET == CS_NUMBER_ONLY) {
            for (int i = 0; i < KEY_LEN; i++)
                k[i] = 48 + (rand() % 10);
            k[KEY_LEN] = 0;
        } else if (CHAR_SET == CS_ONE_PER_OCTET) {
            for (int i = 0; i < KEY_LEN; i++)
                k[i] = ((rand() % 32) << 3) | 0x07;
            k[KEY_LEN] = 0;
        } else if (CHAR_SET == CS_255_RANDOM) {
            for (int i = 0; i < KEY_LEN; i++)
                k[i] = (rand() % 255);
            k[KEY_LEN] = 0;
            for (int i = 0; i < KEY_LEN; i++) {
                if (k[i] == 0)
                    k[i] = i + 1;
            }
        } else if (CHAR_SET == CS_255_DENSE) {
            KEY_LEN = 4;
            k[0] = (l >> 24) & 0xFF;
            k[1] = (l >> 16) & 0xFF;
            k[2] = (l >> 8) & 0xFF;
            k[3] = (l & 0xFF);
            if (k[0] == 0)
                k[0]++;
            if (k[1] == 0)
                k[1]++;
            if (k[2] == 0)
                k[2]++;
            if (k[3] == 0)
                k[3]++;
            k[4] = 0;
        }
        //cout << "Value: ";
        for (int i = 0; i < VALUE_LEN; i++) {
            v[VALUE_LEN - i - 1] = k[i % KEY_LEN];
            //cout << (char) k[i];
        }
        //cout << endl;
        v[VALUE_LEN] = 0;
        //itoa(rand(), v, 10);
        //itoa(rand(), v + strlen(v), 10);
        //itoa(rand(), v + strlen(v), 10);
        if (KEY_VALUE_VAR_LEN) {
            k_len = (rand() % KEY_LEN) + 1;
            v_len = (rand() % VALUE_LEN) + 1;
            if (k_len < MIN_KEY_LEN)
                k_len = MIN_KEY_LEN;
            k[k_len] = 0;
            v[v_len] = 0;
        }
        // if (l == 0)
        //     printf("key: %.*s, value: %.*s\n", KEY_LEN, k, VALUE_LEN, v);
        ret += write_vint32(data_buf + ret, k_len);
        memcpy(data_buf + ret, k, k_len);
        ret += k_len;
        data_buf[ret++] = 0;
        ret += write_vint32(data_buf + ret, v_len);
        memcpy(data_buf + ret, v, v_len);
        ret += v_len;
        data_buf[ret++] = 0;
        if (ret + KEY_LEN + VALUE_LEN + 1000 < data_sz) {
          data_sz += (32 * 1024 * 1024);
          //printf("New data size: %d\n", data_sz / 1024 / 1024);
          data_buf = (uint8_t *) realloc(data_buf, data_sz);
          *data_buf_ptr = data_buf;
        }

    }
    return ret;
}

void check_value(const char *key, int key_len, const char *val, int val_len,
      const char *returned_value, int returned_len, int& cmp) {
      int d = util::compare((const uint8_t *) val, val_len, (const uint8_t *) returned_value, returned_len);
      if (d != 0) {
        cmp++;
          //printf("cmp: %.*s==========%.*s--------->%.*s\n", key_len, key, val_len, val, returned_len, returned_value);
          //cout << cmp << ":" << (char *) key << "=========="
          //        << val << "----------->" << returned_value << endl;
      }
}

vector<string> census_col_names = {"cum_prop100k", "rank", "name", "year", "count", "prop100k", "pctwhite", "pctblack", "pctapi", "pctaian", "pct2prace", "pcthispanic"};
const void *census_col_values[12];
const uint8_t census_col_types[] = {SQLT_TYPE_REAL, SQLT_TYPE_INT32, SQLT_TYPE_TEXT, SQLT_TYPE_INT32, SQLT_TYPE_INT32, SQLT_TYPE_REAL,
                               SQLT_TYPE_REAL, SQLT_TYPE_REAL, SQLT_TYPE_REAL, SQLT_TYPE_REAL, SQLT_TYPE_REAL, SQLT_TYPE_REAL};

bool test_census(int page_size, int cache_size, const char *filename) {

  unlink(filename);
  sqlite_index_blaster *sqib = new sqlite_index_blaster(12, 3, census_col_names, "surnames", page_size, cache_size, filename);
  ifstream file("sample_data/census.txt");
  if (file.is_open()) {
      string line;
      while (getline(file, line)) {
        stringstream ss(line);
        string s;
        int col_idx = 0;
        uint32_t year, rank, count;
        double cum_prop100k, prop100k, pctwhite, pctblack, pctapi, pctaian, pct2prace, pcthispanic;
        string name;
        while (getline(ss, s, '|')) {
          switch (col_idx) {
            case 0:
              year = atoi(s.c_str());
              // cout << year << ",";
              break;
            case 1:
              name = s;
              // cout << name << ",";
              break;
            case 2:
              rank = atoi(s.c_str());
              // cout << rank << ",";
              break;
            case 3:
              count = atoi(s.c_str());
              // cout << count << ",";
              break;
            case 4:
              prop100k = atof(s.c_str());
              // cout << prop100k << ",";
              break;
            case 5:
              cum_prop100k = atof(s.c_str());
              //cout << cum_prop100k;
              break;
            case 6:
              pctwhite = atof(s.c_str());
              //cout << pctwhite;
              break;
            case 7:
              pctblack = atof(s.c_str());
              //cout << pctblack;
              break;
            case 8:
              pctapi = atof(s.c_str());
              //cout << pctapi;
              break;
            case 9:
              pctaian = atof(s.c_str());
              //cout << pctaian;
              break;
            case 10:
              pct2prace = atof(s.c_str());
              //cout << pct2prace;
              break;
            case 11:
              pcthispanic = atof(s.c_str());
              //cout << pcthispanic;
              break;
          }
          col_idx++;
        }
        if (col_idx < 12)
          pcthispanic = 0;
        //cout << endl;
        uint8_t rec[line.length() + 500];
        census_col_values[0] = &cum_prop100k;
        census_col_values[1] = &rank;
        census_col_values[2] = (const void *) name.c_str();
        census_col_values[3] = &year;
        census_col_values[4] = &count;
        census_col_values[5] = &prop100k;
        census_col_values[6] = &pctwhite;
        census_col_values[7] = &pctblack;
        census_col_values[8] = &pctapi;
        census_col_values[9] = &pctaian;
        census_col_values[10] = &pct2prace;
        census_col_values[11] = &pcthispanic;
        int rec_len = sqib->make_new_rec(rec, 12, census_col_values, NULL, census_col_types);
        sqib->put(rec, -rec_len, NULL, 0);
      }
      file.close();
  }
  delete sqib;
  char cmd[500];
  sprintf(cmd, "sqlite3 %s \"pragma integrity_check\"", filename);
  int sysret = system(cmd);
  if (sysret) {
    cout << "Integrity check failed" << endl;
    return false;
  }
  sprintf(cmd, "sqlite3 %s \"select cum_prop100k, rank, name, year, count, prop100k, pctwhite, pctblack, pctapi,"
                 " pctaian, pct2prace, pcthispanic from surnames order by name, rank\" > census.txt", filename);
  sysret = system(cmd);
  strcpy(cmd, "cmp census.txt sample_data/census_cmp.txt");
  sysret = system(cmd);
  if (sysret) {
    cout << filename << " compare failed" << endl;
    return false;
  }
      
  return true;

}

bool test_census() {
  for (int i = 9; i < 17; i++) {
    char filename[30];
    int page_size = 1 << i;
    sprintf(filename, "census_%d.db", page_size);
    cout << "Testing " << filename << endl;
    bool ret = test_census(page_size, 4096, filename);
    if (!ret)
      return false;
  }
  return true;
}

vector<string> baby_col_names = {"year", "state", "name", "total_babies", "primary_sex", "primary_sex_ratio", "per_100k_in_state"};
const uint8_t baby_col_types[] = {SQLT_TYPE_INT32, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT32, SQLT_TYPE_TEXT, SQLT_TYPE_REAL, SQLT_TYPE_REAL};
const void *baby_col_values[7];

bool test_babynames(int page_size, int cache_size, const char *filename) {

  unlink(filename);
  sqlite_index_blaster *sqib = new sqlite_index_blaster(7, 3, baby_col_names,
                                  "gendered_names", page_size, cache_size, filename);
  ifstream file("sample_data/babynames.txt");
  if (file.is_open()) {
      string line;
      while (getline(file, line)) {
        stringstream ss(line);
        string s;
        int col_idx = 0;
        uint32_t year, total_babies;
        double per_100k_in_state, primary_sex_ratio;
        string state, name, primary_sex;
        while (getline(ss, s, '|')) {
          switch (col_idx) {
            case 0:
              year = atoi(s.c_str());
              // cout << year << ",";
              break;
            case 1:
              state = s;
              // cout << state << ",";
              break;
            case 2:
              name = s;
              // cout << name << ",";
              break;
            case 3:
              total_babies = atoi(s.c_str());
              // cout << total_babies<< ",";
              break;
            case 4:
              primary_sex = s;
              // cout << primary_sex << ",";
              break;
            case 5:
              primary_sex_ratio = atof(s.c_str());
              // cout << primary_sex_ratio << ",";
              break;
            case 6:
              per_100k_in_state = atof(s.c_str());
              //cout << per_100k_in_state;
              break;
          }
          col_idx++;
        }
        if (col_idx < 7)
            per_100k_in_state = 0;
        //cout << endl;
        uint8_t rec[line.length() + 100];
        baby_col_values[0] = &year;
        baby_col_values[1] = (const void *) state.c_str();
        baby_col_values[2] = (const void *) name.c_str();
        baby_col_values[3] = &total_babies;
        baby_col_values[4] = (const void *) primary_sex.c_str();
        baby_col_values[5] = &primary_sex_ratio;
        baby_col_values[6] = &per_100k_in_state;
        int rec_len = sqib->make_new_rec(rec, 7, baby_col_values, NULL, baby_col_types);
        sqib->put(rec, -rec_len, NULL, 0);
      }
      file.close();
  }
  delete sqib;
  char cmd[100];
  sprintf(cmd, "sqlite3 %s \"pragma integrity_check\"", filename);
  int sysret = system(cmd);
  if (sysret) {
    cout << "Integrity check failed" << endl;
    return false;
  }
  sprintf(cmd, "sqlite3 %s \"select * from gendered_names order by state,name\" > babynames.txt", filename);
  sysret = system(cmd);
  strcpy(cmd, "cmp babynames.txt sample_data/babynames_cmp.txt");
  sysret = system(cmd);
  if (sysret) {
    cout << filename << " compare failed" << endl;
    return false;
  }
      
  return true;

}

bool test_babynames() {
  for (int i = 9; i < 17; i++) {
    char filename[30];
    int page_size = 1 << i;
    sprintf(filename, "babynames_%d.db", page_size);
    cout << "Testing " << filename << endl;
    bool ret = test_babynames(page_size, 4096, filename);
    if (!ret)
      return false;
  }
  return true;
}

vector<string> const_kv = {"key", "value"};

bool test_random_data(int page_size, long start_count, int cache_size, char *filename) {
  unlink(filename);
  int U = page_size - 5;
  int M = ((U-12)*32/255)-23-10;
  int KEY_LEN = M;
  int VALUE_LEN = page_size * 2 + M;
  int NUM_ENTRIES = start_count * 512 / page_size;
  int64_t data_alloc_sz = 64 * 1024 * 1024;
  uint8_t *data_buf = (uint8_t *) malloc(data_alloc_sz);
  int64_t data_sz = prepare_data(&data_buf, data_alloc_sz, KEY_LEN, VALUE_LEN, NUM_ENTRIES, 1, true);
  cout << "Testing page size: " << page_size << ", count: " << NUM_ENTRIES
       << ", Data size: " << data_sz / 1000 << "kb" << ", Cache size: " << cache_size << "kb" << endl;
  sqlite_index_blaster sqib(2, 1, const_kv, "imain", page_size, cache_size, filename);
  for (int64_t pos = 0; pos < data_sz; pos++) {
      int8_t vlen;
      uint32_t key_len = read_vint32(data_buf + pos, &vlen);
      pos += vlen;
      uint32_t value_len = read_vint32(data_buf + pos + key_len + 1, &vlen);
      sqib.put((char *) data_buf + pos, key_len, (char *) data_buf + pos + key_len + vlen + 1, value_len);
      pos += key_len + value_len + vlen + 1;
  }
  int cmp = 0;
  int ctr = 0;
  int null_ctr = 0;
  char value_buf[VALUE_LEN + 1];
  for (int64_t pos = 0; pos < data_sz; pos++) {
      int len = VALUE_LEN;
      int8_t vlen;
      uint32_t key_len = read_vint32(data_buf + pos, &vlen);
      pos += vlen;
      uint32_t value_len = read_vint32(data_buf + pos + key_len + 1, &vlen);
      bool is_found = sqib.get((char *) data_buf + pos, key_len, &len, value_buf);
      if (!is_found)
        null_ctr++;
      check_value((char *) data_buf + pos, key_len,
              (char *) data_buf + pos + key_len + vlen + 1, value_len, value_buf, len, cmp);
      pos += key_len + value_len + vlen + 1;
      ctr++;
  }
  free(data_buf);
  if (cmp > 0 || null_ctr > 0) {
    cout << "Failed! Null: " << null_ctr << ", Cmp: " << cmp << endl;
    return false;
  }

  return true;
}

bool test_random_data(long start_count, int cache_size) {
  for (int i = 9; i < 17; i++) {
    char filename[30];
    int page_size = 1 << i;
    sprintf(filename, "test_%d.db", page_size);
    bool ret = test_random_data(page_size, start_count, cache_size, filename);
    if (!ret)
      return false;
    char cmd[100];
    sprintf(cmd, "sqlite3 %s \"pragma integrity_check\"", filename);
    int sysret = system(cmd);
    if (sysret) {
      cout << "Integrity check failed" << endl;
      return false;
    }
  }
  return true;
}

int main(int argc, char *argv[]) {

  if (argc == 8 && strcmp(argv[1], "-c") == 0) {
    create_db(argc, argv);
  } else
  if (argc > 6 && strcmp(argv[1], "-i") == 0) {
    insert_db(argc, argv);
  } else
  if (argc == 7 && strcmp(argv[1], "-r") == 0) {
    read_db(argc, argv);
  } else
  if (argc == 2 && strcmp(argv[1], "-t") == 0) {
    int ret = 1;
    // test with lowest possible cache size
    if (test_random_data(150000, 256)) {
      // test file > 1gb
      //if (test_random_data(1400000, 64 * 1024)) {
        if (test_babynames()) {
          if (test_census()) {
            cout << "All tests ok" << endl;
            ret = 0;
          }
        }
      //}
    }
    return ret;
  } else
    print_usage();

  return 0;

}
#endif
