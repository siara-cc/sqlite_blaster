# Sqlite Index Blaster

[![C/C++ CI](https://github.com/siara-cc/sqlite_blaster/actions/workflows/c-cpp.yml/badge.svg)](https://github.com/siara-cc/sqlite_blaster/actions/workflows/c-cpp.yml)

This library provides API for creating huge Sqlite indexes at breakneck speeds for millions of records much faster than the official SQLite library by leaving out crash recovery.

This repo exploits a [lesser known feature of the Sqlite database file format](https://www.sqlite.org/withoutrowid.html) to store records as key-value pairs or documents or regular tuples.

# Statement of need

There are a number of choices available for fast insertion of records, such as Rocks DB, LMDB and MongoDB but even they are slow due to overheads of using logs or journals for providing durability.  These overheads are significant for indexing huge datasets.

This library was created for inserting/updating billions of entries for arriving at word/phrase frequencies for building dictionaries for the [Unishox](https://github.com/siara-cc/Unishox) project using publicly available texts and conversations.

Furthermore, the other choices don't have the same number of IDEs or querying abilities of the most popular Sqlite data format.

# Applications

- Lightning fast index creation for huge datasets
- Fast database indexing for embedded systems
- Fast data set creation and loading for Data Science and Machine Learning

# Performance

The performance of this repo was compared with the Sqlite official library, LMDB and RocksDB under similar conditions of CPU, RAM and NVMe disk and the results are shown below:

![Performance](misc/performance.png?raw=true)

RocksDB performs much better than other choices and performs consistently for over billion entries, but it is quite slow initially.

The chart data can be found [here](https://github.com/siara-cc/sqlite_blaster/blob/master/SqliteBlasterPerformanceLineChart.xlsx?raw=true).

# Building and running tests

Clone this repo and run `make` to build the executable `test_sqlite_blaster` for testing.  To run tests, invoke with `-t` parameter from shell console.

```sh
make
./test_sqlite_blaster -t
```

# Getting started

Essentially, the library provides 2 methods `put()` and `get()` for inserting and retrieving records.  Shown below are examples of how this library can be used to create a key-value store, or a document store or a regular table.

Note: The cache size is used as 40kb in these examples, but in real life 32mb or 64mb would be ideal.  The higher this number, better the performance.

## Creating a Key-Value store

In this mode, a table is created with just 2 columns, `key` and `value` as shown below:

```c++
#include "sqlite_index_blaster.h"
#include <string>
#include <vector>

int main() {

    std::vector<std::string> col_names = {"key", "value"}; // -std >= c++11
    sqlite_index_blaster sqib(2, 1, col_names, "kv_index", 4096, 40, "kv_idx.db");
    sqib.put_string("hello", "world");
    return 0;
    // db file is flushed and closed when sqib is destroyed

}
```

A file `kv_idx.db` is created and can be verified by opening it using `sqlite3` official console program:

```sh
sqlite3 kv_idx.db ".dump"
```

and the output would be:

```sql
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE kv_index (key, value, PRIMARY KEY (key)) WITHOUT ROWID;
INSERT INTO kv_index VALUES('hello','world');
COMMIT;
```

To retrieve the inserted values, use `get` method as shown below

```c++
#include "sqlite_index_blaster.h"
#include <string>
#include <vector>

int main() {
    std::vector<std::string> col_names = {"key", "value"}; // -std >= c++11
    sqlite_index_blaster sqib(2, 1, col_names, "kv_index", 4096, 40, "kv_idx.db");
    sqib.put_string("hello", "world");
    std::cout << "Value of hello is " << sqib.get_string("hello", "not_found") << std::endl;
    return 0;
}
```

## Creating a Document store

In this mode, a table is created with just 2 columns, `key` and `doc` as shown below:

```c++
#include "sqlite_index_blaster.h"
#include <string>
#include <vector>

const char * json1 = "{\"name\": \"Alice\", \"age\": 25, \"email\": \"alice@example.com\"}";
const char * json2 = "{\"name\": \"George\", \"age\": 32, \"email\": \"george@example.com\"}";

int main() {
    std::vector<std::string> col_names = {"key", "doc"}; // -std >= c++11
    sqlite_index_blaster sqib(2, 1, col_names, "doc_index", 4096, 40, "doc_store.db");
    sqib.put_string("primary_contact", json1);
    sqib.put_string("secondary_contact", json2);
    return 0;
}
```

The index is created as `doc_store.db` and the json values can be queried using `sqlite3` console as shown below:

```sql
SELECT json_extract(doc, '$.email') AS email
FROM doc_index
WHERE key = 'primary_contact';
```

## Creating a regular table

This repo can be used to create regular tables with primary key(s) as shown below:

```c++
#include <cmath>
#include <string>
#include <vector>

#include "sqlite_index_blaster.h"

const uint8_t col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_INT8, SQLT_TYPE_INT8, SQLT_TYPE_INT8, SQLT_TYPE_INT8, SQLT_TYPE_REAL};

int main() {

    std::vector<std::string> col_names = {"student_name", "age", "maths_marks", "physics_marks", "chemistry_marks", "average_marks"};
    sqlite_index_blaster sqib(6, 2, col_names, "student_marks", 4096, 40, "student_marks.db");

    int8_t maths, physics, chemistry, age;
    double average;
    uint8_t rec_buf[500];
    int rec_len;

    age = 19; maths = 80; physics = 69; chemistry = 98; average = round((maths + physics + chemistry) * 100 / 3) / 100;
    rec_len = sqib.make_new_rec(rec_buf, 6, (const void *[]) {"Robert", &age, &maths, &physics, &chemistry, &average}, NULL, col_types);
    sqib.put(rec_buf, -rec_len, NULL, 0);

    age = 20; maths = 82; physics = 99; chemistry = 83; average = round((maths + physics + chemistry) * 100 / 3) / 100;
    rec_len = sqib.make_new_rec(rec_buf, 6, (const void *[]) {"Barry", &age, &maths, &physics, &chemistry, &average}, NULL, col_types);
    sqib.put(rec_buf, -rec_len, NULL, 0);

    age = 23; maths = 84; physics = 89; chemistry = 74; average = round((maths + physics + chemistry) * 100 / 3) / 100;
    rec_len = sqib.make_new_rec(rec_buf, 6, (const void *[]) {"Elizabeth", &age, &maths, &physics, &chemistry, &average}, NULL, col_types);
    sqib.put(rec_buf, -rec_len, NULL, 0);

    return 0;
}
```

The index is created as `student_marks.db` and the data can be queried using `sqlite3` console as shown below:

```sql
sqlite3 student_marks.db "select * from student_marks"
Barry|20|82|99|83|88.0
Elizabeth|23|84|89|74|82.33
Robert|19|80|69|98|82.33
```

## Constructor parameters of sqlite_index_blaster class

1. `total_col_count` - Total column count in the index
2. `pk_col_count` - Number of columns to use as key.  These columns have to be positioned at the beginning
3. `col_names` - Column names to create the table
4. `tbl_name` - Table (clustered index) name
5. `block_sz` - Page size (must be one of 512, 1024, 2048, 4096, 8192, 16384, 32768 or 65536)
6. `cache_sz` - Size of LRU cache in kilobytes. 32 or 64 mb would be ideal. Higher values lead to better performance
7. `fname` - Name of the Sqlite database file

# Console Utility for playing around

`test_sqlite_blaster` also has rudimentary ability to create, insert and query databases as shown below.  However this is just for demonstration.

```c++
./test_sqlite_blaster -c movie.db 4096 movie_list 3 1 Film,Genre,Studio
```

To insert records, use -i as shown below:

```c++
./test_sqlite_blaster -i movie.db 4096 3 1 "Valentine's Day,Comedy,Warner Bros." "Sex and the City,Comedy,Disney" "Midnight in Paris,Romance,Sony"
```

This inserts 3 records.  To retrieve inserted records, run:

```c++
./test_sqlite_blaster -r movie.db 4096 3 1 "Valentine's Day"
```
and the output would be:
```
Valentine's Day,Comedy,Warner Bros.
```

# Limitations

- No crash recovery. If the insertion process is interruped, the database would be unusable.
- The record length cannot change for update. Updating with lesser or greater record length is not implemented yet.
- Deletes are not implemented yet.  This library is intended primarily for fast inserts.
- Support for concurrent inserts not implemented yet.
- The regular ROWID table of Sqlite is not implemented.
- Only the equivalent of memcmp is used to index records.  The order in which keys are ordered may not match with official Sqlite lib for non-ASCII char sets.
- Key lengths are limited depending on page size as shown in the table below.  This is just because the source code does not implement support for longer keys. However, this is considered sufficient for most practical purposes.

  | **Page Size** | **Max Key Length** |
  | ------------- | ------------------ |
  | 512 | 35 |
  | 1024 | 99 |
  | 2048 | 227 |
  | 4096 | 484 |
  | 8192 | 998 |
  | 16384 | 2026 |
  | 32768 | 4082 |
  | 65536 | 8194 |

# Stability

This code has been tested with more than 200 million records, so it is expected to be quite stable, but bear in mind that this is so fast because there is no crash recovery.

So this repo is best suited for one time inserts of large datasets.  It may be suitable for power backed systems such as those hosted in Cloud and battery backed systems.

# License

Sqlite Index Blaster and its command line tools are dual-licensed under the MIT license and the AGPL-3.0.  Users may choose one of the above.

- The MIT License
- The GNU Affero General Public License v3 (AGPL-3.0)

# Support

If you face any problem, create issue in this website, or write to the author (Arundale Ramanathan) at arun@siara.cc.
