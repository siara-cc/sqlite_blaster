#include "sqlite_index_blaster.h"

int main() {
    sqlite ix(2, 1, (const char *[]) {"key", "value"}, "idx1", 4096, 32, "hw.db");
    ix.put("hello", 5, "world", 5);
}
