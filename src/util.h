#ifndef SIARA_UTIL_H
#define SIARA_UTIL_H

#include <stdint.h>
#include <stdlib.h>

class util {
  public:
    // Returns how many bytes the given integer will
    // occupy if stored as a variable integer
    static int8_t get_vlen_of_uint16(uint16_t vint) {
        return vint > 16383 ? 3 : (vint > 127 ? 2 : 1);
    }

    // Returns how many bytes the given integer will
    // occupy if stored as a variable integer
    static int8_t get_vlen_of_uint32(uint32_t vint) {
        return vint > ((1 << 28) - 1) ? 5
            : (vint > ((1 << 21) - 1) ? 4 
            : (vint > ((1 << 14) - 1) ? 3
            : (vint > ((1 << 7) - 1) ? 2 : 1)));
    }

    // Returns how many bytes the given integer will
    // occupy if stored as a variable integer
    static int8_t get_vlen_of_uint64(uint64_t vint) {
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
    static void write_uint8(uint8_t *ptr, uint8_t input) {
        ptr[0] = input;
    }

    // Stores the given uint16_t in the given location
    // in big-endian sequence
    static void write_uint16(uint8_t *ptr, uint16_t input) {
        ptr[0] = input >> 8;
        ptr[1] = input & 0xFF;
    }

    // Stores the given int24_t in the given location
    // in big-endian sequence
    static void write_int24(uint8_t *ptr, uint32_t input) {
        int i = 3;
        ptr[1] = ptr[2] = 0;
        *ptr = (input >> 24) & 0x80;
        while (i--)
            *ptr++ |= ((input >> (8 * i)) & 0xFF);
    }

    // Stores the given uint32_t in the given location
    // in big-endian sequence
    static void write_uint32(uint8_t *ptr, uint32_t input) {
        int i = 4;
        while (i--)
            *ptr++ = (input >> (8 * i)) & 0xFF;
    }

    // Stores the given int64_t in the given location
    // in big-endian sequence
    static void write_int48(uint8_t *ptr, uint64_t input) {
        int i = 7;
        memset(ptr + 1, '\0', 7);
        *ptr = (input >> 56) & 0x80;
        while (i--)
            *ptr++ |= ((input >> (8 * i)) & 0xFF);
    }

    // Stores the given uint64_t in the given location
    // in big-endian sequence
    static void write_uint64(uint8_t *ptr, uint64_t input) {
        int i = 8;
        while (i--)
            *ptr++ = (input >> (8 * i)) & 0xFF;
    }

    // Stores the given uint16_t in the given location
    // in variable integer format
    static int write_vint16(uint8_t *ptr, uint16_t vint) {
        int len = get_vlen_of_uint16(vint);
        for (int i = len - 1; i > 0; i--)
            *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
        *ptr = vint & 0x7F;
        return len;
    }

    // Stores the given uint32_t in the given location
    // in variable integer format
    static int write_vint32(uint8_t *ptr, uint32_t vint) {
        int len = get_vlen_of_uint32(vint);
        for (int i = len - 1; i > 0; i--)
            *ptr++ = 0x80 + ((vint >> (7 * i)) & 0x7F);
        *ptr = vint & 0x7F;
        return len;
    }

    // Stores the given uint64_t in the given location
    // in variable integer format
    static int write_vint64(uint8_t *ptr, uint64_t vint) {
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
    static uint8_t read_uint8(const uint8_t *ptr) {
        return *ptr;
    }

    // Reads and returns big-endian uint16_t
    // at a given memory location
    static uint16_t read_uint16(const uint8_t *ptr) {
        return (*ptr << 8) + ptr[1];
    }

    // Reads and returns big-endian int24_t
    // at a given memory location
    static int32_t read_int24(const uint8_t *ptr) {
        uint32_t ret;
        ret = ((uint32_t)(*ptr & 0x80)) << 24;
        ret |= ((uint32_t)(*ptr++ & 0x7F)) << 16;
        ret |= ((uint32_t)*ptr++) << 8;
        ret += *ptr;
        return ret;
    }

    // Reads and returns big-endian uint24_t
    // at a given memory location
    static uint32_t read_uint24(const uint8_t *ptr) {
        uint32_t ret;
        ret = ((uint32_t)*ptr++) << 16;
        ret += ((uint32_t)*ptr++) << 8;
        ret += *ptr;
        return ret;
    }

    // Reads and returns big-endian uint32_t
    // at a given memory location
    static uint32_t read_uint32(const uint8_t *ptr) {
        uint32_t ret;
        ret = ((uint32_t)*ptr++) << 24;
        ret += ((uint32_t)*ptr++) << 16;
        ret += ((uint32_t)*ptr++) << 8;
        ret += *ptr;
        return ret;
    }

    // Reads and returns big-endian int48_t
    // at a given memory location
    static int64_t read_int48(const uint8_t *ptr) {
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
    static uint64_t read_uint48(const uint8_t *ptr) {
        uint64_t ret = 0;
        int len = 6;
        while (len--)
            ret += (*ptr++ << (8 * len));
        return ret;
    }

    // Reads and returns big-endian uint64_t
    // at a given memory location
    static uint64_t read_uint64(const uint8_t *ptr) {
        uint64_t ret = 0;
        int len = 8;
        while (len--)
            ret += (*ptr++ << (8 * len));
        return ret;
    }

    // Reads and returns variable integer
    // from given location as uint16_t
    // Also returns the length of the varint
    static uint16_t read_vint16(const uint8_t *ptr, int8_t *vlen) {
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
    static uint32_t read_vint32(const uint8_t *ptr, int8_t *vlen) {
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
    static int64_t float_to_double(const void *val) {
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

    static double read_double(const uint8_t *data) {
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

    static int compare(const uint8_t *v1, int len1, const uint8_t *v2,
            int len2, int k = 0) {
        int lim = (len2 < len1 ? len2 : len1);
        while (k < lim) {
            uint8_t c1 = v1[k];
            uint8_t c2 = v2[k];
            k++;
            if (c1 < c2)
                return -k;
            else if (c1 > c2)
                return k;
        }
        if (len1 == len2)
            return 0;
        k++;
        return (len1 < len2 ? -k : k);
    }

};

#endif
