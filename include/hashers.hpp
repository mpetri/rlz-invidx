#pragma once

#include <future>
#include <thread>

/* The MIT License

   Copyright (C) 2012 Zilong Tan (eric.zltan@gmail.com)
*/
// Compression function for Merkle-Damgard construction.
// This function is generated using the framework provided.
#define mix(h) ({					\
			(h) ^= (h) >> 23;		\
			(h) *= 0x2127599bf4325c37ULL;	\
			(h) ^= (h) >> 47; })

template<uint32_t len>
inline uint64_t fasthash64(const void* buf,uint64_t seed)
{
    const uint64_t m = 0x880355f21e6d1965ULL;
    const uint64_t* pos = (const uint64_t*)buf;
    const uint64_t* end = pos + (len / 8);
    const unsigned char* pos2;
    uint64_t h = seed ^ (len * m);
    uint64_t v;

    while (pos != end) {
        v = *pos++;
        h ^= mix(v);
        h *= m;
    }

    pos2 = (const unsigned char*)pos;
    v = 0;

    switch (len & 7) {
    case 7:
        v ^= (uint64_t)pos2[6] << 48;
    case 6:
        v ^= (uint64_t)pos2[5] << 40;
    case 5:
        v ^= (uint64_t)pos2[4] << 32;
    case 4:
        v ^= (uint64_t)pos2[3] << 24;
    case 3:
        v ^= (uint64_t)pos2[2] << 16;
    case 2:
        v ^= (uint64_t)pos2[1] << 8;
    case 1:
        v ^= (uint64_t)pos2[0];
        h ^= mix(v);
        h *= m;
    }

    return mix(h);
}


template<>
inline uint64_t fasthash64<8>(const void* buf,uint64_t seed)
{
    const uint64_t m = 0x880355f21e6d1965ULL;
    const uint64_t* pos = (const uint64_t*)buf;
    uint64_t h = seed ^ (8 * m);
    uint64_t v = pos[0];
    h ^= mix(v);
    h *= m;
    return mix(h);
}

template<>
inline uint64_t fasthash64<16>(const void* buf,uint64_t seed)
{
    const uint64_t m = 0x880355f21e6d1965ULL;
    const uint64_t* pos = (const uint64_t*)buf;
    uint64_t h = seed ^ (16 * m);
    uint64_t v = pos[0];
    h ^= mix(v);
    h *= m;
    v = pos[1];
    h ^= mix(v);
    h *= m;
    return mix(h);
}

template <uint32_t t_block_size>
struct fixed_hasher {
    const uint64_t seed = 4711;
    const uint64_t buf_start_pos = t_block_size - 1;
    std::array<uint8_t, t_block_size * 1024 * 1024> buf;
    uint64_t overflow_offset = (t_block_size * 1024 * 1024) - (t_block_size - 1);
    uint64_t cur_pos_in_buf = buf_start_pos;

    inline uint64_t compute_current_hash()
    {
        return fasthash64<t_block_size>(buf.data() + cur_pos_in_buf - buf_start_pos,seed);
    }

    inline uint64_t compute_hash(const uint8_t* ptr) {

        return fasthash64<t_block_size>(ptr,seed);
    }

    inline uint64_t update(uint8_t sym)
    {
        if (cur_pos_in_buf == buf.size()) {
            memcpy(buf.data() + overflow_offset, buf.data(), t_block_size - 1);
            cur_pos_in_buf = buf_start_pos;
        }
        buf[cur_pos_in_buf] = sym;
        auto hash = compute_current_hash();
        cur_pos_in_buf++;
        return hash;
    }

    inline uint64_t update_and_hash(uint8_t sym) {
        return update(sym);
    }

    static std::string type()
    {
        return "fixed_hasher";
    }
};

template <uint32_t t_block_size>
struct fixed_hasher_lazy {
    const uint64_t seed = 4711;
    const uint64_t buf_start_pos = t_block_size - 1;
    std::array<uint8_t, t_block_size * 1024 * 1024> buf;
    uint64_t overflow_offset = (t_block_size * 1024 * 1024) - (t_block_size - 1);
    uint64_t cur_pos_in_buf = buf_start_pos;

    inline uint64_t compute_current_hash()
    {
        return fasthash64<t_block_size>(buf.data() + cur_pos_in_buf - buf_start_pos,seed);
    }

    inline void update(uint8_t sym)
    {
        if (cur_pos_in_buf == buf.size()) {
            memcpy(buf.data() + overflow_offset, buf.data(), t_block_size - 1);
            cur_pos_in_buf = buf_start_pos;
        }
        buf[cur_pos_in_buf] = sym;
        cur_pos_in_buf++;
    }

    inline uint64_t update_and_hash(uint8_t sym)
    {
        if (cur_pos_in_buf == buf.size()) {
            memcpy(buf.data() + overflow_offset, buf.data(), t_block_size - 1);
            cur_pos_in_buf = buf_start_pos;
        }
        buf[cur_pos_in_buf] = sym;
        auto hash = compute_current_hash();
        cur_pos_in_buf++;
        return hash;
    }


    static std::string type()
    {
        return "fixed_hasher_lazy";
    }
};