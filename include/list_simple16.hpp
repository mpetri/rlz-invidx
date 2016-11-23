#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "simple16.h"

template<bool t_dgap>
struct list_simple16 {
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,const list_meta_data& lm) {
        static FastPForLib::Simple16<0> s16coder;
        if(t_dgap) utils::dgap_list(buf,lm.list_len);
        out.expand_if_needed(1024+2*32*buf.size());
        out.align64();
        uint32_t* out32 = (uint32_t*) out.cur_data();
        const uint32_t* in = buf.data();
        size_t written_ints = buf.size();
        s16coder.encodeArray(in,lm.list_len,out32,written_ints);
        size_t bits_written = written_ints * sizeof(uint32_t)*8;
        out.skip(bits_written);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,const list_meta_data& lm) {
        static FastPForLib::Simple16<0> s16coder;
        in.align64();
        const uint32_t* in32 = (const uint32_t*) in.cur_data();
        uint32_t* out = buf.data();
        size_t read_ints = lm.list_len;
        s16coder.decodeArray(in32,lm.list_len,out,read_ints);
        size_t bits_read = read_ints * sizeof(uint32_t)*8;
        in.skip(bits_read);
        if(t_dgap) utils::undo_dgap_list(buf,lm.list_len);
    }
};
