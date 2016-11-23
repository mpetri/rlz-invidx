#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

template<bool t_dgap>
struct list_vbyte {
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,const list_meta_data& lm) {
        static coder::vbyte vcoder;
        if(t_dgap) {
            size_t prev = buf[0];
            for(size_t i=1;i<lm.list_len;i++) {
                uint32_t cur = buf[i];
                uint32_t gap = cur - prev;
                prev = cur;
                buf[i] = gap;
            }
        }
        vcoder.encode(out,buf.data(),lm.list_len);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,const list_meta_data& lm) {
        static coder::vbyte vcoder;
        vcoder.decode(in,buf.data(),lm.list_len);
        if(t_dgap) {
            size_t prev = buf[0];
            for(size_t i=1;i<lm.list_len;i++) {
                uint32_t cur = buf[i];
                buf[i] = cur + prev;
                prev = buf[i];
            }
        }
    }
};
