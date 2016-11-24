#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

template<bool t_prefix>
struct list_ef {
    static std::string name() {
        return "ef";
    }
    
    static std::string type() {
        return "ef(prefix="+std::to_string(t_prefix)+")";
    } 
    
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,size_t n,size_t universe) {
        static coder::elias_fano ef_coder;
        if(t_prefix) utils::prefixsum_list(buf,n);
        const uint32_t* in = buf.data();
        ef_coder.encode(out,in,n,universe);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,size_t n,size_t universe) {
        static coder::elias_fano ef_coder;
        auto out = buf.data();
        ef_coder.decode(in,out,n,universe);
        if(t_prefix) utils::undo_prefixsum_list(buf,n);
    }
};

