#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

template<bool t_prefix>
struct list_interp {
    static std::string name() {
        return "interp";
    }
    
    static std::string type() {
        return "interp(prefix="+std::to_string(t_prefix)+")";
    } 
    
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,size_t n,size_t universe) {
        static coder::interpolative interp_coder;
        if(t_prefix) utils::prefixsum_list(buf,n);
        const uint32_t* in = buf.data();
        interp_coder.encode(out,in,n,universe);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,size_t n,size_t universe) {
        static coder::interpolative interp_coder;
        auto out = buf.data();
        interp_coder.decode(in,out,n,universe);
        if(t_prefix) utils::undo_prefixsum_list(buf,n);
    }
};

