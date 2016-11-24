#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

template<bool t_dgap>
struct list_u32 {
    static std::string name() {
        return "u32";
    }
    
    static std::string type() {
        return "u32";
    } 
    
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::fixed<32> u32coder;
        if(t_dgap) utils::dgap_list(buf,n);
        u32coder.encode(out,buf.data(),n);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::fixed<32> u32coder;
        u32coder.decode(in,buf.data(),n);
        if(t_dgap) utils::undo_dgap_list(buf,n);
    }
};
