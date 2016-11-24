#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

template<bool t_dgap>
struct list_vbyte {
    static std::string name() {
        return "vbyte";
    }
    
    static std::string type() {
        return "vbyte(dgap="+std::to_string(t_dgap)+")";
    } 
    
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::vbyte vcoder;
        if(t_dgap) utils::dgap_list(buf,n);
        vcoder.encode(out,buf.data(),n);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::vbyte vcoder;
        vcoder.decode(in,buf.data(),n);
        if(t_dgap) utils::undo_dgap_list(buf,n);
    }
};
