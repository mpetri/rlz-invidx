#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

template<bool t_dgap,size_t t_thres,class t_ent_coder>
struct list_u32_lz {
    static std::string name() {
        return "u32-" + t_ent_coder::type();
    }
    
    static std::string type() {
        return "u32(dgap="+std::to_string(t_dgap)+"-"+std::to_string(t_thres)+"-"+t_ent_coder::type()+")";
    } 
    
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::vbyte vcoder;
        if(t_dgap) utils::dgap_list(buf,n);
        
        // (0) small lists remain vbyte only
        if(n <= t_thres) {
            vcoder.encode(out,buf.data(),n);
            return;
        }
        
        // (1) entropy encode the u32 encoded data
        size_t num_u32 = buf.size();
        const uint32_t* u32_data = (const uint32_t*) buf.data(); 
        static t_ent_coder ent_coder;
        ent_coder.encode(out,u32_data,num_u32);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::vbyte vcoder;
        // (0) small lists remain vbyte only
        if(n <= t_thres) {
            vcoder.decode(in,buf.data(),n);
            if(t_dgap) utils::undo_dgap_list(buf,n);
            return;
        }
        
        // (1) undo the entropy coder
        uint32_t* u32_data = (uint32_t*) buf.data();
        static t_ent_coder ent_coder;
        ent_coder.decode(in,u32_data,n);
        
        if(t_dgap) utils::undo_dgap_list(buf,n);
    }
};
