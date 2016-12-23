#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

template<bool t_dgap,size_t t_thres,class t_ent_coder>
struct list_vbyte_lz {
    static std::string name() {
        return "vbyte-" + t_ent_coder::type();
    }
    
    static std::string type() {
        return "vbyte(dgap="+std::to_string(t_dgap)+"-"+std::to_string(t_thres)+"-"+t_ent_coder::type()+")";
    } 
    
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::vbyte_fastpfor vcoder;
        if(t_dgap) utils::dgap_list(buf,n);
        
        // (0) small lists remain vbyte only
        if(n <= t_thres) {
            vcoder.encode(out,buf.data(),n);
            return;
        }
        
        // (1) vbyte encode stuff
        sdsl::bit_vector tmp;
        {
            bit_ostream<sdsl::bit_vector> tmpfs(tmp);
            vcoder.encode(tmpfs,buf.data(),n);
        }
        if(tmp.size() % 32 != 0) {
            size_t add = 32 - (tmp.size()%32);
            tmp.resize(tmp.size()+add);
        }
    
        // (2) entropy encode the vbyte encoded data
        size_t num_u32 = tmp.size() / 32;
        out.put_int(num_u32,32);  
        const uint32_t* vbyte_data = (const uint32_t*) tmp.data(); 
        static t_ent_coder ent_coder;
        ent_coder.encode(out,vbyte_data,num_u32);
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,size_t n,size_t) {
        static coder::vbyte_fastpfor vcoder;
        // (0) small lists remain vbyte only
        if(n <= t_thres) {
            vcoder.decode(in,buf.data(),n);
            if(t_dgap) utils::undo_dgap_list(buf,n);
            return;
        }
        
        // (1) undo the entropy coder
        size_t num_u32 = in.get_int(32);
        static sdsl::bit_vector tmp(30*1024*1024*32);
        uint32_t* vbyte_data = (uint32_t*) tmp.data();
        static t_ent_coder ent_coder;
        ent_coder.decode(in,vbyte_data,num_u32);
        
        // (2) undo the vbyte
        {
            bit_istream<sdsl::bit_vector> tmpfs(tmp);
            vcoder.decode(tmpfs,buf.data(),n);
        }
        
        if(t_dgap) utils::undo_dgap_list(buf,n);
    }
};
