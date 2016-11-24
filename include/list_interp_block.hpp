#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "list_interp.hpp"

template<size_t t_block_size,bool t_prefix>
struct list_interp_block {
    static std::string name() {
        return "interp-block";
    }
    
    static std::string type() {
        return "interp-block(prefix="+std::to_string(t_prefix)+")";
    } 
    
    static void encode(bit_ostream<sdsl::bit_vector>& out,std::vector<uint32_t>& buf,size_t n,size_t universe) {
        // small lists
        if(n < t_block_size) {
            list_interp<t_prefix>::encode(out,buf,n,universe);
            return;
        }
        
        if(t_prefix) utils::prefixsum_list(buf,n);
        
        // encode the skips first
        size_t num_skips = n / t_block_size;
        std::vector<uint32_t> skips;
        for(size_t i=(t_block_size-1);i<n;i+=t_block_size) {
            auto s = buf[i];
            skips.push_back(s);
        }
        static coder::fixed<32> skip_coder;
        skip_coder.encode(out,skips.data(),skips.size());
        
        // encode the content next
        static coder::interpolative interp_coder;
        size_t prev_skip = 0;
        if(t_prefix) prev_skip++;
        for(size_t i=0;i<skips.size();i++) {
            size_t offset = i*t_block_size;
            for(size_t j=0;j<(t_block_size-1);j++) {
                buf[offset+j] = buf[offset+j] - prev_skip;
            }
            size_t local_universe = skips[i] - prev_skip - 1;
            const uint32_t* in = buf.data() + offset;
            interp_coder.encode(out,in,t_block_size-1,local_universe);
            prev_skip = skips[i]+1;
        }
        
        // encode last block
        size_t left = n % t_block_size;
        size_t last_skip = skips.back() + 1;
        if(left != 0) {
            size_t offset = num_skips*t_block_size;
            for(size_t j=0;j<left;j++) {
                buf[offset+j] = buf[offset+j] - last_skip;
            }
            size_t local_universe = universe - last_skip;
            const uint32_t* in = buf.data() + offset;
            interp_coder.encode(out,in,left,local_universe);
        }
    }
    
    static void decode(bit_istream<sdsl::bit_vector>& in,std::vector<uint32_t>& buf,size_t n,size_t universe) {
        // small lists
        if(n < t_block_size) {
            list_interp<t_prefix>::decode(in,buf,n,universe);
            return;
        }
        
        // decode the skips first
        size_t num_skips = n / t_block_size;
        std::vector<uint32_t> skips(num_skips);
        static coder::fixed<32> skip_coder;
        skip_coder.decode(in,skips.data(),num_skips);
        
        // decode the content next
        static coder::interpolative interp_coder;
        size_t prev_skip = 0;
        if(t_prefix) prev_skip++;
        for(size_t i=0;i<skips.size();i++) {
            size_t local_universe = skips[i] - prev_skip - 1;
            size_t offset = i*t_block_size;
            uint32_t* out = buf.data() + offset;
            interp_coder.decode(in,out,t_block_size-1,local_universe);
            for(size_t j=0;j<(t_block_size-1);j++) {
                buf[offset+j] = buf[offset+j] + prev_skip;
            }
            buf[offset+t_block_size-1] = skips[i];
            prev_skip = skips[i]+1;
        }

        // decode last block
        size_t left = n % t_block_size;
        size_t last_skip = skips.back() + 1;
        if(left != 0) {
            size_t local_universe = universe - last_skip;
            size_t offset = num_skips*t_block_size;
            uint32_t* out = buf.data() + offset;
            interp_coder.decode(in,out,left,local_universe);
            for(size_t j=0;j<left;j++) {
                buf[offset+j] = buf[offset+j] + last_skip;
            }
        }
        if(t_prefix) utils::undo_prefixsum_list(buf,n);
    }
};

