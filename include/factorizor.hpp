#pragma once

#include "utils.hpp"
#include "collection.hpp"
#include "factor_coder.hpp"
#include "bit_streams.hpp"
#include "timings.hpp"
#include "dict_index_sa.hpp"
#include "factor_selector.hpp"

#include <sdsl/int_vector_mapped_buffer.hpp>

#include <cctype>
#include <future>

template <uint32_t t_block_size,
          class t_coder>
struct factorizor {
    
    struct block_encodings {
        size_t id;
        std::vector<uint64_t> offsets;
        std::vector<uint64_t> factors;
        sdsl::bit_vector data;
        bool operator<(const block_encodings& fi) const
        {
            return id < fi.id;
        }
    };

    static std::string type()
    {
        return "factorizor-" + std::to_string(t_block_size) + "-" + t_coder::type();
    }

    template<class t_enc_stream>
    static uint64_t factorize_block(dict_index_sa& dict_idx,block_factor_data& fs,t_coder& coder,t_enc_stream& encoded_stream,const uint8_t* data_ptr,size_t size)
    {
        auto itr = data_ptr;
        auto end = itr + size;
        auto factor_itr = dict_idx.factorize<decltype(data_ptr)>(itr, end);
        fs.reset();
        size_t syms_encoded = 0;
        while (!factor_itr.finished()) {
            if (factor_itr.len == 0) {
                fs.add_factor(coder, itr + syms_encoded, 0, 1);
                syms_encoded++;
            } else {
                uint64_t offset = factor_select_first::pick_offset(dict_idx, factor_itr);
                fs.add_factor(coder, itr + syms_encoded, offset, factor_itr.len);
                syms_encoded += factor_itr.len;
            }
            ++factor_itr;
        }
        auto num_factors = fs.encode_current_block(encoded_stream,coder);
        return num_factors;
    }

    static block_encodings 
    factorize_blocks(dict_index_sa& dict_idx,const uint8_t* data_ptr, size_t block_size, size_t blocks_to_encode, size_t id)
    {
        block_encodings be;
        be.id = id;
        t_coder coder;
        block_factor_data bfd(block_size);
        {
            bit_ostream<sdsl::bit_vector> encoded_stream(be.data);
            for (size_t i = 0; i < blocks_to_encode; i++) {
                be.offsets.push_back(encoded_stream.tellp());
                auto num_factors = factorize_block(dict_idx,bfd,coder,encoded_stream,data_ptr,block_size);
                be.factors.push_back(num_factors);
                data_ptr += block_size;
            }
        }
        return be;
    }

    static void
    parallel_factorize(collection& col,std::string input_file,sdsl::int_vector<8>& dict,uint32_t hash,uint32_t num_threads,std::string name)
    {
        auto rlz_output_file = col.file_name(hash,type());
        
        LOG(INFO) << "["<<name<<"] "  "create dictionary index";
        dict_index_sa dict_idx(dict);
        const sdsl::int_vector_mapper<8, std::ios_base::in> input(input_file,true);
        auto data_size = input.size();
        auto data_size_mb = data_size / (1024 * 1024.0);
        LOG(INFO) << "["<<name<<"] "  "factorize data - " << data_size_mb << " MiB (" << num_threads << " threads) - (" << type() << ")";
        auto num_blocks = input.size() / t_block_size;
        auto left = input.size() % t_block_size;

        block_map_uncompressed<true> bmap;
        if(left)
            bmap.m_block_offsets.resize(num_blocks+1);
        else
            bmap.m_block_offsets.resize(num_blocks);
            
        const size_t blocks_per_thread = (512 * 1024 * 1024) / t_block_size; // 0.5GiB Ram used per thread
        
        auto encoded_data = sdsl::write_out_buffer<1>::create(rlz_output_file);
        bit_ostream<sdsl::int_vector_mapper<1> > encoded_stream(encoded_data);
        size_t init_blocks = num_blocks;
        size_t offset_idx = 0;
        const uint8_t* data_ptr = (const uint8_t*) input.data();
        while (num_blocks) {
            std::vector<std::future<block_encodings> > fis;
            for (size_t i = 0; i < num_threads; i++) {
                size_t blocks_to_encode = std::min(blocks_per_thread, num_blocks);
                fis.push_back(std::async(std::launch::async, [&dict_idx,data_ptr, blocks_to_encode, i] {
                    return factorize_blocks(dict_idx,data_ptr, t_block_size, blocks_to_encode, i);
                }));
                data_ptr += (t_block_size * blocks_to_encode);
                num_blocks -= blocks_to_encode;
                if (num_blocks == 0)
                    break;
            }
            // join the threads
            for (auto& fbe : fis) {
                const auto& be = fbe.get();
                size_t size_offset = encoded_stream.tellp();    
                for(size_t i=0;i<be.offsets.size();i++) {
                    size_t o = be.offsets[i];
                    size_t f = be.factors[i];
                    bmap.m_block_offsets[offset_idx] = size_offset + o;
                    bmap.m_block_factors[offset_idx] = f;
                    offset_idx++;
                }
                encoded_stream.append(be.data);
            }
            LOG(INFO) << "["<<name<<"] " << "\t encoded blocks: " << init_blocks - num_blocks << "/" << init_blocks;
        }
        
        if (left) { // last block
            block_factor_data bfd(left);
            t_coder coder;
            bmap.m_block_offsets[bmap.m_block_offsets.size()-1] = encoded_stream.tellp();
            auto num_factors = factorize_block(dict_idx,bfd,coder,encoded_stream,data_ptr,left);
            bmap.m_block_factors[bmap.m_block_offsets.size()-1] = num_factors;
        }
        
        LOG(INFO) << "["<<name<<"] "  "store blockmap";
        bmap.bit_compress();
        auto bmap_output_file = col.file_name(hash,block_map_uncompressed<true>::type());
        sdsl::store_to_file(bmap,bmap_output_file);
    }
};
