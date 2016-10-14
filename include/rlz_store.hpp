#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "iterators.hpp"
#include "block_maps.hpp"
#include "factorizor.hpp"
#include "factor_coder.hpp"
#include "dict_strategies.hpp"

#include <sdsl/suffix_arrays.hpp>

using namespace std::chrono;

template <class t_dictionary_creation_strategy,
    uint32_t t_factorization_block_size,
    class t_factor_coder>
class rlz_store {
public:
    using dictionary_creation_strategy = t_dictionary_creation_strategy;
    using factor_coder_type = t_factor_coder;
    using factorization_strategy = factorizor<t_factorization_block_size,factor_coder_type>;
    using block_map_type = block_map_uncompressed<true>;
    using size_type = uint64_t;
private:
    sdsl::int_vector_mapper<1, std::ios_base::in> m_factored_data;
    bit_istream<sdsl::int_vector_mapper<1, std::ios_base::in> > m_factor_stream;
    sdsl::int_vector<8> m_dict;
    block_map_type m_blockmap;
public:
    enum { block_size = t_factorization_block_size };
    uint64_t encoding_block_size = block_size;
    block_map_type& block_map = m_blockmap;
    sdsl::int_vector<8>& dict = m_dict;
    factor_coder_type m_factor_coder;
    sdsl::int_vector_mapper<1, std::ios_base::in>& factor_text = m_factored_data;
    uint64_t data_size;
    std::string name;
public:
    class builder;

    std::string type() const
    {
        auto dict_size_mb = dict.size() / (1024 * 1024);
        return dictionary_creation_strategy::type() + "-" + std::to_string(dict_size_mb) + "_"
            + factor_coder_type::type();
    }

    rlz_store() = delete;
    rlz_store(rlz_store&&) = default;
    rlz_store& operator=(rlz_store&&) = default;
    rlz_store(collection& col,std::string input_file,uint32_t dict_hash,uint32_t input_hash,std::string n)
        : m_factored_data( col.file_name(dict_hash xor input_hash,factorization_strategy::type()) )
        , m_factor_stream(m_factored_data) // (1) mmap factored text
        , name(n)
    {
        LOG(INFO) << "["<<name<<"] " << "loading RLZ store into memory";
        uint32_t hash = dict_hash xor input_hash;
        
        // (2) load the block map
        LOG(INFO) << "["<<name<<"] " << "\tload block map";
        sdsl::load_from_file( m_blockmap, col.file_name(hash,block_map_uncompressed<true>::type()) );

        // (3) load dictionary from disk
        LOG(INFO) << "["<<name<<"] " << "\tload dictionary";
        auto dict_file_name = col.file_name(input_hash,dictionary_creation_strategy::type());
        sdsl::load_from_file(m_dict,dict_file_name);
        {
            LOG(INFO) << "["<<name<<"] " << "\tdetermine text size";
            const sdsl::int_vector_mapper<8, std::ios_base::in> text(input_file,true);
            data_size = text.size();
        }
        LOG(INFO) << "["<<name<<"] " << "RLZ store ready";
    }

    auto factors_begin() const -> factor_iterator<decltype(*this)>
    {
        return factor_iterator<decltype(*this)>(*this, 0, 0);
    }

    auto factors_end() const -> factor_iterator<decltype(*this)>
    {
        return factor_iterator<decltype(*this)>(*this, m_blockmap.num_blocks(), 0);
    }

    auto begin() const -> text_iterator<decltype(*this)>
    {
        return text_iterator<decltype(*this)>(*this, 0);
    }

    auto end() const -> text_iterator<decltype(*this)>
    {
        return text_iterator<decltype(*this)>(*this, size());
    }

    size_type size() const
    {
        return data_size;
    }

    size_type size_in_bytes() const
    {
        return m_dict.size() + (m_factored_data.size() >> 3) + m_blockmap.size_in_bytes();
    }

    inline void decode_factors(size_t offset,
        block_factor_data& bfd,
        size_t num_factors) const
    {
        m_factor_stream.seek(offset);
        m_factor_coder.decode_block(m_factor_stream, bfd, num_factors);
    }

    inline uint64_t decode_block(uint64_t block_id, std::vector<uint8_t>& text, block_factor_data& bfd) const
    {
        auto block_start = m_blockmap.block_offset(block_id);
        auto num_factors = m_blockmap.block_factors(block_id);
        decode_factors(block_start, bfd, num_factors);

        auto out_itr = text.begin();
        size_t literals_used = 0;
        size_t offsets_used = 0;
        for (size_t i = 0; i < num_factors; i++) {
            const auto& factor_len = bfd.lengths[i];
            if (factor_len <= m_factor_coder.literal_threshold) {
                /* copy literals */
                for (size_t i = 0; i < factor_len; i++) {
                    *out_itr = bfd.literals[literals_used + i];
                    out_itr++;
                }
                literals_used += factor_len;
            }
            else {
                /* copy from dict */
                const auto& factor_offset = bfd.offsets[offsets_used];
                auto begin = m_dict.begin() + factor_offset;
                std::copy(begin, begin + factor_len, out_itr);
                out_itr += factor_len;
                offsets_used++;
            }
        }
        auto written_syms = std::distance(text.begin(), out_itr);
        return written_syms;
    }

    std::vector<uint8_t>
    block(const size_t block_id) const
    {
        block_factor_data bfd(block_size);
        std::vector<uint8_t> block_content(block_size);
        auto decoded_syms = decode_block(block_id, block_content, bfd);
        block_content.resize(decoded_syms);
        return block_content;
    }

    std::vector<factor_data>
    block_factors(const size_t block_id) const
    {
        auto num_factors = m_blockmap.block_factors(block_id);
        factor_iterator<decltype(*this)> fitr(*this, block_id, 0);
        std::vector<factor_data> fdata(num_factors);
        for (size_t i = 0; i < num_factors; i++) {
            factor_data fd = *fitr;
            fdata[i] = fd;
            ++fitr;
        }
        return fdata;
    }
};

#include "rlz_store_builder.hpp"