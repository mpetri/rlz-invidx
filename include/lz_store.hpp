#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "lz_iterators.hpp"
#include "block_maps.hpp"
#include "bit_streams.hpp"
#include "bit_coders.hpp"

#include <future>

using namespace std::chrono;

template <class t_coder, uint64_t t_block_size>
class lz_store {
public:
    using coder_type = t_coder;
    using block_map_type = block_map_uncompressed<false>;
    using size_type = uint64_t;
private:
    sdsl::int_vector_mapper<1, std::ios_base::in> m_compressed_data;
    bit_istream<sdsl::int_vector_mapper<1, std::ios_base::in> > m_compressed_stream;
    block_map_type m_blockmap;
public:
    enum { block_size = t_block_size };
    uint64_t encoding_block_size = block_size;
    block_map_type& block_map = m_blockmap;
    coder_type coder;
    sdsl::int_vector_mapper<1, std::ios_base::in>& compressed_data = m_compressed_data;
    uint64_t data_size;
    std::string name;
public:
    class builder;

    static std::string type()
    {
        return coder_type::type() + "-" + std::to_string(t_block_size);
    }

    lz_store() = delete;
    lz_store(lz_store&&) = default;
    lz_store& operator=(lz_store&&) = default;
    lz_store(collection& col,std::string input_file,uint32_t hash,std::string n)
        : m_compressed_data(col.file_name(hash,type()))
        , m_compressed_stream(m_compressed_data) // (1) mmap factored data
        , name(n)
    {
        LOG(INFO) << "[" << name << "] " << "loading lz store into memory (" << type() << ")";
        // (2) load the block map
        LOG(INFO) << "[" << name << "] " << "\tload block map";
        sdsl::load_from_file(m_blockmap, col.file_name(hash,type()+"-"+block_map_type::type()));
        {
            LOG(INFO) << "[" << name << "] " << "\tdetermine data size";
            const sdsl::int_vector_mapper<8, std::ios_base::in> data(input_file,true);
            data_size = data.size();
        }
        LOG(INFO) << "[" << name << "] " << "lz store ready (" << type() << ")";
    }

    auto begin() const -> lz_iterator<decltype(*this)>
    {
        return lz_iterator<decltype(*this)>(*this, 0);
    }

    auto end() const -> lz_iterator<decltype(*this)>
    {
        return lz_iterator<decltype(*this)>(*this, size());
    }

    size_type size() const
    {
        return data_size;
    }
    
    size_type size_in_bytes() const
    {
        return (m_compressed_data.size() >> 3) + m_blockmap.size_in_bytes();
    }
    
    inline uint64_t decode_block(uint64_t block_id, std::vector<uint8_t>& dat) const
    {
        auto offset = m_blockmap.block_offset(block_id);
        m_compressed_stream.seek(offset);
        size_t out_size = block_size;
        if (block_id == m_blockmap.num_blocks() - 1) {
            auto left = data_size % block_size;
            if (left != 0)
                out_size = left;
        }
        coder.decode(m_compressed_stream, dat.data(), out_size);
        return out_size;
    }


    std::vector<uint8_t>
    block(const size_t block_id) const
    {
        std::vector<uint8_t> block_content(block_size);
        auto out_size = decode_block(block_id, block_content);
        if (out_size != block_size)
            block_content.resize(out_size);
        return block_content;
    }
    
};

template <class t_coder, uint64_t t_block_size>
class lz_store<t_coder,
    t_block_size>::builder {
public:
    using base_type = lz_store<t_coder, t_block_size>;
    using coder_type = t_coder;
    using block_map_type = block_map_uncompressed<false>;

    struct block_encodings {
        size_t id;
        std::vector<uint64_t> offsets;
        sdsl::bit_vector data;
        bool operator<(const block_encodings& fi) const
        {
            return id < fi.id;
        }
    };

public:
    builder& set_rebuild(bool r)
    {
        rebuild = r;
        return *this;
    };
    builder& set_threads(uint8_t nt)
    {
        num_threads = nt;
        return *this;
    };

    static block_encodings encode_blocks(const uint8_t* data_ptr, size_t block_size, size_t blocks_to_encode, size_t id)
    {
        block_encodings be;
        be.id = id;
        coder_type c;
        {
            bit_ostream<sdsl::bit_vector> encoded_stream(be.data);
            for (size_t i = 0; i < blocks_to_encode; i++) {
                be.offsets.push_back(encoded_stream.tellp());
                c.encode(encoded_stream, data_ptr, block_size);
                data_ptr += block_size;
            }
        }
        return be;
    }

    lz_store build_or_load(collection& col,std::string input_file,std::string name) const
    {
        auto start = hrclock::now();
        auto hash = utils::crc(input_file);
        auto lz_output_file = col.file_name(hash,base_type::type());
        if (rebuild || !utils::file_exists(lz_output_file)) {
            auto start_enc = hrclock::now();
            const sdsl::int_vector_mapper<8, std::ios_base::in> input(input_file,true);
            auto encoded_data = sdsl::write_out_buffer<1>::create(lz_output_file);
            bit_ostream<sdsl::int_vector_mapper<1> > encoded_stream(encoded_data);
            
            size_t num_blocks = input.size() / t_block_size;
            auto left = input.size() % t_block_size;
            block_map_type bmap;
            if(left)
                bmap.m_block_offsets.resize(num_blocks+1);
            else
                bmap.m_block_offsets.resize(num_blocks);
            
            const uint8_t* data_ptr = (const uint8_t*) input.data();
            size_t blocks_per_thread = (256 * 1024 * 1024) / t_block_size; // 0.5GiB Ram used per thread
            if(blocks_per_thread == 0) blocks_per_thread = 1;

            size_t init_blocks = num_blocks;
            size_t offset_idx = 0;
            while (num_blocks) {
                std::vector<std::future<block_encodings> > fis;
                for (size_t i = 0; i < num_threads; i++) {
                    size_t blocks_to_encode = std::min(blocks_per_thread, num_blocks);
                    fis.push_back(std::async(std::launch::async, [data_ptr, blocks_to_encode, i] {
                        return encode_blocks(data_ptr, t_block_size, blocks_to_encode, i);
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
                    for (const auto& o : be.offsets) {
                        bmap.m_block_offsets[offset_idx++] = size_offset + o;
                    }
                    encoded_stream.append(be.data);
                }
                LOG(INFO) << "["<<name<<"] " << "\t encoded blocks: " << init_blocks - num_blocks << "/" << init_blocks;
            }
            if (left) { // last block
                bmap.m_block_offsets[bmap.m_block_offsets.size()-1] = encoded_stream.tellp();
                coder_type coder;
                coder.encode(encoded_stream, data_ptr, left);
                data_ptr += left;
            }
            bmap.bit_compress();
            auto bmap_output_file = col.file_name(hash,base_type::type()+"-"+block_map_type::type());
            sdsl::store_to_file(bmap,bmap_output_file);
            auto bytes_written = encoded_stream.tellp()  + (bmap.size_in_bytes()*8);
            auto stop_enc = hrclock::now();
            auto docs_size_mb = input.size() / (1024 * 1024.0);
            auto enc_seconds = duration_cast<milliseconds>(stop_enc - start_enc).count() / 1000.0;
            LOG(INFO) << "["<<name<<"] " << "encoding time = " << enc_seconds << " sec";
            LOG(INFO) << "["<<name<<"] " << "encoding speed = " << docs_size_mb / enc_seconds << " MB/s";
            LOG(INFO) << "["<<name<<"] " << "encoded size = " << bytes_written << " bytes";
        } else {
            LOG(INFO) << "["<<name<<"] " << "lz store exists.";
        }
        auto stop = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "lz construction complete. time = " << duration_cast<seconds>(stop - start).count() << " sec";
        return lz_store(col,input_file,hash,name);
    }
private:
    bool rebuild = false;
    uint32_t num_threads = 1;
};
