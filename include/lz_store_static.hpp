#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "lz_iterators.hpp"
#include "block_maps.hpp"
#include "bit_streams.hpp"
#include "bit_coders.hpp"

#include <future>

using namespace std::chrono;

template <class t_coder, uint32_t t_block_size>
class lz_store_static {
public:
    using coder_type = t_coder;
    using block_map_type = block_map_uncompressed<false>;
    using size_type = uint64_t;

private:
    sdsl::int_vector_mapper<1, std::ios_base::in> m_compressed_docs;
    sdsl::int_vector_mapper<1, std::ios_base::in> m_compressed_freqs;
    bit_istream<sdsl::int_vector_mapper<1, std::ios_base::in> > m_compressed_docs_stream;
    bit_istream<sdsl::int_vector_mapper<1, std::ios_base::in> > m_compressed_freqs_stream;
    block_map_type m_blockmap_docs;
    block_map_type m_blockmap_freqs;
public:
    enum { block_size = t_block_size };
    uint64_t encoding_block_size = block_size;
    block_map_type& block_map_docs = m_blockmap_docs;
    block_map_type& block_map_freqs = m_blockmap_freqs;
    coder_type coder;
    sdsl::int_vector_mapper<1, std::ios_base::in>& compressed_docs = m_compressed_docs;
    sdsl::int_vector_mapper<1, std::ios_base::in>& compressed_freqs = m_compressed_freqs;
    mutable block_factor_data dummy;
    uint64_t docs_size;
    uint64_t freqs_size;
public:
    class builder;

    static std::string type()
    {
        return coder_type::type() + "-" + std::to_string(t_block_size);
    }

    lz_store_static() = delete;
    lz_store_static(lz_store_static&&) = default;
    lz_store_static& operator=(lz_store_static&&) = default;
    lz_store_static(collection& col)
        : m_compressed_docs(col.file_map[KEY_LZ_DOCS])
        , m_compressed_freqs(col.file_map[KEY_LZ_FREQS])
        , m_compressed_docs_stream(m_compressed_docs) // (1) mmap factored docs
        , m_compressed_freqs_stream(m_compressed_freqs) // (1) mmap factored freqs
    {
        LOG(INFO) << "Loading Zlib store into memory (" << type() << ")";
        // (2) load the block map
        LOG(INFO) << "\tLoad block maps";
        sdsl::load_from_file(m_blockmap_docs, col.file_map[KEY_BLOCKMAP_DOCS]);
        sdsl::load_from_file(m_blockmap_freqs, col.file_map[KEY_BLOCKMAP_FREQS]);
        {
            LOG(INFO) << "\tDetermine docs size";
            const sdsl::int_vector_mapper<8, std::ios_base::in> docs(col.file_map[KEY_DOCIDS]);
            docs_size = docs.size();
            LOG(INFO) << "\tDetermine freqs size";
            const sdsl::int_vector_mapper<8, std::ios_base::in> freqs(col.file_map[KEY_FREQS]);
            freqs_size = freqs.size();
        }
        LOG(INFO) << "Zlib store ready (" << type() << ")";
    }

    auto begin_docs() const -> zlib_data_iterator<decltype(*this)>
    {
        return zlib_data_iterator<decltype(*this)>(*this, 0,access_type::docs);
    }

    auto end_docs() const -> zlib_data_iterator<decltype(*this)>
    {
        return zlib_data_iterator<decltype(*this)>(*this, size_docs(),access_type::docs);
    }

    auto begin_freqs() const -> zlib_data_iterator<decltype(*this)>
    {
        return zlib_data_iterator<decltype(*this)>(*this, 0,access_type::freqs);
    }

    auto end_freqs() const -> zlib_data_iterator<decltype(*this)>
    {
        return zlib_data_iterator<decltype(*this)>(*this, size_freqs(),access_type::freqs);
    }

    inline size_type size_docs() const
    {
        return docs_size;
    }

    inline size_type size_freqs() const
    {
        return freqs_size;
    }

    size_type size_in_bytes_docs() const
    {
        return (m_compressed_docs.size() >> 3) + m_blockmap_docs.size_in_bytes();
    }
    
    size_type size_in_bytes_freqs() const
    {
        return (m_compressed_freqs.size() >> 3) + m_blockmap_freqs.size_in_bytes();
    }

    size_type size_in_bytes() const
    {
        return size_in_bytes_docs() + size_in_bytes_freqs();
    }

    inline uint64_t decode_block_docs(uint64_t block_id, std::vector<uint8_t>& dat, block_factor_data&) const
    {
        auto offset = m_blockmap_docs.block_offset(block_id);
        m_compressed_docs_stream.seek(offset);
        size_t out_size = block_size;
        if (block_id == m_blockmap_docs.num_blocks() - 1) {
            auto left = docs_size % block_size;
            if (left != 0)
                out_size = left;
        }
        coder.decode(m_compressed_docs_stream, dat.data(), out_size);
        return out_size;
    }

    inline uint64_t decode_block_freqs(uint64_t block_id, std::vector<uint8_t>& dat, block_factor_data&) const
    {
        auto offset = m_blockmap_freqs.block_offset(block_id);
        m_compressed_freqs_stream.seek(offset);
        size_t out_size = block_size;
        if (block_id == m_blockmap_freqs.num_blocks() - 1) {
            auto left = freqs_size % block_size;
            if (left != 0)
                out_size = left;
        }
        coder.decode(m_compressed_freqs_stream, dat.data(), out_size);
        return out_size;
    }

    std::vector<uint8_t>
    block_docs(const size_t block_id) const
    {
        std::vector<uint8_t> block_content(block_size);
        auto out_size = decode_block_docs(block_id, block_content, dummy);
        if (out_size != block_size)
            block_content.resize(out_size);
        return block_content;
    }
    
    std::vector<uint8_t>
    block_freqs(const size_t block_id) const
    {
        std::vector<uint8_t> block_content(block_size);
        auto out_size = decode_block_freqs(block_id, block_content, dummy);
        if (out_size != block_size)
            block_content.resize(out_size);
        return block_content;
    }
};

template <class t_coder, uint32_t t_block_size>
class lz_store_static<t_coder,
    t_block_size>::builder {
public:
    using base_type = lz_store_static<t_coder, t_block_size>;
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

    static std::string blockmap_file_name_docs(collection& col)
    {
        return col.path + "/index/" + KEY_BLOCKMAP_DOCS + "-" + base_type::type() + "-"
            + block_map_type::type() + ".sdsl";
    }

    static std::string blockmap_file_name_freqs(collection& col)
    {
        return col.path + "/index/" + KEY_BLOCKMAP_FREQS + "-" + base_type::type() + "-"
            + block_map_type::type() + ".sdsl";
    }

    static std::string encoding_file_name_docs(collection& col)
    {
        return col.path + "/index/" + KEY_LZ_DOCS + "-" + block_map_type::type() + "-" + base_type::type() + ".sdsl";
    }

    static std::string encoding_file_name_freqs(collection& col)
    {
        return col.path + "/index/" + KEY_LZ_FREQS + "-" + block_map_type::type() + "-" + base_type::type() + ".sdsl";
    }

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

    lz_store_static build_or_load(collection& col,std::string name) const
    {
        auto start = hrclock::now();
        auto lzd_file_name = encoding_file_name_docs(col);
        if (rebuild || !utils::file_exists(lzd_file_name)) {
            LOG(INFO) << "["<<name<<"] " << "Encoding docids (" << base_type::type() << ") threads = " << num_threads;
            
            auto start_enc = hrclock::now();
            const sdsl::int_vector_mapper<8, std::ios_base::in> docids(col.file_map[KEY_DOCIDS]);
            auto encoded_data = sdsl::write_out_buffer<1>::create(lzd_file_name);
            
            bit_ostream<sdsl::int_vector_mapper<1> > encoded_stream(encoded_data);

            auto num_blocks = docids.size() / t_block_size;
            auto left = docids.size() % t_block_size;
            block_map_type bmap;
            if(left)
                bmap.m_block_offsets.resize(num_blocks+1);
            else
                bmap.m_block_offsets.resize(num_blocks);
            
            const uint8_t* data_ptr = (const uint8_t*)docids.data();
            const size_t blocks_per_thread = (512 * 1024 * 1024) / t_block_size; // 0.5GiB Ram used per thread
            
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
            sdsl::store_to_file(bmap, blockmap_file_name_docs(col));
            auto bits_written = encoded_stream.tellp()  + (bmap.size_in_bytes()*8);
            auto bytes_written = bits_written/8;
            auto stop_enc = hrclock::now();
            auto docs_size_mb = docids.size() / (1024 * 1024.0);
            auto enc_seconds = duration_cast<milliseconds>(stop_enc - start_enc).count() / 1000.0;
            LOG(INFO) << "["<<name<<"] " << "Encoding time docs = " << enc_seconds << " sec";
            LOG(INFO) << "["<<name<<"] " << "Encoding speed docs = " << docs_size_mb / enc_seconds << " MB/s";
            LOG(INFO) << "["<<name<<"] " << "Encoded size docs = " << bytes_written << " bytes";
            double bpi_docs = double(bits_written) / double(col.num_postings);
            LOG(INFO) << "["<<name<<"] " << "Encoded docs BPI = " << bpi_docs << " bits per posting";
        }
        else {
            LOG(INFO) << "["<<name<<"] " << "Encoded docids exists.";
        }
        col.file_map[KEY_LZ_DOCS] = lzd_file_name;
        col.file_map[KEY_BLOCKMAP_DOCS] = blockmap_file_name_docs(col);
        
        auto lzf_file_name = encoding_file_name_freqs(col);
        if (rebuild || !utils::file_exists(lzf_file_name)) {
            LOG(INFO) << "["<<name<<"] " << "Encoding freqs (" << base_type::type() << ") threads = " << num_threads;
            
            auto start_enc = hrclock::now();
            const sdsl::int_vector_mapper<8, std::ios_base::in> freqs(col.file_map[KEY_FREQS]);
            auto encoded_data = sdsl::write_out_buffer<1>::create(lzf_file_name);
            
            bit_ostream<sdsl::int_vector_mapper<1> > encoded_stream(encoded_data);

            auto num_blocks = freqs.size() / t_block_size;
            auto left = freqs.size() % t_block_size;
            block_map_type bmap;
            if(left)
                bmap.m_block_offsets.resize(num_blocks+1);
            else
                bmap.m_block_offsets.resize(num_blocks);
            
            const uint8_t* data_ptr = (const uint8_t*)freqs.data();
            const size_t blocks_per_thread = (512 * 1024 * 1024) / t_block_size; // 0.5GiB Ram used per thread
            
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
            sdsl::store_to_file(bmap, blockmap_file_name_freqs(col));
            auto bits_written = encoded_stream.tellp() + (bmap.size_in_bytes()*8);
            auto bytes_written = bits_written/8;
            auto stop_enc = hrclock::now();
            auto freqs_size_mb = freqs.size() / (1024 * 1024.0);
            auto enc_seconds = duration_cast<milliseconds>(stop_enc - start_enc).count() / 1000.0;
            LOG(INFO) << "["<<name<<"] " << "Encoding time freqs = " << enc_seconds << " sec";
            LOG(INFO) << "["<<name<<"] " << "Encoding speed freqs = " << freqs_size_mb / enc_seconds << " MB/s";
            LOG(INFO) << "["<<name<<"] " << "Encoded size freqs = " << bytes_written << " bytes";
            double bpi_freqs = double(bits_written) / double(col.num_postings);
            LOG(INFO) << "["<<name<<"] " << "Encoded freqs BPI = " << bpi_freqs << " bits per posting";
        }
        else {
            LOG(INFO) << "["<<name<<"] " << "Encoded freqs exists.";
            
        }
        col.file_map[KEY_LZ_FREQS] = lzf_file_name;
        col.file_map[KEY_BLOCKMAP_FREQS] = blockmap_file_name_freqs(col);
        
        auto stop = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "LZ construction complete. time = " << duration_cast<seconds>(stop - start).count() << " sec";
        return lz_store_static(col);
    }

    lz_store_static load(collection& col) const
    {
        /* (1) check factorized docs */
        auto lzd_file_name = encoding_file_name_docs(col);
        if (!utils::file_exists(lzd_file_name)) {
            throw std::runtime_error("LOAD FAILED: Cannot find encoded docids.");
        }
        else {
            col.file_map[KEY_LZ_DOCS] = lzd_file_name;
            col.file_map[KEY_BLOCKMAP_DOCS] = blockmap_file_name_docs(col);
        }

        /* (2) check factorized freqs */
        auto lzf_file_name = encoding_file_name_freqs(col);
        if (!utils::file_exists(lzf_file_name)) {
            throw std::runtime_error("LOAD FAILED: Cannot find encoded freqs.");
        }
        else {
            col.file_map[KEY_LZ_FREQS] = lzf_file_name;
            col.file_map[KEY_BLOCKMAP_FREQS] = blockmap_file_name_freqs(col);
        }

        /* load */
        return lz_store_static(col);
    }

private:
    bool rebuild = false;
    uint32_t num_threads = 1;
};
