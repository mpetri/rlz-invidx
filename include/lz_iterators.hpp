#pragma once

#include "factor_data.hpp"

enum class access_type {
    docs,
    freqs
};

template <class t_idx>
class zlib_data_iterator {
public:
    using size_type = uint64_t;

private:
    t_idx& m_idx;
    size_t m_data_offset;
    size_t m_data_block_offset;
    size_t m_block_size;
    size_t m_block_offset;
    access_type m_access;
    std::vector<uint8_t> m_data_buf;
    block_factor_data m_block_factor_data;

public:
    const size_t& block_id = m_block_offset;
    const size_t& block_size = m_block_size;

public:
    zlib_data_iterator(t_idx& idx, size_t data_offset, access_type a)
        : m_idx(idx)
        , m_data_offset(data_offset)
        , m_data_block_offset(data_offset % m_idx.encoding_block_size)
        , m_block_size(m_idx.encoding_block_size)
        , m_block_offset(data_offset / m_idx.encoding_block_size)
        , m_access(a)
    {
        m_data_buf.resize(m_block_size);
    }
    inline void decode_cur_block()
    {
        if(m_access == access_type::docs)
            m_block_size = m_idx.decode_block_docs(m_block_offset, m_data_buf, m_block_factor_data);
        else
            m_block_size = m_idx.decode_block_freqs(m_block_offset, m_data_buf, m_block_factor_data);
    }
    inline uint8_t operator*()
    {
        if (m_data_block_offset == 0) {
            decode_cur_block();
        }
        return m_data_buf[m_data_block_offset];
    }
    inline bool operator==(const zlib_data_iterator& b) const
    {
        return m_data_offset == b.m_data_offset;
    }
    inline bool operator!=(const zlib_data_iterator& b) const
    {
        return m_data_offset != b.m_data_offset;
    }
    inline zlib_data_iterator& operator++()
    {
        if (m_data_block_offset + 1 == m_block_size) {
            m_data_block_offset = 0;
            m_block_offset++;
        }
        else {
            m_data_block_offset++;
        }
        m_data_offset++;
        return *this;
    }

    void seek(size_type new_data_offset)
    {
        auto new_block_offset = new_data_offset / m_block_size;
        m_data_block_offset = new_data_offset % m_block_size;
        if (new_block_offset != m_block_offset) {
            m_block_offset = new_block_offset;
            if (m_data_block_offset != 0)
                decode_cur_block();
        }
    }
};
