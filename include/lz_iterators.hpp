#pragma once

template <class t_idx>
class lz_iterator {
public:
    using size_type = uint64_t;
private:
    t_idx& m_idx;
    size_t m_data_offset;
    size_t m_data_block_offset;
    size_t m_block_size;
    size_t m_block_offset;
    std::vector<uint8_t> m_data_buf;
public:
    const size_t& block_id = m_block_offset;
    const size_t& block_size = m_block_size;
public:
    lz_iterator(t_idx& idx, size_t data_offset)
        : m_idx(idx)
        , m_data_offset(data_offset)
        , m_data_block_offset(data_offset % m_idx.encoding_block_size)
        , m_block_size(m_idx.encoding_block_size)
        , m_block_offset(data_offset / m_idx.encoding_block_size)
    {
        m_data_buf.resize(m_block_size);
    }
    inline void decode_cur_block()
    {
        m_idx.decode_block(m_block_offset, m_data_buf);
    }
    inline uint8_t operator*()
    {
        if (m_data_block_offset == 0) {
            decode_cur_block();
        }
        return m_data_buf[m_data_block_offset];
    }
    inline bool operator==(const lz_iterator& b) const
    {
        return m_data_offset == b.m_data_offset;
    }
    inline bool operator!=(const lz_iterator& b) const
    {
        return m_data_offset != b.m_data_offset;
    }
    inline lz_iterator& operator++()
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
