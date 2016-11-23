#pragma once

#include "sdsl/io.hpp"

struct list_meta_data {
    using size_type = uint64_t;
    
    uint64_t doc_offset = 0;
    uint64_t freq_offset = 0;
    uint32_t list_len = 0; // list len
    uint32_t Ft = 0; // sum of freqs
    
    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += sdsl::serialize(doc_offset,out,child,"doc_offset");
        written_bytes += sdsl::serialize(freq_offset,out,child,"freq_offset");
        written_bytes += sdsl::serialize(list_len,out,child,"list_len");
        written_bytes += sdsl::serialize(Ft,out,child,"Ft");
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        sdsl::load(doc_offset,in);
        sdsl::load(freq_offset,in);
        sdsl::load(list_len,in);
        sdsl::load(Ft,in);
    }
};

struct meta_data {
    using size_type = uint64_t;
    uint64_t m_num_postings = 0;
    uint64_t m_num_docs = 0;
    uint64_t m_num_lists = 0;
    std::vector<list_meta_data> m_list_data;
    
    inline size_type serialize(std::ostream& out, sdsl::structure_tree_node* v = NULL, std::string name = "") const
    {
        using namespace sdsl;
        structure_tree_node* child = structure_tree::add_child(v, name, sdsl::util::class_name(*this));
        size_type written_bytes = 0;
        written_bytes += sdsl::serialize(m_num_postings,out,child,"num_postings");
        written_bytes += sdsl::serialize(m_num_docs,out,child,"num_docs");
        written_bytes += sdsl::serialize(m_num_lists,out,child,"num_lists");
        for(size_t i=0;i<m_num_lists;i++) {
            written_bytes += sdsl::serialize(m_list_data[i],out,child,"list_data");
        }
        sdsl::structure_tree::add_size(child, written_bytes);
        return written_bytes;
    }

    inline void load(std::istream& in)
    {
        sdsl::load(m_num_postings,in);
        sdsl::load(m_num_docs,in);
        sdsl::load(m_num_lists,in);
        m_list_data.resize(m_num_lists);
        for(size_t i=0;i<m_num_lists;i++) {
            sdsl::load(m_list_data[i],in);
        }
    }
};


