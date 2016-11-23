#pragma once

#include "collection.hpp"
#include "meta_data.hpp"

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "list_vbyte.hpp"

struct list_data {
    size_t list_len;
    std::vector<uint32_t> doc_ids;
    std::vector<uint32_t> freqs;
    
    bool operator!=(const list_data& other) const {
        if(list_len != other.list_len) return true;
        
        for(size_t i=0;i<list_len;i++) {
            if(doc_ids[i] != other.doc_ids[i]) return true;
            if(freqs[i] != other.freqs[i]) return true;
        }
        
        return false;
    }
};

template<class t_doc_list,class t_freq_list>
struct inverted_index {
    using size_type = uint64_t;
    
    inverted_index() {
        
    }
    
    // construct index
    inverted_index(std::string input_prefix) {
        std::string input_docids = input_prefix + ".docs";
        std::string input_freqs = input_prefix + ".freqs";
        if (!utils::file_exists(input_docids)) {
            LOG(FATAL) << "input prefix does not contain docids file: " << input_docids;
            throw std::runtime_error("input prefix does not contain docids file.");
        }
        if (!utils::file_exists(input_freqs)) {
            LOG(FATAL) << "input prefix does not contain freqs file: " << input_freqs;
            throw std::runtime_error("input prefix does not contain freqs file.");
        }
        
        // read and compress docs
        {
            std::ifstream docs_in(input_docids, std::ios::binary);
            utils::read_uint32(docs_in); // skip the 1
            m_meta_data.m_num_docs = utils::read_uint32(docs_in);
       
            bit_ostream<sdsl::bit_vector> ofs(m_doc_data);
            std::vector<uint32_t> buf(m_meta_data.m_num_docs);
            list_meta_data lm;
            size_t num_lists = 0;
            while(!docs_in.eof()) {
                lm.list_len = utils::read_uint32(docs_in);
                for(uint32_t i=0;i<lm.list_len;i++) {
                    buf[i] = utils::read_uint32(docs_in);
                    m_meta_data.m_num_postings++;
                }
                num_lists++;
                lm.doc_offset = ofs.tellp();
                t_doc_list::encode(ofs,buf,lm);
                m_meta_data.m_list_data.push_back(lm);
            }
            m_meta_data.m_num_lists = num_lists;
        }
        // read and compress freqs
        {
            std::ifstream freqs_in(input_freqs, std::ios::binary);
            bit_ostream<sdsl::bit_vector> ffs(m_freq_data);
            std::vector<uint32_t> buf(m_meta_data.m_num_docs);
            size_t num_lists = 0;
            while(!freqs_in.eof()) {
                auto& lm = m_meta_data.m_list_data[num_lists];
                size_t freq_list_len = utils::read_uint32(freqs_in);
                if(freq_list_len != lm.list_len) {
                    LOG(ERROR) << "freq and doc_id lists not same len";
                }
                lm.Ft = 0;
                for(uint32_t i=0;i<lm.list_len;i++) {
                    buf[i] = utils::read_uint32(freqs_in);
                    lm.Ft += buf[i];
                }
                num_lists++;
                lm.freq_offset = ffs.tellp();
                t_freq_list::encode(ffs,buf,lm);
            }
        }
    }
    
    void write(std::string collection_dir) {
        utils::create_directory(collection_dir);
        std::string output_docids = collection_dir + "/" + DOCS_NAME;
        std::string output_freqs = collection_dir + "/" + FREQS_NAME;
        std::string output_meta = collection_dir + "/" + META_NAME;
        sdsl::store_to_file(m_doc_data,output_docids);
        sdsl::store_to_file(m_freq_data,output_freqs);
        sdsl::store_to_file(m_meta_data,output_meta);
    }
    
    void read(std::string collection_dir) {
        std::string output_docids = collection_dir + "/" + DOCS_NAME;
        std::string output_freqs = collection_dir + "/" + FREQS_NAME;
        std::string output_meta = collection_dir + "/" + META_NAME;
        sdsl::load_from_file(m_doc_data,output_docids);
        sdsl::load_from_file(m_freq_data,output_freqs);
        sdsl::load_from_file(m_meta_data,output_meta);
    }
    
    void stats() {
        LOG(INFO) << "NUM POSTINGS = " << m_meta_data.m_num_postings;
        LOG(INFO) << "NUM DOCS = " << m_meta_data.m_num_docs;
        LOG(INFO) << "NUM LISTS = " << m_meta_data.m_num_lists;
        LOG(INFO) << "DOC BPI = " << double(m_doc_data.size()) / double(m_meta_data.m_num_postings);
        LOG(INFO) << "FREQ BPI = " << double(m_freq_data.size()) / double(m_meta_data.m_num_postings);
    }
    
    list_data operator[](size_type idx) const {
        list_data ld;
        
        const auto& lm = m_meta_data.m_list_data[idx];
        
        ld.list_len = lm.list_len;
        ld.doc_ids.resize(ld.list_len);
        ld.freqs.resize(ld.list_len);
        
        bit_istream<sdsl::bit_vector> docfs(m_doc_data);
        docfs.seek(lm.doc_offset);
        t_doc_list::decode(docfs,ld.doc_ids,lm);
        
        bit_istream<sdsl::bit_vector> freqfs(m_freq_data);
        freqfs.seek(lm.freq_offset);
        t_freq_list::decode(freqfs,ld.freqs,lm);
        
        return ld;
    }
    
    size_type num_lists() const {
        return m_meta_data.m_num_lists;
    }
    
    bool operator!=(const inverted_index<t_doc_list,t_freq_list>& other) {
        if(other.num_lists() != num_lists()) return true;
        
        for(size_t i=0;i<num_lists();i++) {
            auto first = (*this)[i];
            auto second = other[i];
            if(first != second) return true;
        }
        
        return false;
    }
    
    bool verify(std::string input_prefix) const {
        std::string input_docids = input_prefix + ".docs";
        std::string input_freqs = input_prefix + ".freqs";
        if (!utils::file_exists(input_docids)) {
            LOG(FATAL) << "input prefix does not contain docids file: " << input_docids;
            throw std::runtime_error("input prefix does not contain docids file.");
        }
        if (!utils::file_exists(input_freqs)) {
            LOG(FATAL) << "input prefix does not contain freqs file: " << input_freqs;
            throw std::runtime_error("input prefix does not contain freqs file.");
        }
        
        // read and verify docs
        {
            std::ifstream docs_in(input_docids, std::ios::binary);
            utils::read_uint32(docs_in); // skip the 1
            uint32_t num_docs = utils::read_uint32(docs_in);
            if(num_docs != m_meta_data.m_num_docs) return false;
            
            size_t num_lists = 0;
            while(!docs_in.eof()) {
                uint32_t list_len = utils::read_uint32(docs_in);
                if(list_len != m_meta_data.m_list_data[num_lists].list_len) return false;
                
                auto cur_list = (*this)[num_lists];
                
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t cur_id = utils::read_uint32(docs_in);
                    if(cur_list.doc_ids[i] != cur_id) return false;
                }
                num_lists++;
            }
            if(num_lists != m_meta_data.m_num_lists) return false;
        }
        // read and verify freqs
        {
            std::ifstream freqs_in(input_freqs, std::ios::binary);
            size_t num_lists = 0;
            while(!freqs_in.eof()) {
                auto& lm = m_meta_data.m_list_data[num_lists];
                auto cur_list = (*this)[num_lists];
                size_t freq_list_len = utils::read_uint32(freqs_in);
                if(freq_list_len != lm.list_len) return false;
                uint64_t Ft = 0;
                for(uint32_t i=0;i<lm.list_len;i++) {
                    uint32_t cur_freq = utils::read_uint32(freqs_in);
                    if(cur_list.freqs[i] != cur_freq) return false;
                    Ft += cur_freq;
                }
                if(Ft != lm.Ft) return false;
                num_lists++;
            }
        }
        
        return true;
    }
    
    meta_data m_meta_data;
    sdsl::bit_vector m_doc_data;
    sdsl::bit_vector m_freq_data;
};
