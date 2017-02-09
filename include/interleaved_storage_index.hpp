#pragma once

#include "collection.hpp"
#include "meta_data.hpp"

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "boost/progress.hpp"

template <class t_transform, class t_compress>
struct interleaved_storage_index {
  using size_type = uint64_t;

  static std::string type() {
    return "interleaved_storage_index<" + t_transform::type() + "," +
           t_compress::type() + ">";
  }

  interleaved_storage_index() {}

  // construct index
  interleaved_storage_index(std::string input_prefix) {
    std::string input_docids = input_prefix + ".docs";
    std::string input_freqs = input_prefix + ".freqs";
    if (!utils::file_exists(input_docids)) {
      LOG(FATAL) << "input prefix does not contain docids file: "
                 << input_docids;
      throw std::runtime_error("input prefix does not contain docids file.");
    }
    if (!utils::file_exists(input_freqs)) {
      LOG(FATAL) << "input prefix does not contain freqs file: " << input_freqs;
      throw std::runtime_error("input prefix does not contain freqs file.");
    }

    std::vector<uint32_t> tmp_buf;
    {
      std::ifstream docs_in(input_docids, std::ios::binary);
      utils::read_uint32(docs_in);              // skip the 1
      m_num_docs = utils::read_uint32(docs_in); // skip num docs

      // allocate some space first
      size_t file_size = utils::file_size(input_docids);

      LOG(INFO) << "read doc ids";

      boost::progress_display pd(file_size);
      std::vector<uint32_t> list_lens;
      pd += sizeof(uint32_t) * 2;
      m_num_postings = 0;
      while (!docs_in.eof()) {
        uint32_t list_len = utils::read_uint32(docs_in);
        list_lens.push_back(list_len);
        uint32_t prev = 0;
        for (uint32_t i = 0; i < list_len; i++) {
          uint32_t doc_id = utils::read_uint32(docs_in);
          tmp_buf.push_back(doc_id - prev);
          tmp_buf.push_back(0); // place holder for the frequency
          m_num_postings++;
          prev = doc_id;
        }
        pd += sizeof(uint32_t) * (list_len + 1);
      }
      LOG(INFO) << "num postings = " << m_num_postings;
      LOG(INFO) << "num lists = " << list_lens.size();

      LOG(INFO) << "store list lens";
      m_list_lens.resize(list_lens.size());
      for (size_t i = 0; i < list_lens.size(); i++)
        m_list_lens[i] = list_lens[i];
    }

    {
      std::ifstream freqs_in(input_freqs, std::ios::binary);
      size_t file_size = utils::file_size(input_freqs);
      LOG(INFO) << "read freqs";
      boost::progress_display pd(file_size);
      size_t cur_freq_pos = 1;
      while (!freqs_in.eof()) {
        uint32_t list_len = utils::read_uint32(freqs_in); // skip list len
        for (uint32_t i = 0; i < list_len; i++) {
          uint32_t freq = utils::read_uint32(freqs_in);
          tmp_buf[cur_freq_pos] = freq;
          cur_freq_pos += 2;
        }
        pd += sizeof(uint32_t) * (list_len + 1);
      }

      LOG(INFO) << "transform data";
      sdsl::bit_vector transformed_data;
      {
        bit_ostream<sdsl::bit_vector> tfs(transformed_data);
        tfs.expand_if_needed(file_size);
        t_transform coder;
        coder.encode(tfs, tmp_buf.data(), tmp_buf.size());
        tmp_buf = std::vector<uint32_t>(); // clear the data
      }

      LOG(INFO) << "compress data";
      {
        bit_ostream<sdsl::bit_vector> ffs(m_data);
        ffs.expand_if_needed(file_size);
        const uint8_t *data_ptr = (const uint8_t *)transformed_data.data();
        m_transfromed_data_size = transformed_data.size() / 8;
        t_compress coder;
        coder.encode(ffs, data_ptr, m_transfromed_data_size);
      }
    }
    LOG(INFO) << "done storing inverted index.";
  }

  void write(std::string collection_dir) {
    utils::create_directory(collection_dir);
    std::string output_docfreqs = collection_dir + "/" + DOCFREQS_NAME;
    std::string output_meta = collection_dir + "/" + META_NAME;
    sdsl::store_to_file(m_data, output_docfreqs);
    std::ofstream meta_fs(output_meta);
    sdsl::serialize(m_num_docs, meta_fs);
    sdsl::serialize(m_transfromed_data_size, meta_fs);
    sdsl::serialize(m_num_postings, meta_fs);
    sdsl::serialize(m_list_lens, meta_fs);
  }

  void read(std::string collection_dir) {
    std::string output_docfreqs = collection_dir + "/" + DOCFREQS_NAME;
    std::string output_meta = collection_dir + "/" + META_NAME;
    sdsl::load_from_file(m_data, output_docfreqs);

    std::ifstream meta_fs(output_meta);
    sdsl::read_member(m_num_docs, meta_fs);
    sdsl::read_member(m_transfromed_data_size, meta_fs);
    sdsl::read_member(m_num_postings, meta_fs);
    sdsl::load(m_list_lens, meta_fs);
  }

  void stats() {
    LOG(INFO) << type() << " NUM POSTINGS = " << m_num_postings;
    LOG(INFO) << type()
              << " BPI = " << double(m_data.size()) / double(m_num_postings);
  }

  bool verify(std::string input_prefix) const {
    std::string input_docids = input_prefix + ".docs";
    std::string input_freqs = input_prefix + ".freqs";
    if (!utils::file_exists(input_docids)) {
      LOG(FATAL) << "input prefix does not contain docids file: "
                 << input_docids;
      throw std::runtime_error("input prefix does not contain docids file.");
    }
    if (!utils::file_exists(input_freqs)) {
      LOG(FATAL) << "input prefix does not contain freqs file: " << input_freqs;
      throw std::runtime_error("input prefix does not contain freqs file.");
    }

    // (1) reconstruct data
    std::vector<uint32_t> postings_data((2 * m_num_postings) + 1024);
    {
      bit_istream<sdsl::bit_vector> datafs(m_data);
      sdsl::bit_vector transformed_data(1024 + m_transfromed_data_size * 8);
      uint8_t *data_ptr = (uint8_t *)transformed_data.data();
      t_compress decoder;
      LOG(INFO) << "reversing compression";
      decoder.decode(datafs, data_ptr, m_transfromed_data_size);

      bit_istream<sdsl::bit_vector> tfs(transformed_data);
      t_transform coder;
      LOG(INFO) << "reversing transform";
      coder.decode(tfs, postings_data.data(), m_num_postings * 2);
    }

    // (2) read and verify docs
    {
      LOG(INFO) << "read and verify docs";
      std::ifstream docs_in(input_docids, std::ios::binary);
      utils::read_uint32(docs_in); // skip the 1
      uint32_t num_docs = utils::read_uint32(docs_in);
      if (num_docs != m_num_docs) {
        LOG(ERROR) << "num docs not equal";
        return false;
      }

      size_t num_lists = 0;
      size_t num_postings = 0;
      while (!docs_in.eof()) {
        uint32_t list_len = utils::read_uint32(docs_in);
        if (list_len != m_list_lens[num_lists]) {
          LOG(ERROR) << "list lens not equal";
          return false;
        }

        size_t prev = 0;
        for (uint32_t i = 0; i < list_len; i++) {
          uint32_t cur_id = utils::read_uint32(docs_in);
          uint32_t recovered_id = postings_data[num_postings] + prev;
          num_postings += 2;
          prev = recovered_id;
          if (recovered_id != cur_id) {
            LOG(ERROR) << "list=" << num_lists << " (llen=" << list_len
                       << ") doc ids not equal i=" << i
                       << " is: " << recovered_id << " should be: " << cur_id;
            return false;
          }
        }
        num_lists++;
      }
      if (num_lists != m_list_lens.size()) {
        LOG(ERROR) << "num lists not equal";
        return false;
      }
    }

    // read and verify freqs
    {
      LOG(INFO) << "read and verify freqs";
      std::ifstream freqs_in(input_freqs, std::ios::binary);
      size_t num_lists = 0;
      size_t num_postings = 1;
      while (!freqs_in.eof()) {
        size_t freq_list_len = utils::read_uint32(freqs_in);
        if (freq_list_len != m_list_lens[num_lists]) {
          LOG(ERROR) << "freq list len not equal";
          return false;
        }
        for (uint32_t i = 0; i < freq_list_len; i++) {
          uint32_t cur_freq = utils::read_uint32(freqs_in);
          uint32_t recovered_freq = postings_data[num_postings];
          num_postings += 2;
          if (recovered_freq != cur_freq) {
            LOG(ERROR) << "freq data not equal";
            return false;
          }
        }
        num_lists++;
      }
    }
    return true;
  }

  uint64_t m_num_docs;
  uint64_t m_transfromed_data_size;
  uint64_t m_num_postings;
  sdsl::int_vector<32> m_list_lens;
  sdsl::bit_vector m_data;
};
