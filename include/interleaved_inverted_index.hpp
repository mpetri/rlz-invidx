#pragma once

#include "collection.hpp"
#include "meta_data.hpp"

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "list_vbyte.hpp"
#include "list_simple16.hpp"
#include "list_op4.hpp"
#include "list_ef.hpp"
#include "list_interp.hpp"
#include "list_interp_block.hpp"
#include "list_u32.hpp"
#include "list_vbyte_lz.hpp"
#include "list_u32_lz.hpp"

#include "boost/progress.hpp"

#include "inverted_index.hpp"

template <class t_list>
struct interleaved_inverted_index {
	using size_type = uint64_t;

	static std::string type() { return "interleaved_inverted_index<" + t_list::type() + ">"; }

	interleaved_inverted_index() {}

	// construct index
	template <class t_invidx>
	interleaved_inverted_index(const t_invidx& idx)
	{
		LOG(INFO) << "create interleaved index from (" << idx.num_lists() << "," << idx.num_docs()
				  << ")";
		std::vector<uint32_t> tmp_buf(idx.num_docs() * 2);

		m_meta_data.m_num_lists = idx.num_lists();
		m_meta_data.m_num_docs  = idx.num_docs();
        m_meta_data.m_num_postings = idx.num_postings();
		bit_ostream<sdsl::bit_vector> lfs(m_list_data);
		list_meta_data				  lm;
		LOG(INFO) << "read and compress doc + freq data interleaved";
		boost::progress_display pd(idx.num_lists());
		for (size_t i = 0; i < idx.num_lists(); i++) {
			auto cur_list = idx[i];

			// create interleaved representation with docs d-gapped
			tmp_buf[0] = cur_list.doc_ids[0];
			tmp_buf[1] = cur_list.freqs[0];
            uint32_t Ft = cur_list.freqs[0];
			for (size_t i = 1; i < cur_list.list_len; i++) {
				size_t offset		= i * 2;
				tmp_buf[offset]		= cur_list.doc_ids[i] - cur_list.doc_ids[i - 1];
				tmp_buf[offset + 1] = cur_list.freqs[i];
                Ft += cur_list.freqs[i];
			}

			// encode
			lm.list_len   = cur_list.list_len;
			lm.doc_offset = lfs.tellp();
            lm.Ft = Ft;
			t_list::encode(lfs, tmp_buf, lm.list_len * 2, m_meta_data.m_num_docs + Ft);
			m_meta_data.m_list_data.push_back(lm);
			++pd;
		}
		LOG(INFO) << "done creating interleaved inverted index.";
	}

	void write(std::string collection_dir)
	{
		utils::create_directory(collection_dir);
		std::string output_docfreqs = collection_dir + "/" + DOCFREQS_NAME;
		std::string output_meta		= collection_dir + "/" + META_NAME;
		sdsl::store_to_file(m_list_data, output_docfreqs);
		sdsl::store_to_file(m_meta_data, output_meta);
	}

	void read(std::string collection_dir)
	{
		std::string output_docfreqs = collection_dir + "/" + DOCFREQS_NAME;
		std::string output_meta		= collection_dir + "/" + META_NAME;
		sdsl::load_from_file(m_list_data, output_docfreqs);
		sdsl::load_from_file(m_meta_data, output_meta);
	}

	void stats()
	{
		LOG(INFO) << type() << " NUM POSTINGS = " << m_meta_data.m_num_postings;
		LOG(INFO) << type() << " NUM DOCS = " << m_meta_data.m_num_docs;
		LOG(INFO) << type() << " NUM LISTS = " << m_meta_data.m_num_lists;
		LOG(INFO) << type() << " LIST BPI = "
				  << double(m_list_data.size()) / double(m_meta_data.m_num_postings);
	}

	list_data operator[](size_type idx) const
	{
		list_data					 ld;
		static std::vector<uint32_t> tmp_buf(m_meta_data.m_num_docs * 2);

		const auto& lm = m_meta_data.m_list_data[idx];

		ld.list_len = lm.list_len;
		ld.doc_ids.resize(ld.list_len + 1024); // overhead needed for FastPFor methods
		ld.freqs.resize(ld.list_len + 1024);   // overhead needed for FastPFor methods

		bit_istream<sdsl::bit_vector> listfs(m_list_data);
		listfs.seek(lm.doc_offset);
		t_list::decode(listfs, tmp_buf, lm.list_len * 2, m_meta_data.m_num_docs + lm.Ft );

		// undo the interleaving and d-gap
		ld.doc_ids[0] = tmp_buf[0];
		ld.freqs[0]   = tmp_buf[1];
		for (size_t i = 1; i < lm.list_len; i++) {
			size_t offset = i * 2;
			ld.doc_ids[i] = tmp_buf[offset] + ld.doc_ids[i - 1];
			ld.freqs[i]   = tmp_buf[offset + 1];
		}


		return ld;
	}

	size_t list_len(size_type idx) const
	{
		const auto& lm = m_meta_data.m_list_data[idx];
		return lm.list_len;
	}

	size_t list_encoding_bits(size_type idx) const
	{
		const auto& lm				= m_meta_data.m_list_data[idx];
		size_t		next_doc_offset = m_list_data.size();
		if (idx + 1 != m_meta_data.m_list_data.size()) {
			const auto& lm1 = m_meta_data.m_list_data[idx + 1];
			next_doc_offset = lm1.doc_offset;
		}
		size_t list_size_bits = (next_doc_offset - lm.doc_offset);
		return list_size_bits;
	}

	size_type num_lists() const { return m_meta_data.m_num_lists; }

	size_type num_docs() const { return m_meta_data.m_num_docs; }

	size_type num_postings() const { return m_meta_data.m_num_postings; }

	bool operator!=(const interleaved_inverted_index<t_list>& other)
	{
		if (other.num_lists() != num_lists()) {
			LOG(ERROR) << "num lists not equal";
			return true;
		}

		for (size_t i = 0; i < num_lists(); i++) {
			auto first  = (*this)[i];
			auto second = other[i];
			if (first != second) return true;
		}

		return false;
	}

	bool verify(std::string input_prefix) const
	{
		std::string input_docids = input_prefix + ".docs";
		std::string input_freqs  = input_prefix + ".freqs";
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
			if (num_docs != m_meta_data.m_num_docs) {
				LOG(ERROR) << "num docs not equal";
				return false;
			}

			size_t num_lists = 0;
			while (!docs_in.eof()) {
				uint32_t list_len = utils::read_uint32(docs_in);
				if (list_len != m_meta_data.m_list_data[num_lists].list_len) {
					LOG(ERROR) << "list lens not equal";
					return false;
				}
				auto cur_list = (*this)[num_lists];

				for (uint32_t i = 0; i < list_len; i++) {
					uint32_t cur_id = utils::read_uint32(docs_in);
					if (cur_list.doc_ids[i] != cur_id) {
						LOG(ERROR) << "list=" << num_lists << " (llen=" << list_len
								   << ") doc ids not equal i=" << i
								   << " is: " << cur_list.doc_ids[i] << " should be: " << cur_id;
						return false;
					}
				}
				num_lists++;
			}
			if (num_lists != m_meta_data.m_num_lists) {
				LOG(ERROR) << "num lists not equal";
				return false;
			}
		}
		// read and verify freqs
		{
			std::ifstream freqs_in(input_freqs, std::ios::binary);
			size_t		  num_lists = 0;
			while (!freqs_in.eof()) {
				auto&  lm			 = m_meta_data.m_list_data[num_lists];
				auto   cur_list		 = (*this)[num_lists];
				size_t freq_list_len = utils::read_uint32(freqs_in);
				if (freq_list_len != lm.list_len) {
					LOG(ERROR) << "freq list len not equal";
					return false;
				}
				uint64_t Ft = 0;
				for (uint32_t i = 0; i < lm.list_len; i++) {
					uint32_t cur_freq = utils::read_uint32(freqs_in);
					if (cur_list.freqs[i] != cur_freq) {
						LOG(ERROR) << "freq data in list " << num_lists << " not equal at "<< i<< " is("
                            <<cur_list.freqs[i] << ") expected("
                            << cur_freq << ")";
						return false;
					}
					Ft += cur_freq;
				}
				if (Ft != lm.Ft) {
                    LOG(ERROR) << "Ft in list " << num_lists << " not equal is(" <<Ft<< ") expected("<<lm.Ft<<")";
                    return false;
                }
				num_lists++;
			}
		}

		return true;
	}

	meta_data		 m_meta_data;
	sdsl::bit_vector m_list_data;
};
