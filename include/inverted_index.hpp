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
#include "list_s16_lz.hpp"
#include "list_s16_vblz.hpp"
#include "list_qmx.hpp"

#include "boost/progress.hpp"

struct list_data {
	size_t				  list_len;
	std::vector<uint32_t> doc_ids;
	std::vector<uint32_t> freqs;

	list_data(size_t n)
	{
		doc_ids.resize(n + 1024);
		freqs.resize(n + 1024);
	}

	bool operator!=(const list_data& other) const
	{
		if (list_len != other.list_len) {
			LOG(ERROR) << "list len not equal";
			return true;
		}

		for (size_t i = 0; i < list_len; i++) {
			if (doc_ids[i] != other.doc_ids[i]) {
				LOG(ERROR) << "doc id data not equal";
				return true;
			}
			if (freqs[i] != other.freqs[i]) {
				LOG(ERROR) << "freq data not equal";
				return true;
			}
		}

		return false;
	}
};

template <class t_doc_list, class t_freq_list>
struct inverted_index {
	using size_type = uint64_t;

	static std::string type()
	{
		return "invidx<" + t_doc_list::type() + "," + t_freq_list::type() + ">";
	}

	inverted_index() {}

	// construct index
	inverted_index(std::string input_prefix)
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

		{
			LOG(INFO) << "read and compress doc ids";
			std::ifstream docs_in(input_docids, std::ios::binary);
			utils::read_uint32(docs_in); // skip the 1
			m_meta_data.m_num_docs = utils::read_uint32(docs_in);

			bit_ostream<sdsl::bit_vector> ofs(m_doc_data);

			// allocate some space first
			size_t file_size = utils::file_size(input_docids);
			ofs.expand_if_needed(file_size / 11); // 3 bits per elem

			std::vector<uint32_t>   buf(m_meta_data.m_num_docs);
			list_meta_data			lm;
			size_t					num_lists = 0;
			boost::progress_display pd(file_size);
			pd += sizeof(uint32_t) * 2;
			while (!docs_in.eof()) {
				lm.list_len = utils::read_uint32(docs_in);
				for (uint32_t i = 0; i < lm.list_len; i++) {
					buf[i] = utils::read_uint32(docs_in);
					m_meta_data.m_num_postings++;
				}
				num_lists++;
				lm.doc_offset = ofs.tellp();
				t_doc_list::encode(ofs, buf, lm.list_len, m_meta_data.m_num_docs);
				m_meta_data.m_list_data.push_back(lm);
				pd += sizeof(uint32_t) * (lm.list_len + 1);
			}
			m_meta_data.m_num_lists = num_lists;
		}
		{
			LOG(INFO) << "read and compress freqs";
			std::ifstream				  freqs_in(input_freqs, std::ios::binary);
			bit_ostream<sdsl::bit_vector> ffs(m_freq_data);

			// allocate some space first
			size_t file_size = utils::file_size(input_freqs);
			ffs.expand_if_needed(file_size / 16); // 2 bits per elem

			std::vector<uint32_t>   buf(m_meta_data.m_num_docs);
			size_t					num_lists = 0;
			boost::progress_display pd(file_size);
			while (!freqs_in.eof()) {
				auto&  lm			 = m_meta_data.m_list_data[num_lists];
				size_t freq_list_len = utils::read_uint32(freqs_in);
				if (freq_list_len != lm.list_len) {
					LOG(ERROR) << "freq and doc_id lists not same len";
				}
				lm.Ft = 0;
				for (uint32_t i = 0; i < lm.list_len; i++) {
					buf[i] = utils::read_uint32(freqs_in);
					lm.Ft += buf[i];
				}
				num_lists++;
				lm.freq_offset = ffs.tellp();
				t_freq_list::encode(ffs, buf, lm.list_len, lm.Ft);
				pd += sizeof(uint32_t) * (lm.list_len + 1);
			}
		}
		LOG(INFO) << "done creating inverted index.";
	}

	void write(std::string collection_dir)
	{
		utils::create_directory(collection_dir);
		std::string output_docids = collection_dir + "/" + DOCS_NAME;
		std::string output_freqs  = collection_dir + "/" + FREQS_NAME;
		std::string output_meta   = collection_dir + "/" + META_NAME;
		sdsl::store_to_file(m_doc_data, output_docids);
		sdsl::store_to_file(m_freq_data, output_freqs);
		sdsl::store_to_file(m_meta_data, output_meta);
	}

	void read(std::string collection_dir)
	{
		std::string output_docids = collection_dir + "/" + DOCS_NAME;
		std::string output_freqs  = collection_dir + "/" + FREQS_NAME;
		std::string output_meta   = collection_dir + "/" + META_NAME;
		sdsl::load_from_file(m_doc_data, output_docids);
		sdsl::load_from_file(m_freq_data, output_freqs);
		sdsl::load_from_file(m_meta_data, output_meta);
	}

	void stats()
	{
		LOG(INFO) << type() << " NUM POSTINGS = " << m_meta_data.m_num_postings;
		LOG(INFO) << type() << " NUM DOCS = " << m_meta_data.m_num_docs;
		LOG(INFO) << type() << " NUM LISTS = " << m_meta_data.m_num_lists;
		LOG(INFO) << type() << " DOC BPI = "
				  << double(m_doc_data.size()) / double(m_meta_data.m_num_postings);
		LOG(INFO) << type() << " FREQ BPI = "
				  << double(m_freq_data.size()) / double(m_meta_data.m_num_postings);
	}

	list_data& operator[](size_type idx) const
	{
		static list_data ld(m_meta_data.m_num_docs);

		const auto& lm = m_meta_data.m_list_data[idx];

		ld.list_len = lm.list_len;
		// ld.doc_ids.resize(ld.list_len + 1024); // overhead needed for FastPFor methods
		// ld.freqs.resize(ld.list_len + 1024);   // overhead needed for FastPFor methods

		bit_istream<sdsl::bit_vector> docfs(m_doc_data);
		docfs.seek(lm.doc_offset);
		t_doc_list::decode(docfs, ld.doc_ids, lm.list_len, m_meta_data.m_num_docs);

		bit_istream<sdsl::bit_vector> freqfs(m_freq_data);
		freqfs.seek(lm.freq_offset);
		t_freq_list::decode(freqfs, ld.freqs, lm.list_len, lm.Ft);


		return ld;
	}

	size_t list_len(size_type idx) const
	{
		const auto& lm = m_meta_data.m_list_data[idx];
		return lm.list_len;
	}

	size_t list_encoding_bits(size_type idx) const
	{
		const auto& lm				 = m_meta_data.m_list_data[idx];
		size_t		next_doc_offset  = m_doc_data.size();
		size_t		next_freq_offset = m_freq_data.size();
		if (idx + 1 != m_meta_data.m_list_data.size()) {
			const auto& lm1  = m_meta_data.m_list_data[idx + 1];
			next_doc_offset  = lm1.doc_offset;
			next_freq_offset = lm1.freq_offset;
		}
		size_t doc_size_bits  = (next_doc_offset - lm.doc_offset);
		size_t freq_size_bits = (next_freq_offset - lm.freq_offset);
		return doc_size_bits + freq_size_bits;
	}

	size_type num_lists() const { return m_meta_data.m_num_lists; }

	size_type num_docs() const { return m_meta_data.m_num_docs; }

	size_type num_postings() const { return m_meta_data.m_num_postings; }

	bool operator!=(const inverted_index<t_doc_list, t_freq_list>& other)
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
				const auto& cur_list = (*this)[num_lists];

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
				auto&		lm			  = m_meta_data.m_list_data[num_lists];
				const auto& cur_list	  = (*this)[num_lists];
				size_t		freq_list_len = utils::read_uint32(freqs_in);
				if (freq_list_len != lm.list_len) {
					LOG(ERROR) << "freq list len not equal";
					return false;
				}
				uint64_t Ft = 0;
				for (uint32_t i = 0; i < lm.list_len; i++) {
					uint32_t cur_freq = utils::read_uint32(freqs_in);
					if (cur_list.freqs[i] != cur_freq) {
						LOG(ERROR) << "freq data not equal";
						return false;
					}
					Ft += cur_freq;
				}
				if (Ft != lm.Ft) return false;
				num_lists++;
			}
		}

		return true;
	}

	meta_data		 m_meta_data;
	sdsl::bit_vector m_doc_data;
	sdsl::bit_vector m_freq_data;
};
