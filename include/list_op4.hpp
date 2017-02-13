#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "optpfor.h"

template <size_t t_block_size, bool t_dgap>
struct list_op4 {
	static std::string name() { return "op4"; }

	static std::string type() { return "op4(dgap=" + std::to_string(t_dgap) + ")"; }

	static void
	encode(bit_ostream<sdsl::bit_vector>& out, std::vector<uint32_t>& buf, size_t n, size_t)
	{
		static FastPForLib::OPTPFor<t_block_size / 32> optpfor_coder;
		static coder::vbyte_fastpfor				   vcoder;
		if (t_dgap) utils::dgap_list(buf, n);
		size_t bits_needed = 256ULL * 1024ULL + 40ULL * buf.size();
		out.expand_if_needed(bits_needed);
		out.align8();

		const uint32_t* in	= buf.data();
		uint32_t*		out32 = (uint32_t*)out.cur_data8();
		for (size_t i = 0; i < n; i += t_block_size) {
			size_t elems					= t_block_size;
			if (n - i < t_block_size) elems = n - i;
			if (elems != t_block_size) { // write incomplete blocks as vbyte
				auto cur_in = in + i;
				vcoder.encode(out, cur_in, elems);
			} else { // write optpfor block
				size_t written_ints = buf.size();
				auto   cur_in		= in + i;
				optpfor_coder.encodeBlock(cur_in, out32, written_ints);
				out32 += written_ints;
				size_t bits_written = written_ints * sizeof(uint32_t) * 8;
				if (bits_written > bits_needed) {
					std::cerr << "ERROR! bits_needed(" << bits_needed << ") bits_written("
							  << bits_written << ")" << std::endl;
				}
				out.skip(bits_written);
			}
		}
	}

	static void
	decode(bit_istream<sdsl::bit_vector>& in, std::vector<uint32_t>& buf, size_t n, size_t)
	{
		static FastPForLib::OPTPFor<t_block_size / 32> optpfor_coder;
		static coder::vbyte_fastpfor				   vcoder;
		in.align8();
		const uint32_t* in32  = (const uint32_t*)in.cur_data8();
		uint32_t*		out32 = buf.data();
		for (size_t i = 0; i < n; i += t_block_size) {
			size_t elems					= t_block_size;
			if (n - i < t_block_size) elems = n - i;
			if (elems != t_block_size) { // write incomplete blocks as vbyte
				vcoder.decode(in, out32, elems);
			} else {
				size_t read_ints	  = n;
				auto   newin32		  = optpfor_coder.decodeBlock(in32, out32, read_ints);
				size_t processed_ints = newin32 - in32;
				in32				  = newin32;
				out32 += t_block_size;
				in.skip(processed_ints * sizeof(uint32_t) * 8);
			}
		}
		if (t_dgap) utils::undo_dgap_list(buf, n);
	}
};
