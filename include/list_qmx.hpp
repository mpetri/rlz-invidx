#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "compress_qmx.h"

template <bool t_dgap>
struct list_qmx {
	static std::string name() { return "qmx"; }

	static std::string type() { return "qmx(dgap=" + std::to_string(t_dgap) + ")"; }

	static void
	encode(bit_ostream<sdsl::bit_vector>& out, std::vector<uint32_t>& buf, size_t n, size_t)
	{
		static compress_qmx qmxcoder;
		if (t_dgap) utils::dgap_list(buf, n);
		size_t bits_expected = 256ULL * 1024ULL + 40ULL * buf.size();
		out.expand_if_needed(bits_expected);
		out.align8();

		// write length
		uint32_t* out32			= (uint32_t*)out.cur_data8();
		uint32_t* written_bytes = out32;
		out.skip(32);

		// align and write data
		out.align128();
		out32			   = (uint32_t*)out.cur_data8();
		const uint32_t* in = buf.data();
		size_t			wb = buf.size() * sizeof(uint32_t);
		qmxcoder.encodeArray(in, n, out32, &wb);
		size_t bits_written = (wb * 8);
		*written_bytes		= wb;
		out.skip(bits_written);
	}

	static void
	decode(bit_istream<sdsl::bit_vector>& in, std::vector<uint32_t>& buf, size_t n, size_t)
	{
		static compress_qmx qmxcoder;

		// read length
		in.align8();
		const uint32_t* in32			  = (const uint32_t*)in.cur_data8();
		uint32_t		input_buffer_size = *in32;
		in.skip(32);

		// align 128 and read
		in.align128();

		in32				= (const uint32_t*)in.cur_data8();
		uint32_t* out		= buf.data();
		size_t	read_ints = n;
		qmxcoder.decodeArray(in32, input_buffer_size, out, read_ints);
		size_t bits_read = input_buffer_size * sizeof(uint8_t) * 8;
		in.skip(bits_read);
		if (t_dgap) utils::undo_dgap_list(buf, n);
	}
};
