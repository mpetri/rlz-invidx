#pragma once

#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "simple16.h"
#include "list_simple16.hpp"

template <bool t_dgap, size_t t_thres, class t_ent_coder>
struct list_s16_vblz {
	static std::string name()
	{
		return "simple16-" + std::to_string(t_thres) + "-vb-" + t_ent_coder::type();
	}

	static std::string type()
	{
		return "simple16(dgap=" + std::to_string(t_dgap) + "-" + std::to_string(t_thres) + "-" +
			   t_ent_coder::type() + ")";
	}
	static void
	encode(bit_ostream<sdsl::bit_vector>& out, std::vector<uint32_t>& buf, size_t n, size_t u)
	{
		if (n < t_thres) {
			list_simple16<t_dgap>::encode(out, buf, n, u);
			return;
		}

		if (t_dgap) utils::dgap_list(buf, n);

		// (1) vbyte encode stuff
		static coder::vbyte_fastpfor vcoder;
		sdsl::bit_vector			 tmp;
		{
			bit_ostream<sdsl::bit_vector> tmpfs(tmp);
			vcoder.encode(tmpfs, buf.data(), n);
		}
		if (tmp.size() % 32 != 0) {
			size_t add = 32 - (tmp.size() % 32);
			tmp.resize(tmp.size() + add);
			LOG(ERROR) << "encoding error!";
		}

		// (2) entropy encode the vbyte encoded data
		size_t num_u32 = tmp.size() / 32;
		out.put_int(num_u32, 32);
		const uint32_t*	vbyte_data = (const uint32_t*)tmp.data();
		static t_ent_coder ent_coder;
		ent_coder.encode(out, vbyte_data, num_u32);
	}

	static void
	decode(bit_istream<sdsl::bit_vector>& in, std::vector<uint32_t>& buf, size_t n, size_t u)
	{
		if (n < t_thres) {
			list_simple16<t_dgap>::decode(in, buf, n, u);
			return;
		}

		// (1) undo the entropy coder
		size_t					num_u32 = in.get_int(32);
		static sdsl::bit_vector tmp(30 * 1024 * 1024 * 32);
		uint32_t*				s16_data = (uint32_t*)tmp.data();
		static t_ent_coder		ent_coder;
		ent_coder.decode(in, s16_data, num_u32);

		// (2) undo the simple16
		static coder::vbyte_fastpfor vcoder;
		{
			bit_istream<sdsl::bit_vector> tmpfs(tmp);
			vcoder.decode(tmpfs, buf.data(), n);
		}

		if (t_dgap) utils::undo_dgap_list(buf, n);
	}
};
