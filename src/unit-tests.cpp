#include "gtest/gtest.h"
#include "sdsl/int_vector.hpp"
#include "bit_streams.hpp"
#include "bit_coders.hpp"
#include <functional>
#include <random>


#include "utils.hpp"
#include "list_interp.hpp"
#include "list_interp_block.hpp"
#include "list_vbyte_lz.hpp"
#include "list_op4.hpp"
#include "list_qmx.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template <class t_compressor>
void test_compressor_u8()
{
	t_compressor							c;
	size_t									n = 10;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint8_t>  dis(1, 255);
	std::uniform_int_distribution<uint64_t> ldis(1, 1000000);
	for (size_t i = 0; i < n; i++) {
		size_t				 len = ldis(gen);
		std::vector<uint8_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), len);
		}
		std::vector<uint8_t> B(1024 + len);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), len);
		}
		for (auto i = 0ull; i < len; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}

template <class t_compressor>
void test_compressor_u32()
{
	t_compressor							c;
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint32_t> dis(1, 2 << 27);
	std::uniform_int_distribution<uint64_t> ldis(1, 1000000);
	for (size_t i = 0; i < n; i++) {
		size_t				  len = ldis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), len);
		}
		std::vector<uint32_t> B(1024 + len);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), len);
		}
		for (auto i = 0ull; i < len; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}


template <class t_compressor>
void test_compressor_u8_as_32()
{
	t_compressor							c;
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint32_t> dis(1, 512);
	std::uniform_int_distribution<uint64_t> ldis(1, 1000000);
	for (size_t i = 0; i < n; i++) {
		size_t				  len = ldis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), len);
		}
		std::vector<uint32_t> B(1024 + len);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), len);
		}
		for (auto i = 0ull; i < len; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}


TEST(bit_stream, simple16)
{
	test_compressor_u8_as_32<coder::simple16>();
	test_compressor_u32<coder::simple16>();
}


TEST(bit_stream, vbytefastpfor)
{
	size_t									n = 200;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto   last = std::unique(A.begin(), A.end());
		auto   n	= std::distance(A.begin(), last);
		size_t prev = A[0];
		for (int64_t i = 1; i < n; i++) {
			auto val = A[i] - prev;
			prev	 = A[i];
			A[i]	 = val;
		}
		coder::vbyte_fastpfor c;
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), n);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), n);
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}


TEST(list_interp, increasing)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(0, 1000000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto				  last = std::unique(A.begin(), A.end());
		auto				  n	= std::distance(A.begin(), last);
		std::vector<uint32_t> C(A.begin(), last);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_interp<false>::encode(os, A, n, 1000000);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_interp<false>::decode(is, B, n, 1000000);
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}

TEST(list_interp, increasing2)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 1000);
	std::uniform_int_distribution<uint64_t> ndis(1, 100000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);

		for (size_t j = 0; j < len; j++) {
			A[j] = ndis(gen) + 1;
		}
		std::sort(A.begin(), A.end());
		auto				  last = std::unique(A.begin(), A.end());
		auto				  n	= std::distance(A.begin(), last);
		std::vector<uint32_t> C(A.begin(), last);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_interp<false>::encode(os, A, n, A[n - 1]);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_interp<false>::decode(is, B, n, A[n - 1]);
		}
		for (auto i = 0; i < n; i++) {
			if (B[i] != C[i]) {
				std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
			}
			ASSERT_EQ(B[i], C[i]);
		}
	}
}


TEST(list_interp, prefixsum)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100000);
	std::uniform_int_distribution<uint64_t> ndis(1, 1000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		std::vector<uint32_t> C(len);
		size_t				  upper_bound = 0;
		for (size_t j = 0; j < len; j++) {
			A[j] = ndis(gen);
			C[j] = A[j];
			upper_bound += A[j];
		}
		auto			 n = len;
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_interp<true>::encode(os, A, n, upper_bound);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_interp<true>::decode(is, B, n, upper_bound);
		}
		for (size_t i = 0; i < n; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}


TEST(list_interp_block, increasing)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);

		for (size_t j = 0; j < len; j++) {
			A[j] = dis(gen);
		}
		std::sort(A.begin(), A.end());
		auto				  last = std::unique(A.begin(), A.end());
		auto				  n	= std::distance(A.begin(), last);
		std::vector<uint32_t> C(A.begin(), last);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_interp_block<128, false>::encode(os, A, n, 100000);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_interp_block<128, false>::decode(is, B, n, 100000);
		}
		for (auto i = 0; i < n; i++) {
			if (B[i] != C[i]) {
				std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
			}
			ASSERT_EQ(B[i], C[i]);
		}
	}
}

TEST(list_interp_block, increasing2)
{
	size_t									n = 2000;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 10000);
	std::uniform_int_distribution<uint64_t> ndis(0, 100000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);

		for (size_t j = 0; j < len; j++) {
			A[j] = ndis(gen);
		}
		std::sort(A.begin(), A.end());
		auto				  last = std::unique(A.begin(), A.end());
		auto				  n	= std::distance(A.begin(), last);
		std::vector<uint32_t> C(A.begin(), last);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_interp_block<128, false>::encode(os, A, n, A[n - 1]);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_interp_block<128, false>::decode(is, B, n, C[n - 1]);
		}
		for (auto i = 0; i < n; i++) {
			if (B[i] != C[i]) {
				std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
			}
			ASSERT_EQ(B[i], C[i]);
		}
	}
}

TEST(list_interp_block, prefixsum)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100000);
	std::uniform_int_distribution<uint64_t> ndis(1, 1000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		std::vector<uint32_t> C(len);
		size_t				  upper_bound = 0;
		for (size_t j = 0; j < len; j++) {
			A[j] = ndis(gen);
			C[j] = A[j];
			upper_bound += A[j];
		}
		auto			 n = len;
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_interp_block<128, true>::encode(os, A, n, upper_bound);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_interp_block<128, true>::decode(is, B, n, upper_bound);
		}
		for (size_t i = 0; i < n; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}

TEST(list_interp_block, prefixsum2)
{
	size_t									n = 2000;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 200000);
	std::uniform_int_distribution<uint64_t> ndis(1, 100);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		std::vector<uint32_t> C(len);
		size_t				  upper_bound = 0;
		for (size_t j = 0; j < len; j++) {
			A[j] = ndis(gen);
			C[j] = A[j];
			upper_bound += A[j];
		}
		auto			 n = len;
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_interp_block<128, true>::encode(os, A, n, upper_bound);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_interp_block<128, true>::decode(is, B, n, upper_bound);
		}
		for (size_t i = 0; i < n; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}


TEST(bit_stream, gamma)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto			 last = std::unique(A.begin(), A.end());
		auto			 n	= std::distance(A.begin(), last);
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			for (auto i = 0; i < n; i++) {
				os.put_gamma(A[i]);
			}
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			for (auto i = 0; i < n; i++) {
				B[i] = is.get_gamma();
			}
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}


TEST(bit_stream, vbyte)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto			 last = std::unique(A.begin(), A.end());
		auto			 n	= std::distance(A.begin(), last);
		coder::vbyte	 c;
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), n);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), n);
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}

TEST(bit_stream, fixed)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto			 last = std::unique(A.begin(), A.end());
		auto			 n	= std::distance(A.begin(), last);
		coder::fixed<7>  c;
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), n);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), n);
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}

TEST(bit_stream, aligned_fixed)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto						   last = std::unique(A.begin(), A.end());
		auto						   n	= std::distance(A.begin(), last);
		coder::aligned_fixed<uint32_t> c;
		sdsl::bit_vector			   bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), n);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), n);
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}

	for (size_t i = 0; i < n; i++) {
		size_t				 len = dis(gen);
		std::vector<uint8_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto						  last = std::unique(A.begin(), A.end());
		auto						  n	= std::distance(A.begin(), last);
		coder::aligned_fixed<uint8_t> c;
		sdsl::bit_vector			  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			c.encode(os, A.data(), n);
		}
		std::vector<uint8_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			c.decode(is, B.data(), n);
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], A[i]);
		}
	}
}

TEST(bit_stream, zlib)
{
	test_compressor_u8<coder::zlib<6>>();
	test_compressor_u8<coder::zlib<9>>();
	test_compressor_u32<coder::zlib<6>>();
	test_compressor_u32<coder::zlib<9>>();
}

TEST(bit_stream, lz4)
{
	test_compressor_u8<coder::lz4hc<6>>();
	test_compressor_u8<coder::lz4hc<9>>();
	test_compressor_u32<coder::lz4hc<6>>();
	test_compressor_u32<coder::lz4hc<9>>();
}

TEST(bit_stream, bzip2)
{
	test_compressor_u8<coder::bzip2<6>>();
	test_compressor_u8<coder::bzip2<9>>();
	test_compressor_u32<coder::bzip2<6>>();
	test_compressor_u32<coder::bzip2<9>>();
}

TEST(bit_stream, brotli)
{
	test_compressor_u8<coder::brotlih<6>>();
	test_compressor_u32<coder::brotlih<6>>();
}


TEST(bit_stream, lzma)
{
	test_compressor_u8<coder::lzma<6>>();
	test_compressor_u32<coder::lzma<6>>();
}


TEST(bit_stream, zstd)
{
	test_compressor_u8<coder::zstd<9>>();
	test_compressor_u8<coder::zstd<6>>();
	test_compressor_u32<coder::zstd<9>>();
	test_compressor_u32<coder::zstd<6>>();
}

template <class t_list>
void test_list_increasing()
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 1000000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len + 1024);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.begin() + len);
		auto				  last	 = std::unique(A.begin(), A.begin() + len);
		auto				  list_len = std::distance(A.begin(), last);
		std::vector<uint32_t> C(A.begin(), last);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			t_list::encode(os, A, list_len, 1000000);
		}
		std::vector<uint32_t> B(list_len + 1024);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			t_list::decode(is, B, list_len, 1000000);
		}
		for (auto i = 0; i < list_len; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}


template <class t_list>
void test_list_unordered()
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> ldis(10, 1000000);
	std::uniform_int_distribution<uint64_t> dis(1, 16);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = ldis(gen);
		std::vector<uint32_t> A(len + 1024);
		size_t				  sum = 0;
		for (size_t j = 0; j < len; j++) {
			A[j] = dis(gen);
			sum += A[j];
		}
		std::vector<uint32_t> C(A.begin(), A.begin() + len);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			t_list::encode(os, A, len, sum);
		}
		std::vector<uint32_t> B(len + 1024);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			t_list::decode(is, B, len, sum);
		}
		for (auto i = 0ULL; i < len; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}

TEST(list_op4, increasing) { test_list_increasing<list_op4<128, true>>(); }

TEST(list_op4, unordered) { test_list_unordered<list_op4<128, false>>(); }

TEST(list_qmx, increasing) { test_list_increasing<list_qmx<true>>(); }

TEST(list_qmx, unordered) { test_list_unordered<list_qmx<false>>(); }


TEST(list_vbyte_lz, increasing)
{
	size_t									n = 20;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 1000000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		for (size_t j = 0; j < len; j++)
			A[j]	  = dis(gen);
		std::sort(A.begin(), A.end());
		auto				  last = std::unique(A.begin(), A.end());
		auto				  n	= std::distance(A.begin(), last);
		std::vector<uint32_t> C(A.begin(), last);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_vbyte_lz<true, 128, coder::zstd<9>>::encode(os, A, n, 1000000);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_vbyte_lz<true, 128, coder::zstd<9>>::decode(is, B, n, 1000000);
		}
		for (auto i = 0; i < n; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}

TEST(list_vbyte_lz, increasing2)
{
	size_t									n = 200;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 1000);
	std::uniform_int_distribution<uint64_t> ndis(1, 100000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);

		for (size_t j = 0; j < len; j++) {
			A[j] = ndis(gen) + 1;
		}
		std::sort(A.begin(), A.end());
		auto				  last = std::unique(A.begin(), A.end());
		auto				  n	= std::distance(A.begin(), last);
		std::vector<uint32_t> C(A.begin(), last);
		sdsl::bit_vector	  bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_vbyte_lz<true, 128, coder::zstd<9>>::encode(os, A, n, A[n - 1]);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_vbyte_lz<true, 128, coder::zstd<9>>::decode(is, B, n, A[n - 1]);
		}
		for (auto i = 0; i < n; i++) {
			if (B[i] != C[i]) {
				std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
			}
			ASSERT_EQ(B[i], C[i]);
		}
	}
}


TEST(list_vbyte_lz, prefixsum)
{
	size_t									n = 200;
	std::mt19937							gen(4711);
	std::uniform_int_distribution<uint64_t> dis(1, 100000);
	std::uniform_int_distribution<uint64_t> ndis(1, 1000);

	for (size_t i = 0; i < n; i++) {
		size_t				  len = dis(gen);
		std::vector<uint32_t> A(len);
		std::vector<uint32_t> C(len);
		size_t				  upper_bound = 0;
		for (size_t j = 0; j < len; j++) {
			A[j] = ndis(gen);
			C[j] = A[j];
			upper_bound += A[j];
		}
		auto			 n = len;
		sdsl::bit_vector bv;
		{
			bit_ostream<sdsl::bit_vector> os(bv);
			list_vbyte_lz<false, 128, coder::zstd<9>>::encode(os, A, n, upper_bound);
		}
		std::vector<uint32_t> B(n);
		{
			bit_istream<sdsl::bit_vector> is(bv);
			list_vbyte_lz<false, 128, coder::zstd<9>>::decode(is, B, n, upper_bound);
		}
		for (size_t i = 0; i < n; i++) {
			ASSERT_EQ(B[i], C[i]);
		}
	}
}


int main(int argc, char* argv[])
{
	::testing::InitGoogleTest(&argc, argv);

	return RUN_ALL_TESTS();
}
