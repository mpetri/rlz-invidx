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

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP


TEST(bit_stream, vbytefastpfor)
{
    size_t n = 200;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        size_t prev = A[0];
        for(int64_t i=1;i<n;i++) {
            auto val = A[i] - prev;
            prev = A[i];
            A[i] = val;
        }
        coder::vbyte_fastpfor c;
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

TEST(bit_stream, interp)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(0, 1000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen)*dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::interpolative c;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            c.encode(os, A.data(), n,100000);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            c.decode(is, B.data(), n,100000);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], A[i]);
        }
    }
}

TEST(list_interp, increasing)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(0, 1000000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        std::vector<uint32_t> C(A.begin(),last);
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_interp<false>::encode(os,A,n,1000000);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_interp<false>::decode(is,B,n,1000000);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], C[i]);
        }
    }
}

TEST(list_interp, increasing2)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 1000);
    std::uniform_int_distribution<uint64_t> ndis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        
        for (size_t j = 0; j < len; j++) {
            A[j] = ndis(gen)+1;
        }
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        std::vector<uint32_t> C(A.begin(),last); 
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_interp<false>::encode(os,A,n,A[n-1]);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_interp<false>::decode(is,B,n,A[n-1]);
        }
        for (auto i = 0; i < n; i++) {
            if(B[i]!=C[i]) {
                std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
            }
            ASSERT_EQ(B[i], C[i]);
        }
    }
}


TEST(list_interp, prefixsum)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);
    std::uniform_int_distribution<uint64_t> ndis(1, 1000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        std::vector<uint32_t> C(len);
        size_t upper_bound = 0;
        for (size_t j = 0; j < len; j++) {
            A[j] = ndis(gen);
            C[j] = A[j];
            upper_bound += A[j];
        }
        auto n = len;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_interp<true>::encode(os,A,n,upper_bound);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_interp<true>::decode(is,B,n,upper_bound);
        }
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(B[i], C[i]);
        }
    }
}


TEST(list_interp_block, increasing)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        
        for (size_t j = 0; j < len; j++) {
            A[j] = dis(gen);
        }
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        std::vector<uint32_t> C(A.begin(),last); 
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_interp_block<128,false>::encode(os,A,n,100000);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_interp_block<128,false>::decode(is,B,n,100000);
        }
        for (auto i = 0; i < n; i++) {
            if(B[i]!=C[i]) {
                std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
            }
            ASSERT_EQ(B[i], C[i]);
        }
    }
}

TEST(list_interp_block, increasing2)
{
    size_t n = 2000;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 10000);
    std::uniform_int_distribution<uint64_t> ndis(0, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        
        for (size_t j = 0; j < len; j++) {
            A[j] = ndis(gen);
        }
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        std::vector<uint32_t> C(A.begin(),last); 
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_interp_block<128,false>::encode(os,A,n,A[n-1]);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_interp_block<128,false>::decode(is,B,n,C[n-1]);
        }
        for (auto i = 0; i < n; i++) {
            if(B[i]!=C[i]) {
                std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
            }
            ASSERT_EQ(B[i], C[i]);
        }
    }
}

TEST(list_interp_block, prefixsum)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);
    std::uniform_int_distribution<uint64_t> ndis(1, 1000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        std::vector<uint32_t> C(len);
        size_t upper_bound = 0;
        for (size_t j = 0; j < len; j++) {
            A[j] = ndis(gen);
            C[j] = A[j];
            upper_bound += A[j];
        }
        auto n = len;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_interp_block<128,true>::encode(os,A,n,upper_bound);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_interp_block<128,true>::decode(is,B,n,upper_bound);
        }
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(B[i], C[i]);
        }
    }
}

TEST(list_interp_block, prefixsum2)
{
    size_t n = 2000;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200000);
    std::uniform_int_distribution<uint64_t> ndis(1, 100);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        std::vector<uint32_t> C(len);
        size_t upper_bound = 0;
        for (size_t j = 0; j < len; j++) {
            A[j] = ndis(gen);
            C[j] = A[j];
            upper_bound += A[j];
        }
        auto n = len;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_interp_block<128,true>::encode(os,A,n,upper_bound);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_interp_block<128,true>::decode(is,B,n,upper_bound);
        }
        for (size_t i = 0; i < n; i++) {
            ASSERT_EQ(B[i], C[i]);
        }
    }
}



TEST(bit_stream, gamma)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            for(auto i=0;i<n;i++) {
                os.put_gamma(A[i]);
            }
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            for(auto i=0;i<n;i++) {
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
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::vbyte c;
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
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::fixed<7> c;
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
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::aligned_fixed<uint32_t> c;
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

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::aligned_fixed<uint8_t> c;
        sdsl::bit_vector bv;
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
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::zlib<6> c;
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

TEST(bit_stream, zlib_uint8)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::zlib<6> c;
        sdsl::bit_vector bv;
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

TEST(bit_stream, lz4_uint8)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::lz4hc<9> c;
        sdsl::bit_vector bv;
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

// TEST(bit_stream, bzip2)
// {
//     size_t n = 20;
//     std::mt19937 gen(4711);
//     std::uniform_int_distribution<uint64_t> dis(1, 200);

//     for (size_t i = 0; i < n; i++) {
//         size_t len = dis(gen);
//         std::vector<uint8_t> A(len);
//         for (size_t j = 0; j < len; j++)
//             A[j] = dis(gen);
//         std::sort(A.begin(), A.end());
//         auto last = std::unique(A.begin(), A.end());
//         auto n = std::distance(A.begin(), last);
//         coder::bzip2<9> c;
//         sdsl::bit_vector bv;
//         {
//             bit_ostream<sdsl::bit_vector> os(bv);
//             c.encode(os, A.data(), n);
//         }
//         std::vector<uint8_t> B(n);
//         {
//             bit_istream<sdsl::bit_vector> is(bv);
//             c.decode(is, B.data(), n);
//         }
//         for (auto i = 0; i < n; i++) {
//             ASSERT_EQ(B[i], A[i]);
//         }
//     }
// }


TEST(bit_stream, brotli)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::brotlih<6> c;
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

TEST(bit_stream, brotli_uint8)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::brotlih<6> c;
        sdsl::bit_vector bv;
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


// TEST(bit_stream, lzma)
// {
//     size_t n = 20;
//     std::mt19937 gen(4711);
//     std::uniform_int_distribution<uint64_t> dis(1, 100000);

//     for (size_t i = 0; i < n; i++) {
//         size_t len = dis(gen);
//         std::vector<uint32_t> A(len);
//         for (size_t j = 0; j < len; j++)
//             A[j] = dis(gen);
//         std::sort(A.begin(), A.end());
//         auto last = std::unique(A.begin(), A.end());
//         auto n = std::distance(A.begin(), last);
//         coder::lzma<6> c;
//         sdsl::bit_vector bv;
//         {
//             bit_ostream<sdsl::bit_vector> os(bv);
//             c.encode(os, A.data(), n);
//         }
//         std::vector<uint32_t> B(n);
//         {
//             bit_istream<sdsl::bit_vector> is(bv);
//             c.decode(is, B.data(), n);
//         }
//         for (auto i = 0; i < n; i++) {
//             ASSERT_EQ(B[i], A[i]);
//         }
//     }
// }

// TEST(bit_stream, lzma_uint8)
// {
//     size_t n = 20;
//     std::mt19937 gen(4711);
//     std::uniform_int_distribution<uint64_t> dis(1, 200);

//     for (size_t i = 0; i < n; i++) {
//         size_t len = dis(gen);
//         std::vector<uint8_t> A(len);
//         for (size_t j = 0; j < len; j++)
//             A[j] = dis(gen);
//         std::sort(A.begin(), A.end());
//         auto last = std::unique(A.begin(), A.end());
//         auto n = std::distance(A.begin(), last);
//         coder::lzma<6> c;
//         sdsl::bit_vector bv;
//         {
//             bit_ostream<sdsl::bit_vector> os(bv);
//             c.encode(os, A.data(), n);
//         }
//         std::vector<uint8_t> B(n);
//         {
//             bit_istream<sdsl::bit_vector> is(bv);
//             c.decode(is, B.data(), n);
//         }
//         for (auto i = 0; i < n; i++) {
//             ASSERT_EQ(B[i], A[i]);
//         }
//     }
// }


TEST(bit_stream, zstd)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::zstd<6> c;
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

TEST(bit_stream, zstd_uint8)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 200);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint8_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        coder::zstd<6> c;
        sdsl::bit_vector bv;
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



TEST(list_vbyte_lz, increasing)
{
    size_t n = 20;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 1000000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        for (size_t j = 0; j < len; j++)
            A[j] = dis(gen);
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        std::vector<uint32_t> C(A.begin(),last);
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_vbyte_lz<true,128,coder::zstd<9>>::encode(os,A,n,1000000);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_vbyte_lz<true,128,coder::zstd<9>>::decode(is,B,n,1000000);
        }
        for (auto i = 0; i < n; i++) {
            ASSERT_EQ(B[i], C[i]);
        }
    }
}

TEST(list_vbyte_lz, increasing2)
{
    size_t n = 200;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 1000);
    std::uniform_int_distribution<uint64_t> ndis(1, 100000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        
        for (size_t j = 0; j < len; j++) {
            A[j] = ndis(gen)+1;
        }
        std::sort(A.begin(), A.end());
        auto last = std::unique(A.begin(), A.end());
        auto n = std::distance(A.begin(), last);
        std::vector<uint32_t> C(A.begin(),last); 
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_vbyte_lz<true,128,coder::zstd<9>>::encode(os,A,n,A[n-1]);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_vbyte_lz<true,128,coder::zstd<9>>::decode(is,B,n,A[n-1]);
        }
        for (auto i = 0; i < n; i++) {
            if(B[i]!=C[i]) {
                std::cerr << i << "/" << n << " " << B[i] << " " << C[i] << std::endl;
            }
            ASSERT_EQ(B[i], C[i]);
        }
    }
}


TEST(list_vbyte_lz, prefixsum)
{
    size_t n = 200;
    std::mt19937 gen(4711);
    std::uniform_int_distribution<uint64_t> dis(1, 100000);
    std::uniform_int_distribution<uint64_t> ndis(1, 1000);

    for (size_t i = 0; i < n; i++) {
        size_t len = dis(gen);
        std::vector<uint32_t> A(len);
        std::vector<uint32_t> C(len);
        size_t upper_bound = 0;
        for (size_t j = 0; j < len; j++) {
            A[j] = ndis(gen);
            C[j] = A[j];
            upper_bound += A[j];
        }
        auto n = len;
        sdsl::bit_vector bv;
        {
            bit_ostream<sdsl::bit_vector> os(bv);
            list_vbyte_lz<false,128,coder::zstd<9>>::encode(os,A,n,upper_bound);
        }
        std::vector<uint32_t> B(n);
        {
            bit_istream<sdsl::bit_vector> is(bv);
            list_vbyte_lz<false,128,coder::zstd<9>>::decode(is,B,n,upper_bound);
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
