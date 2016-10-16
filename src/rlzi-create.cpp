#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "lz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    auto args = utils::parse_args(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    invidx_collection col(args.collection_dir);

    /* create rlz index */
    const uint32_t factorization_blocksize = 64 * 1024;
    {
        auto rlz_store_docs = typename rlz_store<dict_local_coverage_norms<>,
                             factorization_blocksize,
                             factor_coder_blocked<3, coder::zstd<9>, coder::zstd<9>, coder::zstd<9> >>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col,col.docs_file,"D-RLZ-ZSTD-9");
        verify_index(col.docs_file, rlz_store_docs);
        
        auto docs_bytes = rlz_store_docs.size_in_bytes();
        auto docs_bits = docs_bytes * 8;
        LOG(INFO) << "D-RLZ-ZSTD-9 bytes = " << docs_bytes;
        double DBPI = docs_bits / double(col.num_postings);
        LOG(INFO) << "D-RLZ-ZSTD-9 BPI = " << DBPI;
        
        auto rlz_store_freqs = typename rlz_store<dict_local_coverage_norms<>,
                             factorization_blocksize,
                             factor_coder_blocked<3, coder::zstd<9>, coder::zstd<9>, coder::zstd<9> >>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col,col.freqs_file,"F-RLZ-ZSTD-9");
        verify_index(col.freqs_file, rlz_store_freqs);
        
        auto freqs_bytes = rlz_store_freqs.size_in_bytes();
        auto freqs_bits = freqs_bytes * 8;
        LOG(INFO) << "F-ZSTD-9 bytes = " << freqs_bytes;
        double FBPI = freqs_bits / double(col.num_postings);
        LOG(INFO) << "F-ZSTD-9 BPI = " << FBPI;
    }
    {
        auto rlz_store_docs = typename rlz_store<dict_local_coverage_norms<>,
                             factorization_blocksize,
                             factor_coder_blocked<3, coder::zlib<9>, coder::zlib<9>, coder::zlib<9> >>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col,col.docs_file,"D-RLZ-ZLIB-9");
        verify_index(col.docs_file, rlz_store_docs);
        
        auto docs_bytes = rlz_store_docs.size_in_bytes();
        auto docs_bits = docs_bytes * 8;
        LOG(INFO) << "D-RLZ-ZLIB-9 bytes = " << docs_bytes;
        double DBPI = docs_bits / double(col.num_postings);
        LOG(INFO) << "D-RLZ-ZLIB-9 BPI = " << DBPI;
        
        auto rlz_store_freqs = typename rlz_store<dict_local_coverage_norms<>,
                             factorization_blocksize,
                             factor_coder_blocked<3, coder::zlib<9>, coder::zlib<9>, coder::zlib<9> >>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col,col.freqs_file,"F-RLZ-ZLIB-9");
        verify_index(col.freqs_file, rlz_store_freqs);
        
        auto freqs_bytes = rlz_store_freqs.size_in_bytes();
        auto freqs_bits = freqs_bytes * 8;
        LOG(INFO) << "F-ZLIB-9 bytes = " << freqs_bytes;
        double FBPI = freqs_bits / double(col.num_postings);
        LOG(INFO) << "F-ZLIB-9 BPI = " << FBPI;
    }
    /*
    {
        auto rlz_store_docs = typename rlz_store<dict_local_coverage_norms<>,
                             factorization_blocksize,
                             factor_coder_blocked<3, coder::lzma<6>, coder::lzma<6>, coder::lzma<6> >>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col,col.docs_file,"D-RLZ-LZMA-6");
        verify_index(col.docs_file, rlz_store_docs);
        
        auto docs_bytes = rlz_store_docs.size_in_bytes();
        auto docs_bits = docs_bytes * 8;
        LOG(INFO) << "D-RLZ-LZMA-6 bytes = " << docs_bytes;
        double DBPI = docs_bits / double(col.num_postings);
        LOG(INFO) << "D-RLZ-LZMA-6 BPI = " << DBPI;
        
        auto rlz_store_freqs = typename rlz_store<dict_local_coverage_norms<>,
                             factorization_blocksize,
                             factor_coder_blocked<3, coder::lzma<6>, coder::lzma<6>, coder::lzma<6> >>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(args.dict_size_in_bytes)
                             .build_or_load(col,col.freqs_file,"F-RLZ-LZMA-6");
        verify_index(col.freqs_file, rlz_store_freqs);
        
        auto freqs_bytes = rlz_store_freqs.size_in_bytes();
        auto freqs_bits = freqs_bytes * 8;
        LOG(INFO) << "F-LZMA-6 bytes = " << freqs_bytes;
        double FBPI = freqs_bits / double(col.num_postings);
        LOG(INFO) << "F-LZMA-6 BPI = " << FBPI;
    }
    */

    return EXIT_SUCCESS;
}
