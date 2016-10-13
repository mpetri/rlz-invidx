#define ELPP_THREAD_SAFE

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
    collection col(args.collection_dir);

    /* create rlz index */
    const uint32_t factorization_blocksize = 64 * 1024;
    {
        auto lz_store = typename lz_store_static<coder::zlib<9>, factorization_blocksize>::builder{}
                            .set_rebuild(args.rebuild)
                            .set_threads(args.threads)
                            .build_or_load(col,"ZLIB-9");
        verify_index(col, lz_store,"ZLIB-9");
    }
    {
        auto lz_store = typename lz_store_static<coder::zstd<9>, factorization_blocksize>::builder{}
                            .set_rebuild(args.rebuild)
                            .set_threads(args.threads)
                            .build_or_load(col,"ZSTD-9");
        verify_index(col, lz_store,"ZSTD-9");
    }
    {
        auto lz_store = typename lz_store_static<coder::lzma<6>, factorization_blocksize>::builder{}
                            .set_rebuild(args.rebuild)
                            .set_threads(args.threads)
                            .build_or_load(col,"LZMA-6");
        verify_index(col, lz_store,"LZMA-6");
    }
    
    return EXIT_SUCCESS;
}
