#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "rlz_utils.hpp"

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
    const uint32_t sample_block_size = 1024;
    {
        auto store = rlz_store<dict_uniform_sample_budget<sample_block_size>,
                             factorization_blocksize,
                             factor_coder_blocked<3, coder::zlib<9>, coder::zlib<9>, coder::zlib<9> >>::builder{}
                             .set_rebuild(args.rebuild)
                             .set_threads(args.threads)
                             .set_dict_size(8 * 1024 * 1024)
                             .build_or_load(col,"test.txt","RLZ-ZLIB");

        verify_index("test.txt", store);
    }

    return EXIT_SUCCESS;
}
