#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "lz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template<uint64_t block_size,class t_idx_type>
void compress(utils::cmdargs& args,invidx_collection& col,std::string name) {
     auto store_docs = typename t_idx_type::builder{}
                            .set_rebuild(args.rebuild)
                            .set_threads(args.threads)
                            .set_dict_size(args.dict_size_in_bytes)
                            .build_or_load(col,col.docs_file,"D-"+name);
    verify_index(col.docs_file, store_docs);
    auto docs_bytes = store_docs.size_in_bytes();
    auto docs_bits = docs_bytes * 8;
    double DBPI = docs_bits / double(col.m_meta_data.m_num_postings);

    auto store_freqs = typename t_idx_type::builder{}
                   .set_rebuild(args.rebuild)
                   .set_threads(args.threads)
                   .build_or_load(col,col.freqs_file,"F-"+name);
    verify_index(col.freqs_file, store_freqs);

    auto freqs_bytes = store_freqs.size_in_bytes();
    auto freqs_bits = freqs_bytes * 8;
    double FBPI = freqs_bits / double(col.m_meta_data.m_num_postings);
    LOG(INFO) << name << ";" << block_size << ";" << col.m_meta_data.m_num_postings << ";" 
        << freqs_bytes << ";" << docs_bytes;
    LOG(INFO) << name << ": " << block_size << " DBPI = " << DBPI << " FBPI = " << FBPI;
}


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
    const uint64_t block_size = 64*1024;
    {
        using idx_type = lz_store<coder::zstd<9>, block_size>;
        compress<block_size,idx_type>(args,col,"ZSTD-9");
    }

    using dict_type = dict_local_coverage_norms<1024,8,64,std::ratio<1,2>>;
    {
        using factor_coder = factor_coder_blocked<3, coder::zstd<9>, coder::zstd<9>, coder::zstd<9> >;
        using idx_type = rlz_store<dict_type,block_size,factor_coder>;
        compress<block_size,idx_type>(args,col,"RLZ-ZSTD-9");
    }

    using dict_type = dict_local_coverage_norms<1024,8,64,std::ratio<1,2>>;
    {
        const uint64_t comp_lvl = 9;
        using idx_type = zstd_store<dict_type,block_size,comp_lvl>;
        compress<block_size,idx_type>(args,col,"ZSTD_DICT-9");
    }


    return EXIT_SUCCESS;
}
