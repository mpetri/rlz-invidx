#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"
#include "lz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template<uint32_t t_block_size,uint32_t t_est_size,uint32_t t_down_size,class t_ratio>
void compress_docs(utils::cmdargs_t& args,invidx_collection& col)
{
    const uint32_t factorization_blocksize = 64 * 1024;
    auto rlz_store_docs = typename rlz_store<dict_local_coverage_norms<t_block_size,t_est_size,t_down_size,t_ratio>,
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
    double DBPI = docs_bits / double(col.m_meta_data.m_num_postings);
    LOG(INFO) << t_block_size<<","<<t_est_size<<","<<t_down_size<<",ratio<"<<t_ratio::num<<","<<t_ratio::den<<": D-RLZ-ZSTD-9 BPI = " << DBPI;
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
    compress_docs<8,4,64,std::ratio<1, 2>>(args,col);
    compress_docs<16,4,64,std::ratio<1, 2>>(args,col);
    compress_docs<32,4,64,std::ratio<1, 2>>(args,col);
    compress_docs<64,4,64,std::ratio<1, 2>>(args,col);
    compress_docs<128,4,64,std::ratio<1, 2>>(args,col);
    compress_docs<256,4,64,std::ratio<1, 2>>(args,col);
    compress_docs<512,4,64,std::ratio<1, 2>>(args,col);
    compress_docs<1024,4,64,std::ratio<1, 2>>(args,col);
    
    compress_docs<8,8,64,std::ratio<1, 2>>(args,col);
    compress_docs<16,8,64,std::ratio<1, 2>>(args,col);
    compress_docs<32,8,64,std::ratio<1, 2>>(args,col);
    compress_docs<64,8,64,std::ratio<1, 2>>(args,col);
    compress_docs<128,8,64,std::ratio<1, 2>>(args,col);
    compress_docs<256,8,64,std::ratio<1, 2>>(args,col);
    compress_docs<512,8,64,std::ratio<1, 2>>(args,col);
    compress_docs<1024,8,64,std::ratio<1, 2>>(args,col);
    
    compress_docs<16,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<32,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<64,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<128,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<256,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<512,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<1024,16,64,std::ratio<1, 2>>(args,col);
    
    compress_docs<16,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<32,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<64,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<128,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<256,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<512,16,64,std::ratio<1, 2>>(args,col);
    compress_docs<1024,16,64,std::ratio<1, 2>>(args,col);
    
    compress_docs<32,32,64,std::ratio<1, 2>>(args,col);
    compress_docs<64,32,64,std::ratio<1, 2>>(args,col);
    compress_docs<128,32,64,std::ratio<1, 2>>(args,col);
    compress_docs<256,32,64,std::ratio<1, 2>>(args,col);
    compress_docs<512,32,64,std::ratio<1, 2>>(args,col);
    compress_docs<1024,32,64,std::ratio<1, 2>>(args,col);
    
    
    
    compress_docs<8,4,64,std::ratio<1, 1>>(args,col);
    compress_docs<16,4,64,std::ratio<1, 1>>(args,col);
    compress_docs<32,4,64,std::ratio<1, 1>>(args,col);
    compress_docs<64,4,64,std::ratio<1, 1>>(args,col);
    compress_docs<128,4,64,std::ratio<1, 1>>(args,col);
    compress_docs<256,4,64,std::ratio<1, 1>>(args,col);
    compress_docs<512,4,64,std::ratio<1, 1>>(args,col);
    compress_docs<1024,4,64,std::ratio<1, 1>>(args,col);
    
    compress_docs<8,8,64,std::ratio<1, 1>>(args,col);
    compress_docs<16,8,64,std::ratio<1, 1>>(args,col);
    compress_docs<32,8,64,std::ratio<1, 1>>(args,col);
    compress_docs<64,8,64,std::ratio<1, 1>>(args,col);
    compress_docs<128,8,64,std::ratio<1, 1>>(args,col);
    compress_docs<256,8,64,std::ratio<1, 1>>(args,col);
    compress_docs<512,8,64,std::ratio<1, 1>>(args,col);
    compress_docs<1024,8,64,std::ratio<1, 1>>(args,col);
    
    compress_docs<16,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<32,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<64,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<128,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<256,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<512,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<1024,16,64,std::ratio<1, 1>>(args,col);
    
    compress_docs<16,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<32,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<64,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<128,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<256,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<512,16,64,std::ratio<1, 1>>(args,col);
    compress_docs<1024,16,64,std::ratio<1, 1>>(args,col);
    
    compress_docs<32,32,64,std::ratio<1, 1>>(args,col);
    compress_docs<64,32,64,std::ratio<1, 1>>(args,col);
    compress_docs<128,32,64,std::ratio<1, 1>>(args,col);
    compress_docs<256,32,64,std::ratio<1, 1>>(args,col);
    compress_docs<512,32,64,std::ratio<1, 1>>(args,col);
    compress_docs<1024,32,64,std::ratio<1, 1>>(args,col);
    
    return EXIT_SUCCESS;
}
