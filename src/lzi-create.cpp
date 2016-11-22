#define ELPP_THREAD_SAFE

#include "utils.hpp"
#include "collection.hpp"
#include "lz_utils.hpp"

#include "indexes.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

template<uint64_t block_size,class t_coder>
void compress(invidx_collection& col,utils::cmdargs args,std::string name) {
    auto lz_store_docs = typename lz_store<t_coder, block_size>::builder{}
                   .set_rebuild(args.rebuild)
                   .set_threads(args.threads)
                   .build_or_load(col,col.docs_file,"D-"+name);
    if(name != "bzip2-9") verify_index(col.docs_file, lz_store_docs);
    auto docs_bytes = lz_store_docs.size_in_bytes();
    auto docs_bits = docs_bytes * 8;
    double DBPI = docs_bits / double(col.num_postings);

    auto lz_store_freqs = typename lz_store<t_coder, block_size>::builder{}
                   .set_rebuild(args.rebuild)
                   .set_threads(args.threads)
                   .build_or_load(col,col.freqs_file,"F-"+name);
    if(name != "bzip2-9" ) verify_index(col.freqs_file, lz_store_freqs);

    auto freqs_bytes = lz_store_freqs.size_in_bytes();
    auto freqs_bits = freqs_bytes * 8;
    double FBPI = freqs_bits / double(col.num_postings);
    LOG(INFO) << name << ";" << block_size << ";" << col.num_postings << ";" 
        << freqs_bytes << ";" << docs_bytes;
    LOG(INFO) << name << ": " << block_size << " DBPI = " << DBPI << " FBPI = " << FBPI;
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    /* parse command line */
    LOG(INFO) << "Parsing command line arguments";
    auto args = utils::parse_args_lz(argc, argv);

    /* parse the collection */
    LOG(INFO) << "Parsing collection directory " << args.collection_dir;
    invidx_collection col(args.collection_dir);

    compress<2*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<4*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<8*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<16*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<32*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<64*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<128*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<256*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<512*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<2*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<4*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<8*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<16*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<32*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<64*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<128*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");
    compress<256*1024*1024,coder::bzip2<9>>(col,args,"BZIP-9");

    compress<2*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<4*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<8*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<16*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<32*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<64*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<128*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<256*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<512*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<2*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<4*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<8*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<16*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<32*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<64*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<128*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<256*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<512*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<1024*1024*1024,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<2*1024*1024*1024ULL,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<4*1024*1024*1024ULL,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<8*1024*1024*1024ULL,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<16*1024*1024*1024ULL,coder::zstd<9>>(col,args,"ZSTD-9");
    compress<32*1024*1024*1024ULL,coder::zstd<9>>(col,args,"ZSTD-9");

    compress<2*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<4*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<8*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<16*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<32*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<64*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<128*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<256*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<512*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<2*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<4*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<8*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<16*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<32*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<64*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<128*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<256*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<512*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<1024*1024*1024,coder::lzma<6>>(col,args,"LZMA-9");
    compress<2*1024*1024*1024ULL,coder::lzma<6>>(col,args,"LZMA-9");
    compress<4*1024*1024*1024ULL,coder::lzma<6>>(col,args,"LZMA-9");
    compress<8*1024*1024*1024ULL,coder::lzma<6>>(col,args,"LZMA-9");
    compress<16*1024*1024*1024ULL,coder::lzma<6>>(col,args,"LZMA-9");
    compress<32*1024*1024*1024ULL,coder::lzma<6>>(col,args,"LZMA-9");


    return EXIT_SUCCESS;
}
