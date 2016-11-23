
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"
#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "simple16.h"
#include "optpfor.h"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

const uint32_t BLOCK_SIZE = 128;
// need an upper bound for the prefix sum of the frequencies in a list 
// currently we use list_len * MAX_FREQ_BOUND
const uint32_t MAX_FREQ_BOUND = 128;

typedef struct cmdargs {
    std::string collection_dir;
    std::string input_prefix;
    std::string encoding;
    bool blocking;
} cmdargs_t;

void print_usage(const char* program)
{
    fprintf(stdout, "%s -c <collection directory> -i <input prefix> -e <encoding> -b\n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout, "  -i <input prefix>          : the d2si input prefix.\n");
    fprintf(stdout, "  -e <encoding>              : encoding (vbyte|u32|s16|optpfor|interpolative|ef).\n");
    fprintf(stdout, "  -b                         : blocking.\n");
};

cmdargs_t
parse_args(int argc, const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.input_prefix = "";
    args.encoding = "u32";
    args.blocking = false;
    while ((op = getopt(argc, (char* const*)argv, "c:i:e:b")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 'i':
            args.input_prefix = optarg;
            break;
        case 'b':
            args.blocking = true;
            break;
        case 'e':
            args.encoding = optarg;
            if(args.encoding != "u32" && args.encoding != "vbyte" && args.encoding != "s16"
              && args.encoding != "op4" && args.encoding != "interp" && args.encoding != "ef") {
                std::cerr << "Inxid encoding command line parameter.\n";
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
            break;
        }
    }
    if (args.collection_dir == "" || args.input_prefix == "") {
        std::cerr << "Missing command line parameters.\n";
        print_usage(argv[0]);
        exit(EXIT_FAILURE);
    }
    return args;
}

inline uint32_t read_uint32(std::ifstream& ifs) {
    std::uint32_t n;
    ifs.read(reinterpret_cast<char*>(&n), sizeof n);
    return n;
}

inline void write_uint8_t(uint8_t*& out,uint8_t x) {
    *out = x;
    out++;
}

template<uint32_t i>
uint8_t extract7bits(const uint32_t x) {
    return static_cast<uint8_t>((x >> (7 * i)) & ((1U << 7) - 1));
}

template<uint32_t i>
uint8_t extract7bitsmaskless(const uint32_t x) {
    return static_cast<uint8_t>((x >> (7 * i)));
}
        
inline uint8_t encode_vbyte(uint8_t* out,uint32_t x) {
    if (x < (1U << 7)) {
        uint8_t b = static_cast<uint8_t>(x | (1U << 7));
        write_uint8_t(out,b);
        return 1;
    } else if (x < (1U << 14)) {
        uint8_t b = extract7bits<0> (x);
        write_uint8_t(out,b);
        b = extract7bitsmaskless<1> (x) | (1U << 7);
        write_uint8_t(out,b);
        return 2;
    } else if (x < (1U << 21)) {
        uint8_t b = extract7bits<0> (x);
        write_uint8_t(out,b);
        b = extract7bits<1> (x);
        write_uint8_t(out,b);
        b = extract7bitsmaskless<2> (x) | (1U << 7);
        write_uint8_t(out,b);
        return 3;
    } else if (x < (1U << 28)) {
        uint8_t b = extract7bits<0> (x);
        write_uint8_t(out,b);
        b = extract7bits<1> (x);
        write_uint8_t(out,b);
        b = extract7bits<2> (x);
        write_uint8_t(out,b);
        b = extract7bitsmaskless<3> (x) | (1U << 7);
        write_uint8_t(out,b);
        return 4;
    } else {
        uint8_t b = extract7bits<0> (x);
        write_uint8_t(out,b);
        b = extract7bits<1> (x);
        write_uint8_t(out,b);
        b = extract7bits<2> (x);
        write_uint8_t(out,b);
        b = extract7bits<3> (x);
        write_uint8_t(out,b);
        b = extract7bitsmaskless<4> (x) | (1U << 7);
        write_uint8_t(out,b);
        return 5;
    }
}

void dgap_list(std::vector<uint32_t>& list,size_t n,bool blocking) {
    size_t prev = list[0];
    for(size_t i=1;i<n;i++) {
        uint32_t cur = list[i];
        uint32_t gap = cur - prev;
        prev = cur;
        if(blocking == true && i % BLOCK_SIZE == 0) {
            gap = cur;
        }
        list[i] = gap;
    }
}

void prefixsum_list(std::vector<uint32_t>& list,size_t n,bool blocking) {
    size_t prefixsum = list[0];
    for(size_t i=1;i<n;i++) {
        uint32_t cur = list[i];
        prefixsum += cur;
        if(blocking == true && i % BLOCK_SIZE == 0) {
            prefixsum = cur;
        }
        list[i] = prefixsum;
    }
}

uint32_t
compress_doc_list(std::ofstream& output,std::vector<uint32_t>& list,size_t n,uint32_t ndocs_d,std::string encoding,std::vector<uint32_t>& tmp,bool blocking) {
    if(encoding == "s16" || encoding == "vbyte" || encoding == "op4" || encoding == "u32" ) {
        dgap_list(list,n,blocking);
    }
    
    // encode data
    size_t bytes_written = 0;
    if(encoding == "vbyte") {
        uint8_t* out_buf = (uint8_t*) tmp.data();
        for(size_t i=0;i<n;i++) {
            uint32_t bytes = encode_vbyte(out_buf,list[i]);
            bytes_written += bytes;
            out_buf += bytes;
        }
    } else if(encoding == "s16") {
        static FastPForLib::Simple16<0> s16coder;
        const uint32_t* in = list.data();
        uint32_t* out = tmp.data();
        size_t written_ints = tmp.size();
        s16coder.encodeArray(in,n,out,written_ints);
        bytes_written = written_ints * sizeof(uint32_t);
    } else if(encoding == "op4") {
        static FastPForLib::OPTPFor<BLOCK_SIZE/32> optpfor_coder;
        const uint32_t* in = list.data();
        uint32_t* out = tmp.data();
        for(size_t i=0;i<n;i+=BLOCK_SIZE) {
            size_t elems = BLOCK_SIZE;
            if( n - i < BLOCK_SIZE) elems = n - i;
            if(elems != BLOCK_SIZE) { // write incomplete blocks as vbyte
                uint8_t* u8 = (uint8_t*) out;
                for(size_t j=0;j<elems;j++) {
                    uint32_t bytes = encode_vbyte(u8,list[i+j]);
                    u8 += bytes;
                    bytes_written += bytes;
                }
            } else { // write optpfor block
                size_t written_ints = tmp.size();
                auto cur_in = in + i;
                optpfor_coder.encodeBlock(cur_in,out,written_ints);
                out += written_ints;
                bytes_written += (written_ints * sizeof(uint32_t));
            }
        }
    } else if(encoding == "u32") {
        for(size_t i=0;i<n;i++) tmp[i] = list[i];
    } else if(encoding == "ef") {
        static coder::elias_fano ef_coder;
        if(blocking) {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            size_t upper_bound = ndocs_d;
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            for(size_t i=0;i<n;i+=BLOCK_SIZE) {
                auto cur_in = in + i;
                size_t elems = BLOCK_SIZE;
                if( n - i < BLOCK_SIZE) elems = n - i;
                ef_coder.encode(ofs,cur_in,elems,upper_bound);
            }
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        } else {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            size_t upper_bound = ndocs_d;
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            ef_coder.encode(ofs,in,n,upper_bound);
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        }
    } else if(encoding == "interp") {
        static coder::interpolative intp_coder;
        if(blocking) {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            size_t upper_bound = ndocs_d;
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            for(size_t i=0;i<n;i+=BLOCK_SIZE) {
                auto cur_in = in + i;
                size_t elems = BLOCK_SIZE;
                if( n - i < BLOCK_SIZE) elems = n - i;
                intp_coder.encode(ofs,cur_in,elems,upper_bound);
            }
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        } else {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            size_t upper_bound = ndocs_d;
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            intp_coder.encode(ofs,in,n,upper_bound);
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        }
    }
    
    // write output to file
    uint8_t* out8 = (uint8_t*) tmp.data();
    output.write(reinterpret_cast<char*>(out8),bytes_written);
    return bytes_written;
}

uint32_t
compress_freq_list(std::ofstream& output,std::vector<uint32_t>& list,size_t n,std::string encoding,std::vector<uint32_t>& tmp,bool blocking) {
    if(encoding == "interp" || encoding == "ef") {
        prefixsum_list(list,n,blocking);
    }
    
    // encode data
    size_t bytes_written = 0;
    if(encoding == "vbyte") {
        uint8_t* out_buf = (uint8_t*) tmp.data();
        for(size_t i=0;i<n;i++) {
            uint32_t bytes = encode_vbyte(out_buf,list[i]);
            bytes_written += bytes;
            out_buf += bytes;
        }
    } else if(encoding == "s16") {
        static FastPForLib::Simple16<0> s16coder;
        const uint32_t* in = list.data();
        uint32_t* out = tmp.data();
        size_t written_ints = tmp.size();
        s16coder.encodeArray(in,n,out,written_ints);
        bytes_written = written_ints * sizeof(uint32_t);
    } else if(encoding == "op4") {
        static FastPForLib::OPTPFor<BLOCK_SIZE/32> optpfor_coder;
        const uint32_t* in = list.data();
        uint32_t* out = tmp.data();
        for(size_t i=0;i<n;i+=BLOCK_SIZE) {
            size_t elems = BLOCK_SIZE;
            if( n - i < BLOCK_SIZE) elems = n - i;
            if(elems != BLOCK_SIZE) { // write incomplete blocks as vbyte
                uint8_t* u8 = (uint8_t*) out;
                for(size_t j=0;j<elems;j++) {
                    uint32_t bytes = encode_vbyte(u8,list[i+j]);
                    u8 += bytes;
                    bytes_written += bytes;
                }
            } else { // write optpfor block
                size_t written_ints = tmp.size();
                auto cur_in = in + i;
                optpfor_coder.encodeBlock(cur_in,out,written_ints);
                out += written_ints;
                bytes_written += (written_ints * sizeof(uint32_t));
            }
        }
    } else if(encoding == "u32") {
        for(size_t i=0;i<n;i++) tmp[i] = list[i];
    } else if(encoding == "ef") {
        static coder::elias_fano ef_coder;
        if(blocking) {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            
            
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            for(size_t i=0;i<n;i+=BLOCK_SIZE) {
                auto cur_in = in + i;
                size_t elems = BLOCK_SIZE;
                if( n - i < BLOCK_SIZE) elems = n - i;
                
                size_t upper_bound = cur_in[elems-1]+1 - (elems-1);
                ofs.put_gamma(upper_bound);
                ef_coder.encode(ofs,cur_in,elems,upper_bound);
            }
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        } else {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            size_t upper_bound = in[n-1]+1 - (n-1);
            ofs.put_gamma(upper_bound);
            ef_coder.encode(ofs,in,n,upper_bound);
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        }
    } else if(encoding == "interp") {
        static coder::interpolative intp_coder;
        if(blocking) {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            for(size_t i=0;i<n;i+=BLOCK_SIZE) {
                auto cur_in = in + i;
                size_t elems = BLOCK_SIZE;
                if( n - i < BLOCK_SIZE) elems = n - i;
                
                size_t upper_bound = cur_in[elems-1]+1  - (elems-1);
                ofs.put_gamma(upper_bound);
                intp_coder.encode(ofs,cur_in,elems,upper_bound);
            }
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        } else {
            const uint32_t* in = list.data();
            sdsl::bit_vector tmp_bv;
            bit_ostream<sdsl::bit_vector> ofs(tmp_bv);
            size_t upper_bound = in[n-1]+1 - (n-1);
            ofs.put_gamma(upper_bound);
            intp_coder.encode(ofs,in,n,upper_bound);
            bytes_written = (ofs.tellp()/8)+1;
            uint8_t* bv_data = (uint8_t*) tmp_bv.data();
            uint8_t* out_buf = (uint8_t*) tmp.data();
            for(size_t i=0;i<bytes_written;i++) out_buf[i] = bv_data[i];
        }
    }
    
    // write output to file
    uint8_t* out8 = (uint8_t*) tmp.data();
    output.write(reinterpret_cast<char*>(out8),bytes_written);
    return bytes_written;
}

uint32_t compress_docs(std::string output_file,std::string input_file,std::string encoding,bool blocking) {
    uint64_t num_postings = 0;
    size_t list_id = 1;
    LOG(INFO) << "writing docids (encoding=" << encoding << ")";
    std::ofstream output(output_file,std::ios::binary);
    std::ifstream input(input_file, std::ios::binary);
    LOG(INFO) << "docs input file " << input_file;
    LOG(INFO) << "docs output file " << output_file;
    read_uint32(input); // skip the 1
    uint32_t ndocs_d = read_uint32(input);
    LOG(INFO) << "docs in col " << ndocs_d;
    std::vector<uint32_t> buf(ndocs_d);
    std::vector<uint32_t> tmp_buf(2*ndocs_d);
    size_t written_bytes = 0;
    while(!input.eof()) {
        uint32_t list_len = read_uint32(input);
        for(uint32_t i=0;i<list_len;i++) {
            buf[i] = read_uint32(input);
            num_postings++;
        }
        list_id++;
        written_bytes += compress_doc_list(output,buf,list_len,ndocs_d,encoding,tmp_buf,blocking);
    }
    LOG(INFO) << "processed terms = " << list_id;
    LOG(INFO) << "docid postings = " << num_postings;
    LOG(INFO) << "docid BPI = " << double(written_bytes*8) / double(num_postings);
    return ndocs_d;
}

uint32_t compress_freqs(std::string output_file,std::string input_file,std::string encoding,uint32_t ndocs_d,bool blocking) {
    uint64_t num_postings = 0;
    size_t list_id = 1;
    LOG(INFO) << "writing freqs (encoding=" << encoding << ")";
    std::ofstream output(output_file,std::ios::binary);
    std::ifstream input(input_file, std::ios::binary);
    std::vector<uint32_t> buf(ndocs_d);
    std::vector<uint32_t> tmp_buf(ndocs_d);
    size_t written_bytes = 0;
    while(!input.eof()) {
        uint32_t list_len = read_uint32(input);
        for(uint32_t i=0;i<list_len;i++) {
            buf[i] = read_uint32(input);
            num_postings++;
        }
        list_id++;
        written_bytes += compress_freq_list(output,buf,list_len,encoding,tmp_buf,blocking);
    }
    LOG(INFO) << "processed terms = " << list_id;
    LOG(INFO) << "freq postings = " << num_postings;
    LOG(INFO) << "freq BPI = " << double(written_bytes*8) / double(num_postings);
    return num_postings;
}

int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    cmdargs_t args = parse_args(argc, argv);
    
    std::string input_docids = args.input_prefix + ".docs";
    std::string input_freqs = args.input_prefix + ".freqs";
    if (!utils::file_exists(input_docids)) {
        LOG(FATAL) << "input prefix does not contain docids file: " << input_docids;
        throw std::runtime_error("input prefix does not contain docids file.");
    }
    if (!utils::file_exists(input_freqs)) {
        LOG(FATAL) << "input prefix does not contain freqs file: " << input_freqs;
        throw std::runtime_error("input prefix does not contain freqs file.");
    }
    
    utils::create_directory(args.collection_dir);
    
    std::string output_docids = args.collection_dir + "/" + DOCS_NAME;
    std::string output_freqs = args.collection_dir + "/" + FREQS_NAME;
    
    auto ndocs_d = compress_docs(output_docids,input_docids,args.encoding,args.blocking);
    auto num_postings = compress_freqs(output_freqs,input_freqs,args.encoding,ndocs_d,args.blocking);
    {
        std::ofstream statsfs(args.collection_dir + "/" + STATS_NAME);
        statsfs << std::to_string(num_postings) << std::endl;
    }

    return 0;
}
