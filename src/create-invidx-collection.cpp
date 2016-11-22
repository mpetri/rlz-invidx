
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
    fprintf(stdout, "  -e <encoding>              : encoding (vbyte|u32|s16|optpfor|interpolative).\n");
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
              && args.encoding != "optpfor" && args.encoding != "interpolative") {
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

inline void write_uint32(std::ofstream& out,uint32_t x) {
    out.write(reinterpret_cast<char*>(&x), sizeof x);
}

inline void write_uint8_t(std::ofstream& out,uint8_t x) {
    out.write(reinterpret_cast<char*>(&x), sizeof x);
}

template<uint32_t i>
uint8_t extract7bits(const uint32_t x) {
    return static_cast<uint8_t>((x >> (7 * i)) & ((1U << 7) - 1));
}

template<uint32_t i>
uint8_t extract7bitsmaskless(const uint32_t x) {
    return static_cast<uint8_t>((x >> (7 * i)));
}
        
inline void write_vbyte(std::ofstream& out,uint32_t x) {
    if (x < (1U << 7)) {
        uint8_t b = static_cast<uint8_t>(x | (1U << 7));
        write_uint8_t(out,b);
    } else if (x < (1U << 14)) {
        uint8_t b = extract7bits<0> (x);
        write_uint8_t(out,b);
        b = extract7bitsmaskless<1> (x) | (1U << 7);
        write_uint8_t(out,b);
    } else if (x < (1U << 21)) {
        uint8_t b = extract7bits<0> (x);
        write_uint8_t(out,b);
        b = extract7bits<1> (x);
        write_uint8_t(out,b);
        b = extract7bitsmaskless<2> (x) | (1U << 7);
        write_uint8_t(out,b);
    } else if (x < (1U << 28)) {
        uint8_t b = extract7bits<0> (x);
        write_uint8_t(out,b);
        b = extract7bits<1> (x);
        write_uint8_t(out,b);
        b = extract7bits<2> (x);
        write_uint8_t(out,b);
        b = extract7bitsmaskless<3> (x) | (1U << 7);
        write_uint8_t(out,b);
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
    }
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
    
    uint64_t num_postings = 0;   
    LOG(INFO) << "writing docids (encoding=" << args.encoding << ")";
    {
        std::ofstream docs_out(output_docids,std::ios::binary);
        std::ifstream docfs(input_docids, std::ios::binary);
        size_t list_id = 1;
        if(args.encoding == "vbyte") {
            read_uint32(docfs);
            uint32_t ndocs_d = read_uint32(docfs);
            LOG(INFO) << "inverted index contains " << ndocs_d << " documents";
            LOG(INFO) << "encoding docid differences (blocking = " << args.blocking << ")";
            while(!docfs.eof()) {
                uint32_t list_len = read_uint32(docfs);
                
                uint32_t prev = read_uint32(docfs);
                write_vbyte(docs_out,prev);
                
                num_postings++;
                for(uint32_t i=1;i<list_len;i++) {
                    uint32_t cur = read_uint32(docfs);
                    uint32_t gap = cur-prev;
                    prev = cur;
                    
                    if(args.blocking == true && i % BLOCK_SIZE == 0) {
                        gap = cur;
                    }
                    
                    write_vbyte(docs_out,gap);
                    num_postings++;
                }
                list_id++;
            }
        } else if(args.encoding == "s16") {
            std::vector<uint32_t> buf;
            std::vector<uint32_t> out_buf;
            FastPForLib::Simple16<0> s16coder;
            read_uint32(docfs);
            uint32_t ndocs_d = read_uint32(docfs);
            LOG(INFO) << "inverted index contains " << ndocs_d << " documents";
            LOG(INFO) << "encoding docid differences (blocking = " << args.blocking << ")";
            while(!docfs.eof()) {
                uint32_t list_len = read_uint32(docfs);
                if(buf.size() < list_len) {
                    buf.resize(list_len);
                    out_buf.resize(2*buf.size());
                }
                uint32_t prev = read_uint32(docfs);
                buf[0] = prev;
                num_postings++;
                for(uint32_t i=1;i<list_len;i++) {
                    uint32_t cur = read_uint32(docfs);
                    uint32_t gap = cur-prev;
                    prev = cur;
                    if(args.blocking == true && i % BLOCK_SIZE == 0) {
                        gap = cur;
                    }
                    buf[i] = gap;
                    num_postings++;
                }
                // encode with s16 
                const uint32_t* in = buf.data();
                uint32_t* out = out_buf.data();
                size_t written_ints = out_buf.size();
                s16coder.encodeArray(in,list_len,out,written_ints);
                size_t written_bytes = written_ints * sizeof(uint32_t);
                // write to file
                uint8_t* out8 = (uint8_t*) out_buf.data();
                docs_out.write(reinterpret_cast<char*>(out8),written_bytes);
                list_id++;
            }
        } else if(args.encoding == "interpolative") {
            read_uint32(docfs);
            uint32_t ndocs_d = read_uint32(docfs);
            std::vector<uint32_t> buf;
            sdsl::bit_vector out_buf;
            coder::interpolative intp_coder;
            num_postings = 0;
            while(!docfs.eof()) {
                uint32_t list_len = read_uint32(docfs);
                if(buf.size() < list_len) {
                    buf.resize(list_len);
                    out_buf.resize(2*32*buf.size());
                }
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t cur = read_uint32(docfs);
                    buf[i] = cur+1; // in interpolative coder we assume we don't encode 0 but docid 0 exists
                    num_postings++;
                }
                // encode with interpolative 
                const uint32_t* in = buf.data();
                size_t written_bytes = 0;
                {
                    size_t upper_bound = ndocs_d;
                    bit_ostream<sdsl::bit_vector> ofs(out_buf);
                    intp_coder.encode(ofs,in,list_len,upper_bound);
                    written_bytes = (ofs.tellp()/8)+1;
                }
                // write to file
                uint8_t* out8 = (uint8_t*) out_buf.data();
                docs_out.write(reinterpret_cast<char*>(out8),written_bytes);
                list_id++;
            }
        } else if(args.encoding == "optpfor") {
            std::vector<uint32_t> buf;
            std::vector<uint32_t> out_buf;
            FastPForLib::OPTPFor<BLOCK_SIZE/32> optpfor_coder;
            read_uint32(docfs);
            uint32_t ndocs_d = read_uint32(docfs);
            LOG(INFO) << "inverted index contains " << ndocs_d << " documents";
            LOG(INFO) << "encoding docid differences (blocking = " << args.blocking << ")";
            while(!docfs.eof()) {
                uint32_t list_len = read_uint32(docfs);
                if(buf.size() < list_len) {
                    buf.resize(list_len);
                    out_buf.resize(2*buf.size());
                }
                uint32_t prev = read_uint32(docfs);
                buf[0] = prev;
                num_postings++;
                for(uint32_t i=1;i<list_len;i++) {
                    uint32_t cur = read_uint32(docfs);
                    uint32_t gap = cur-prev;
                    prev = cur;
                    if(args.blocking == true && i % BLOCK_SIZE == 0) {
                        gap = cur;
                    }
                    buf[i] = gap;
                    num_postings++;
                }
                
                // encode with optfor
                const uint32_t* in = buf.data();
                uint32_t* out = out_buf.data();
                for(size_t i=0;i<list_len;i+=BLOCK_SIZE) {
                    size_t elems = BLOCK_SIZE;
                    if( list_len - i < BLOCK_SIZE) elems = list_len - i;
                    if(elems != BLOCK_SIZE) { // write incomplete blocks as vbyte
                        for(size_t j=0;j<elems;j++) {
                            write_vbyte(docs_out,buf[i+j]);
                        }
                    } else { // write optpfor block
                        size_t written_ints = out_buf.size();
                        auto cur_in = in + i;
                        optpfor_coder.encodeBlock(cur_in,out,written_ints);
                        size_t written_bytes = written_ints * sizeof(uint32_t);
                        uint8_t* out8 = (uint8_t*) out_buf.data();
                        docs_out.write(reinterpret_cast<char*>(out8),written_bytes);
                    }
                }
                list_id++;
            }
        } else {
            read_uint32(docfs);
            uint32_t ndocs_d = read_uint32(docfs);
            LOG(INFO) << "inverted index contains " << ndocs_d << " documents";
            LOG(INFO) << "encoding docid differences (blocking = " << args.blocking << ")";
            while(!docfs.eof()) {
                uint32_t list_len = read_uint32(docfs);
                uint32_t prev = read_uint32(docfs);
                write_uint32(docs_out,prev);
                num_postings++;
                for(uint32_t i=1;i<list_len;i++) {
                    uint32_t cur = read_uint32(docfs);
                    uint32_t gap = cur-prev;
                    prev = cur;
                    if(args.blocking == true && i % BLOCK_SIZE == 0) {
                        gap = cur;
                    }
                    write_uint32(docs_out,gap);
                    num_postings++;
                }
                list_id++;
            }
        }
        LOG(INFO) << "processed terms = " << list_id;
        LOG(INFO) << "docid postings = " << num_postings;
    }
    LOG(INFO) << "writing freqs (encoding=" << args.encoding << ")";
    {
        size_t list_id = 1;
        std::ofstream freqs_out(output_freqs,std::ios::binary);
        std::ifstream freqsfs(input_freqs, std::ios::binary);
        if(args.encoding == "vbyte") {
            num_postings = 0;
            while(!freqsfs.eof()) {
                uint32_t list_len = read_uint32(freqsfs);
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t freq = read_uint32(freqsfs);
                    write_vbyte(freqs_out,freq);
                    num_postings++;
                }
                list_id++;
            }
        } else if(args.encoding == "s16") {
            std::vector<uint32_t> buf;
            std::vector<uint32_t> out_buf;
            FastPForLib::Simple16<0> s16coder;
            num_postings = 0;
            while(!freqsfs.eof()) {
                uint32_t list_len = read_uint32(freqsfs);
                if(buf.size() < list_len) {
                    buf.resize(list_len);
                    out_buf.resize(2*buf.size());
                }
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t freq = read_uint32(freqsfs);
                    buf[i] = freq;
                    num_postings++;
                }
                // encode with s16 
                const uint32_t* in = buf.data();
                uint32_t* out = out_buf.data();
                size_t written_ints = out_buf.size();
                s16coder.encodeArray(in,list_len,out,written_ints);
                size_t written_bytes = written_ints * sizeof(uint32_t);
                // write to file
                uint8_t* out8 = (uint8_t*) out_buf.data();
                freqs_out.write(reinterpret_cast<char*>(out8),written_bytes);
                list_id++;
            }
        } else if(args.encoding == "interpolative") {
            std::vector<uint32_t> buf;
            sdsl::bit_vector out_buf;
            coder::interpolative intp_coder;
            num_postings = 0;
            while(!freqsfs.eof()) {
                uint32_t list_len = read_uint32(freqsfs);
                if(buf.size() < list_len) {
                    buf.resize(list_len);
                    out_buf.resize(2*32*buf.size());
                }
                uint32_t prefix_sum = read_uint32(freqsfs);
                buf[0] = prefix_sum;
                for(uint32_t i=1;i<list_len;i++) {
                    uint32_t cur = read_uint32(freqsfs);
                    prefix_sum += cur;
                    buf[i] = prefix_sum;
                    num_postings++;
                }
                // encode with interpolative 
                const uint32_t* in = buf.data();
                size_t written_bytes = 0;
                {
                    const uint32_t max_freq = 256;
                    size_t upper_bound = list_len*max_freq;
                    bit_ostream<sdsl::bit_vector> ofs(out_buf);
                    intp_coder.encode(ofs,in,list_len,upper_bound);
                    written_bytes = (ofs.tellp()/8)+1;
                }
                // write to file
                uint8_t* out8 = (uint8_t*) out_buf.data();
                freqs_out.write(reinterpret_cast<char*>(out8),written_bytes);
                list_id++;
            }
        } else if(args.encoding == "optpfor") {
            std::vector<uint32_t> buf;
            std::vector<uint32_t> out_buf;
            FastPForLib::OPTPFor<BLOCK_SIZE/32> optpfor_coder;
            num_postings = 0;
            while(!freqsfs.eof()) {
                uint32_t list_len = read_uint32(freqsfs);
                if(buf.size() < list_len) {
                    buf.resize(list_len);
                    out_buf.resize(2*buf.size());
                }
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t freq = read_uint32(freqsfs);
                    buf[i] = freq;
                    num_postings++;
                }
                // encode with optfor
                const uint32_t* in = buf.data();
                uint32_t* out = out_buf.data();
                for(size_t i=0;i<list_len;i+=BLOCK_SIZE) {
                    size_t elems = BLOCK_SIZE;
                    if( list_len - i < BLOCK_SIZE) elems = list_len - i;
                    if(elems != BLOCK_SIZE) { // write incomplete blocks as vbyte
                        for(size_t j=0;j<elems;j++) {
                            write_vbyte(freqs_out,buf[i+j]);
                        }
                    } else { // write optpfor block
                        size_t written_ints = out_buf.size();
                        auto cur_in = in + i;
                        optpfor_coder.encodeBlock(cur_in,out,written_ints);
                        size_t written_bytes = written_ints * sizeof(uint32_t);
                        uint8_t* out8 = (uint8_t*) out_buf.data();
                        freqs_out.write(reinterpret_cast<char*>(out8),written_bytes);
                    }
                }
                list_id++;
            }
        } else {
            num_postings = 0;
            while(!freqsfs.eof()) {
                uint32_t list_len = read_uint32(freqsfs);
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t freq = read_uint32(freqsfs);
                    write_uint32(freqs_out,freq);
                    num_postings++;
                }
                list_id++;
            }
        }

        LOG(INFO) << "processed terms = " << list_id;
        LOG(INFO) << "freq postings = " << num_postings;
    }
    {
        std::ofstream statsfs(args.collection_dir + "/" + STATS_NAME);
        statsfs << std::to_string(num_postings) << std::endl;
    }

    return 0;
}
