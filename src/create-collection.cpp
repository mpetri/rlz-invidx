
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP


typedef struct cmdargs {
    std::string collection_dir;
    std::string input_prefix;
    std::string encoding;
} cmdargs_t;

void print_usage(const char* program)
{
    fprintf(stdout, "%s -c <collection directory> -i <input prefix> -e <encoding>\n", program);
    fprintf(stdout, "where\n");
    fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
    fprintf(stdout, "  -i <input prefix>          : the d2si input prefix.\n");
    fprintf(stdout, "  -e <encoding>              : encoding (vbyte|u32).\n");
};

cmdargs_t
parse_args(int argc, const char* argv[])
{
    cmdargs_t args;
    int op;
    args.collection_dir = "";
    args.input_prefix = "";
    args.encoding = "u32";
    while ((op = getopt(argc, (char* const*)argv, "c:i:e:")) != -1) {
        switch (op) {
        case 'c':
            args.collection_dir = optarg;
            break;
        case 'i':
            args.input_prefix = optarg;
            break;
        case 'e':
            args.encoding = optarg;
            if(args.encoding != "u32" && args.encoding != "vbyte") {
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

inline void write_uint32(sdsl::int_vector_buffer<8>& buf,uint32_t x) {
    buf.push_back(x>>24);
    buf.push_back(x>>16);
    buf.push_back(x>>8);
    buf.push_back(x&0xFF);
}

template<uint32_t i>
uint8_t extract7bits(const uint32_t x) {
    return static_cast<uint8_t>((x >> (7 * i)) & ((1U << 7) - 1));
}

template<uint32_t i>
uint8_t extract7bitsmaskless(const uint32_t x) {
    return static_cast<uint8_t>((x >> (7 * i)));
}
        
inline void write_vbyte(sdsl::int_vector_buffer<8>& buf,uint32_t x) {
    if (x < (1U << 7)) {
        uint8_t b = static_cast<uint8_t>(x | (1U << 7));
        buf.push_back(b);
    } else if (x < (1U << 14)) {
        uint8_t b = extract7bits<0> (x);
        buf.push_back(b);
        b = extract7bitsmaskless<1> (x) | (1U << 7);
        buf.push_back(b);
    } else if (x < (1U << 21)) {
        uint8_t b = extract7bits<0> (x);
        buf.push_back(b);
        b = extract7bits<1> (x);
        buf.push_back(b);
        b = extract7bitsmaskless<2> (x) | (1U << 7);
        buf.push_back(b);
    } else if (x < (1U << 28)) {
        uint8_t b = extract7bits<0> (x);
        buf.push_back(b);
        b = extract7bits<1> (x);
        buf.push_back(b);
        b = extract7bits<2> (x);
        buf.push_back(b);
        b = extract7bitsmaskless<3> (x) | (1U << 7);
        buf.push_back(b);
    } else {
        uint8_t b = extract7bits<0> (x);
        buf.push_back(b);
        b = extract7bits<1> (x);
        buf.push_back(b);
        b = extract7bits<2> (x);
        buf.push_back(b);
        b = extract7bits<3> (x);
        buf.push_back(b);
        b = extract7bitsmaskless<4> (x) | (1U << 7);
        buf.push_back(b);
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
    
    std::string output_docids = args.collection_dir + "/" + KEY_DOCIDS + KEY_SUFFIX;
    std::string output_freqs = args.collection_dir + "/" + KEY_FREQS + KEY_SUFFIX;
    
    uint64_t num_postings = 0;
    LOG(INFO) << "writing docids";
    {
        sdsl::int_vector_buffer<8> docs_out(output_docids,std::ios::out,128*1024*1024);
        std::ifstream docfs(input_docids, std::ios::binary);
        
        
        read_uint32(docfs);
        uint32_t ndocs_d = read_uint32(docfs);
        LOG(INFO) << "inverted index contains " << ndocs_d << " documents";
        size_t list_id = 1;
        if(args.encoding == "u32") {
            LOG(INFO) << "encoding using u32";
            while(!docfs.eof()) {
                uint32_t list_len = read_uint32(docfs);
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t docid = read_uint32(docfs);
                    write_uint32(docs_out,docid);
                    num_postings++;
                }
                list_id++;
            }
        } else {
            LOG(INFO) << "encoding using vbyte";
            while(!docfs.eof()) {
                uint32_t list_len = read_uint32(docfs);
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t docid = read_uint32(docfs);
                    write_vbyte(docs_out,docid);
                    num_postings++;
                }
                list_id++;
            }
        }
        LOG(INFO) << "processed terms = " << list_id << std::endl;
    }
    LOG(INFO) << "writing freqs";
    {
        sdsl::int_vector_buffer<8> freqs_out(output_freqs,std::ios::out,128*1024*1024);
        std::ifstream freqsfs(input_freqs, std::ios::binary);
        
        size_t list_id = 1;
        if(args.encoding == "u32") {
            while(!freqsfs.eof()) {
                uint32_t list_len = read_uint32(freqsfs);
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t freq = read_uint32(freqsfs);
                    write_uint32(freqs_out,freq);
                }
                list_id++;
            }
        } else {
            LOG(INFO) << "encoding using vbyte";
            while(!freqsfs.eof()) {
                uint32_t list_len = read_uint32(freqsfs);
                for(uint32_t i=0;i<list_len;i++) {
                    uint32_t freq = read_uint32(freqsfs);
                    write_vbyte(freqs_out,freq);
                }
                list_id++;
            }
        }
        LOG(INFO) << "processed terms = " << list_id << std::endl;
    }

    LOG(INFO) << "num postings = " << num_postings << std::endl;
    {
        std::ofstream statsfs(args.collection_dir + "/" + KEY_COL_STATS);
        statsfs << std::to_string(num_postings) << std::endl;
    }

    return 0;
}
