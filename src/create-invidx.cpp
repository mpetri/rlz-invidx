
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"
#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "inverted_index.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

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


int main(int argc, const char* argv[])
{
    setup_logger(argc, argv);

    cmdargs_t args = parse_args(argc, argv);

    {
        // (1) build
        inverted_index<list_vbyte<true>,list_vbyte<false>> invidx(args.input_prefix);
        invidx.write(args.collection_dir);
        
        // (2) print stats
        invidx.stats();
        
        // (2) verify against in-memory
        inverted_index<list_vbyte<true>,list_vbyte<false>> invidx_loaded;
        invidx_loaded.read(args.collection_dir);
        invidx_loaded.stats();
        
        if( invidx !=  invidx_loaded) {
            LOG(ERROR) << "Error recovering index";
        }
        
        // // (3) verify against original input
        invidx_loaded.verify(args.input_prefix);
    }
    
    return 0;
}
