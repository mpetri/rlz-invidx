#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "bit_coders.hpp"
#include "bit_streams.hpp"
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"

#include "inverted_index.hpp"

#include <chrono>
#include <unordered_set>

typedef struct cmdargs {
  std::string collection_dir;
  std::string input_prefix;
} cmdargs_t;

void print_usage(const char *program) {
  fprintf(stdout, "%s -c <collection directory> -i <input prefix>\n", program);
  fprintf(stdout, "where\n");
  fprintf(stdout, "  -c <collection directory>  : the directory the collection "
                  "is stored.\n");
  fprintf(stdout, "  -i <input prefix>          : the d2si input prefix.\n");
};

cmdargs_t parse_args(int argc, const char *argv[]) {
  cmdargs_t args;
  int op;
  args.collection_dir = "";
  args.input_prefix = "";
  while ((op = getopt(argc, (char *const *)argv, "c:i:")) != -1) {
    switch (op) {
    case 'c':
      args.collection_dir = optarg;
      break;
    case 'i':
      args.input_prefix = optarg;
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

bool index_exists(std::string col_dir) {
  auto docs_file = col_dir + "/" + DOCS_NAME;
  auto freqs_file = col_dir + "/" + FREQS_NAME;
  auto meta_file = col_dir + "/" + META_NAME;
  if (!utils::file_exists(docs_file)) {
    return false;
  }
  if (!utils::file_exists(freqs_file)) {
    return false;
  }
  if (!utils::file_exists(meta_file)) {
    return false;
  }
  return true;
}

template <class t_doc_list, class t_freq_list>
void bench_invidx(std::string input_prefix, std::string collection_dir) {
  using timer = std::chrono::high_resolution_clock;
  using invidx_type = inverted_index<t_doc_list, t_freq_list>;
  invidx_type invidx_loaded;
  bool verify = false;
  if (!index_exists(collection_dir)) {
    LOG(INFO) << "building inverted index (" << invidx_type::type() << ")";
    invidx_type invidx(input_prefix);
    LOG(INFO) << "write inverted index";
    invidx.write(collection_dir);
    verify = true;
  }
  LOG(INFO) << "load inverted index (" << invidx_type::type() << ")";
  invidx_loaded.read(collection_dir);
  invidx_loaded.stats();
  if (verify) {
    LOG(INFO) << "verify loaded index against input data";
    if (!invidx_loaded.verify(input_prefix)) {
      LOG(ERROR) << "Error verifying index";
    } else {
      LOG(INFO) << "[OK] loaded index identical to input data";
    }
  }

  std::mt19937 gen(4711);
  std::uniform_int_distribution<uint64_t> dis(0, invidx_loaded.num_lists());
  std::cout << 100000 << std::endl;
  std::unordered_set<uint32_t> picked_ids;
  for(size_t i=0;i<1000000000;i++) {
    if( picked_ids.size() == 100000) break;
    auto pos = dis(gen);
    auto lst = invidx_loaded[pos];
    if( lst.list_len <= 128) continue;
    if( picked_ids.count(pos) != 0) continue;
    picked_ids.insert(pos);
    std::cout << lst.list_len << std::endl;
    std::cout << lst.doc_ids[0] << std::endl;
    for(size_t j=1;j<lst.list_len;j++) {
        std::cout << lst.doc_ids[j] - lst.doc_ids[j-1] << std::endl;
    }
  }
  std::cerr << "found " << picked_ids.size() << " lists";
}

int main(int argc, const char *argv[]) {
  setup_logger(argc, argv);

  cmdargs_t args = parse_args(argc, argv);

  {
    using doc_list_type = list_vbyte<true>;
    using freq_list_type = list_vbyte<false>;
    bench_invidx<doc_list_type, freq_list_type>(
        args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
  }
  return 0;
}
