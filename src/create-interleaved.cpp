
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"
#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "inverted_index.hpp"
#include "interleaved_inverted_index.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

typedef struct cmdargs {
	std::string collection_dir;
	std::string input_prefix;
} cmdargs_t;

void print_usage(const char* program)
{
	fprintf(stdout, "%s -c <collection directory> -i <input prefix>\n", program);
	fprintf(stdout, "where\n");
	fprintf(stdout, "  -c <collection directory>  : the directory the collection is stored.\n");
	fprintf(stdout, "  -i <input prefix>          : the d2si input prefix.\n");
};

cmdargs_t parse_args(int argc, const char* argv[])
{
	cmdargs_t args;
	int		  op;
	args.collection_dir = "";
	args.input_prefix   = "";
	while ((op = getopt(argc, (char* const*)argv, "c:i:")) != -1) {
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

bool interleaved_index_exists(std::string col_dir)
{
	auto docfreqs_file = col_dir + "/" + DOCFREQS_NAME;
	auto meta_file	 = col_dir + "/" + META_NAME;
	if (!utils::file_exists(docfreqs_file)) {
		return false;
	}
	if (!utils::file_exists(meta_file)) {
		return false;
	}
	return true;
}

template <class t_list>
void build_and_verify(std::string input_prefix, std::string collection_dir)
{
	using invidx_type = inverted_index<list_vbyte<true>, list_vbyte<false>>;
	invidx_type invidx(input_prefix);
	bool		verify = false;

	using interleaved_type = interleaved_inverted_index<t_list>;
	if (!interleaved_index_exists(collection_dir)) {
		LOG(INFO) << "building inverted index (" << invidx_type::type() << ")";
		interleaved_type invidx(invidx);
		LOG(INFO) << "write inverted index";
		invidx.write(collection_dir);
		verify = true;
	}
	LOG(INFO) << "load inverted index (" << invidx_type::type() << ")";
	interleaved_type invidx_loaded;
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
}

int main(int argc, const char* argv[])
{
	setup_logger(argc, argv);

	cmdargs_t args = parse_args(argc, argv);

	{
		using list_type = list_vbyte_lz<false, 128, coder::zstd<9>>;
		build_and_verify<list_type>(args.input_prefix,
									args.collection_dir + "-il-" + list_type::name());
	}
	{
		using list_type = list_vbyte_lz<false, 128, coder::lzma<6>>;
		build_and_verify<list_type>(args.input_prefix,
									args.collection_dir + "-il-" + list_type::name());
	}
	{
		using list_type = list_vbyte_lz<false, 128, coder::bzip2<9>>;
		build_and_verify<list_type>(args.input_prefix,
									args.collection_dir + "-il-" + list_type::name());
	}
	return 0;
}
