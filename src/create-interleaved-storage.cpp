
#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"
#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "interleaved_storage_index.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

typedef struct cmdargs {
	std::string col_dir;
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
	args.col_dir	  = "";
	args.input_prefix = "";
	while ((op = getopt(argc, (char* const*)argv, "c:i:")) != -1) {
		switch (op) {
			case 'c':
				args.col_dir = optarg;
				break;
			case 'i':
				args.input_prefix = optarg;
				break;
		}
	}
	if (args.col_dir == "" || args.input_prefix == "") {
		std::cerr << "Missing command line parameters.\n";
		print_usage(argv[0]);
		exit(EXIT_FAILURE);
	}
	return args;
}

bool index_exists(std::string col_dir)
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

template <class t_transform, class t_compress>
void build_and_verify(std::string input_prefix, std::string collection_dir)
{
	using idx_type = interleaved_storage_index<t_transform, t_compress>;
	idx_type idx_loaded;
	bool	 verify = false;
	if (!index_exists(collection_dir)) {
		LOG(INFO) << "building storage index (" << idx_type::type() << ")";
		idx_type idx(input_prefix);
		LOG(INFO) << "write storage index";
		idx.write(collection_dir);
		verify = true;
	}
	LOG(INFO) << "load storage index (" << idx_type::type() << ")";
	idx_loaded.read(collection_dir);
	idx_loaded.stats();
	if (verify) {
		LOG(INFO) << "verify loaded index against input data";
		if (!idx_loaded.verify(input_prefix)) {
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
		using trans_t	= coder::vbyte_fastpfor;
		using compress_t = coder::zstd<9>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::aligned_fixed<uint32_t>;
		using compress_t = coder::zstd<9>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::simple16;
		using compress_t = coder::zstd<9>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}


	{
		using trans_t	= coder::vbyte_fastpfor;
		using compress_t = coder::bzip2<9>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::aligned_fixed<uint32_t>;
		using compress_t = coder::bzip2<9>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::simple16;
		using compress_t = coder::bzip2<9>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}


	{
		using trans_t	= coder::vbyte_fastpfor;
		using compress_t = coder::lzma<6>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::aligned_fixed<uint32_t>;
		using compress_t = coder::lzma<6>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::simple16;
		using compress_t = coder::lzma<6>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}

	{
		using trans_t	= coder::vbyte_fastpfor;
		using compress_t = coder::aligned_fixed<uint8_t>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::aligned_fixed<uint32_t>;
		using compress_t = coder::aligned_fixed<uint8_t>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}
	{
		using trans_t	= coder::simple16;
		using compress_t = coder::aligned_fixed<uint8_t>;
		std::string col_dir =
		args.col_dir + "-istore-" + trans_t::type() + "-" + compress_t::type();
		build_and_verify<trans_t, compress_t>(args.input_prefix, col_dir);
	}


	return 0;
}
