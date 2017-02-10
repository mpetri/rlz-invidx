#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "collection.hpp"
#include "sdsl/int_vector_mapper.hpp"
#include "bit_coders.hpp"
#include "bit_streams.hpp"

#include "inverted_index.hpp"

#include <chrono>

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

bool index_exists(std::string col_dir)
{
	auto docs_file  = col_dir + "/" + DOCS_NAME;
	auto freqs_file = col_dir + "/" + FREQS_NAME;
	auto meta_file  = col_dir + "/" + META_NAME;
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
void bench_invidx(std::string input_prefix, std::string collection_dir)
{
	using timer		  = std::chrono::high_resolution_clock;
	using invidx_type = inverted_index<t_doc_list, t_freq_list>;
	invidx_type invidx_loaded;
	bool		verify = false;
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

	// pick 1000 random lists and benchmark
	std::mt19937 gen(4711);

	LOG(INFO) << "Picking list ids for benchmark...";
	std::vector<uint64_t> list_ids;
	std::vector<uint64_t> list_lens;
	std::vector<uint64_t> list_encoding_size;
	for (size_t i = 0; i < invidx_loaded.num_lists(); i++) {
		auto cur_len = invidx_loaded.list_len(i);
		if (cur_len > 128) {
			list_ids.push_back(i);
		}
	}
	std::shuffle(list_ids.begin(), list_ids.end(), gen);
	if (list_ids.size() > 1000000) list_ids.resize(1000000);
	for (size_t i = 0; i < list_ids.size(); i++) {
		auto cur_id		   = list_ids[i];
		auto cur_len	   = invidx_loaded.list_len(cur_id);
		auto cur_list_size = invidx_loaded.list_encoding_bits(cur_id);
		list_lens.push_back(cur_len);
		list_encoding_size.push_back(cur_list_size);
	}

	LOG(INFO) << "Perform 3 runs and take fastest";
	std::vector<std::chrono::nanoseconds> timings;
    size_t checksum = 0;
	for (size_t j = 0; j < 3; j++) {
		for (size_t i = 0; i < list_ids.size(); i++) {
			auto start = timer::now();
			const auto& list  = invidx_loaded[list_ids[i]];
			auto stop  = timer::now();
            checksum += list.list_len;

			if (timings.size() <= i) {
				timings.push_back(stop - start);
			} else {
				if (timings[i] > (stop - start)) {
					timings[i] = (stop - start);
				}
			}
		}
	}
    LOG(INFO) << "Checksum = " << checksum;

	for (size_t i = 0; i < timings.size(); i++) {
		LOG(INFO) << t_doc_list::name() << ";" << i << ";" << list_lens[i] << ";"
				  << list_encoding_size[i] << ";" << timings[i].count();
	}
}

int main(int argc, const char* argv[])
{
	setup_logger(argc, argv);

	cmdargs_t args = parse_args(argc, argv);

	LOG(INFO) << "method;id;postings;space_bits;time_ns";

	{
		using doc_list_type  = list_vbyte<true>;
		using freq_list_type = list_vbyte<false>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	{
		using doc_list_type  = list_simple16<true>;
		using freq_list_type = list_simple16<false>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	{
		using doc_list_type  = list_op4<128, true>;
		using freq_list_type = list_op4<128, false>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	{
		using doc_list_type  = list_ef<false>;
		using freq_list_type = list_ef<true>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	{
		using doc_list_type  = list_interp<false>;
		using freq_list_type = list_interp<true>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	// {
	// 	using doc_list_type  = list_interp_block<128, false>;
	// 	using freq_list_type = list_interp_block<128, true>;
	// 	bench_invidx<doc_list_type, freq_list_type>(
	// 	args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	// }
	{
		using doc_list_type  = list_u32<true>;
		using freq_list_type = list_u32<false>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	{
		using doc_list_type  = list_vbyte_lz<true, 128, coder::zstd<9>>;
		using freq_list_type = list_vbyte_lz<false, 128, coder::zstd<9>>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	{
	 	using doc_list_type  = list_s16_lz<true, 128, coder::zstd<9>>;
	 	using freq_list_type = list_s16_lz<false, 128, coder::zstd<9>>;
	 	bench_invidx<doc_list_type, freq_list_type>(
	 	args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	{
		using doc_list_type  = list_u32_lz<true, 128, coder::zstd<9>>;
		using freq_list_type = list_u32_lz<false, 128, coder::zstd<9>>;
		bench_invidx<doc_list_type, freq_list_type>(
		args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	}
	// {
	// 	using doc_list_type  = list_u32_lz<true, 128, coder::lzma<6>>;
	// 	using freq_list_type = list_u32_lz<false, 128, coder::lzma<6>>;
	// 	bench_invidx<doc_list_type, freq_list_type>(
	// 	args.input_prefix, args.collection_dir + "-" + doc_list_type::name());
	// }
	return 0;
}
