#define ELPP_THREAD_SAFE
#define ELPP_STL_LOGGING

#include "utils.hpp"
#include "collection.hpp"

#include "logging.hpp"
INITIALIZE_EASYLOGGINGPP

#include "sdsl/io.hpp"
#include "divsufsort64.h"

sdsl::int_vector<32>
compute_lcp(sdsl::int_vector<8>& text,sdsl::int_vector<64>& SA)
{
	//	(1) Calculate PHI (stored in array plcp)
	LOG(INFO) << "construct PHI";
	size_t n = SA.size();
    sdsl::int_vector<64> plcp(n);
    for (size_t i=0, sai_1 = 0; i < n; ++i) {
        size_t sai = SA[i];
        plcp[ sai ] = sai_1;
        sai_1 = sai;
    }

	//  (2) Calculate permuted LCP array (text order), called PLCP
	LOG(INFO) << "construct PLCP";
    size_t max_l = 0;
    for (size_t i=0, l=0; i < n-1; ++i) {
        size_t phii = plcp[i];
        while (text[i+l] == text[phii+l]) {
            ++l;
        }
        plcp[i] = l;
        if (l) {
            max_l = std::max(max_l, l);
            --l;
        }
    }

	//	(3) Transform PLCP into LCP
	LOG(INFO) << "Transform PLCP to LCP";
    sdsl::int_vector<32> LCP(n);
    LCP[0] = 0;
    for (size_t i=1; i < n; ++i) {
        size_t sai = SA[i];
        LCP[i] = plcp[sai];
    }
	return LCP;
}

void
compute_stats(std::string file_name,std::string name) {
	// (1) load text
	sdsl::int_vector<8> T;
	sdsl::load_vector_from_file(T,file_name);
	size_t n = T.size();
	// (2) construct SA
	LOG(INFO) << "construct SA";
	sdsl::int_vector<64> SA(n);
	divsufsort64((const unsigned char*)T.data(), (int64_t*)SA.data(), n);
	// (3) construct LCP
	LOG(INFO) << "construct LCP";
	auto LCP = compute_lcp(T,SA);

	// (4) compute LCP list
	std::vector<uint32_t> lcp_hist(5000000,0);
	for(size_t i=0;i<LCP.size();i++) {
		lcp_hist[ LCP[i] ]++;
	}
	{
		std::ofstream histout("lcp-hist-"+name+".csv");
		for(size_t i=0;i<lcp_hist.size();i++) {
			if(lcp_hist[i] != 0) histout << i << "," << lcp_hist[i] << std::endl;
		}
	}

	// (5) pick a few megabytes and output all
	{
		std::ofstream lcpsnapout("lcp-full-"+name+"-10k.csv");
		for(size_t i=0;i<10;i++) {
			auto start = rand() % (LCP.size() - (1024*1024));
			for(size_t j=0;j<(10000);j++) {
				lcpsnapout << i << "," << start+j << "," << LCP[start+j] << std::endl;
			}
		}
	}
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


    compute_stats(col.docs_file,"docs");

    compute_stats(col.freqs_file,"freqs");

    return EXIT_SUCCESS;
}
