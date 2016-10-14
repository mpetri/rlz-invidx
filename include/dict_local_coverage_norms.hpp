#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "logging.hpp"

#include "hashers.hpp"

#include <unordered_set>

using namespace std::chrono;
enum ACCESS_TYPE : int {
    SEQ,
    RAND
};

template <uint32_t t_block_size = 1024,
    uint32_t t_estimator_block_size = 16,
    uint32_t t_down_size = 512,
    class t_norm = std::ratio<1, 2>,
    ACCESS_TYPE t_method = SEQ>
class dict_local_coverage_norms {
public:
    static std::string type()
    {
        return "dict_local_coverage_norms-" + std::to_string(t_method) + "-" + std::to_string(t_block_size) + "-" + std::to_string(t_estimator_block_size);
    }

public:
    static sdsl::int_vector<8> create(std::string input_file,size_t dict_size_bytes,std::string name)
    {
        uint32_t budget_bytes = dict_size_bytes;
        uint32_t budget_mb = dict_size_bytes / (1024 * 1024);

        // check if we store it already and load it
        auto down_size = 512;
        auto start_total = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "create dictionary with budget " << budget_mb << " MiB";
        LOG(INFO) << "["<<name<<"] " << "block size = " << t_block_size;

        sdsl::read_only_mapper<8> input(input_file,true);
        auto n = input.size();
        size_t num_samples = budget_bytes / t_block_size; //hopefully much smaller than the adjusted
        size_t scale = n / budget_bytes; //hopefully much smaller than the adjusted, may not be divisible, can fix later
        size_t sample_step = scale * t_block_size;
        int thres = 1;
        if (scale >= 8 * 1024)
            thres = 16;
        else if (scale >= 1024 && scale < 8 * 1024)
            thres = 8;
        else if (scale >= 512 && scale < 1024)
            thres = 4; //
        else
            thres = 2; //minimum saving half

        size_t sample_step_adjusted = sample_step / thres / t_block_size * t_block_size; //make a tempate para later
        size_t num_samples_adjusted = n / sample_step_adjusted; //may contain more samples

        LOG(INFO) << "["<<name<<"] " << "dictionary samples to be picked = " << num_samples;
        LOG(INFO) << "["<<name<<"] " << "input epoch size = " << sample_step;
        LOG(INFO) << "["<<name<<"] " << "adjusted epoch size = " << sample_step_adjusted;
        LOG(INFO) << "["<<name<<"] " << "adjusted dictionary samples in the text = " << num_samples_adjusted;

        uint64_t rs_size = input.size() / down_size;
        std::vector<uint64_t> rs; //filter out frequency less than 64
        uint64_t seed = 4711;
        std::mt19937 gen(seed);
        auto start = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "building reservoir sample with downsize: " << down_size;
        std::uniform_real_distribution<double> dis(0.0f, 1.0f);

        double w = std::exp(std::log(dis(gen)) / rs_size);
        double s = std::floor(std::log(dis(gen)) / std::log(1-w));
            
        fixed_hasher<t_estimator_block_size> lrk;
        
        const uint8_t* ptr = (const uint8_t*) input.data();
        size_t i=0;
        for (; i < input.size(); i++) {
            auto hash = lrk.compute_hash(ptr);
            ptr++;
            rs.push_back(hash);
            if(rs.size() == rs_size) break;
        }
        size_t last = input.size() - t_estimator_block_size;
        for (; i <= last; i++) {
            auto hash = lrk.compute_hash(ptr);
            rs[1 + std::floor(rs_size * dis(gen))] = hash;
            w *= std::exp( std::log(dis(gen)) / rs_size );
            double rnd = std::log(dis(gen));
            s = std::floor( rnd / std::log(1-w) );
            i += (size_t) (s+1);
            ptr += (size_t) (s+1);
        }
        auto stop = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "reservoir sampling time = " 
        << duration_cast<milliseconds>(stop - start).count() / 1000.0f << " sec";

        LOG(INFO) << "["<<name<<"] " << "reservoir sample blocks = " << rs.size();
        LOG(INFO) << "["<<name<<"] " << "reservoir sample size = " << rs.size() * 8 / (1024 * 1024) << " MiB";
        //build exact counts of sampled elements
        LOG(INFO) << "["<<name<<"] " << "calculating exact frequencies of small rolling blocks...";
        std::unordered_map<uint64_t, uint32_t> mers_counts;
        mers_counts.max_load_factor(0.1);
        for (uint64_t s : rs) {
            mers_counts[s]++;
        }
        rs.clear(); //might be able to do it in place!!!!

        LOG(INFO) << "["<<name<<"] " << "useful kept small blocks no. = " << mers_counts.size();

        LOG(INFO) << "["<<name<<"] " << "first pass: getting random steps...";
        start = hrclock::now();

        std::vector<uint32_t> step_indices;
        //try ordered max cov from front to back, proven to be good for small datasets
        for (size_t i = 0; i < num_samples_adjusted; i++) {
            step_indices.push_back(i);
        }
        // //try randomly ordered max cov
        if (t_method == RAND)
            std::shuffle(step_indices.begin(), step_indices.end(),gen);

        stop = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "1st pass runtime = " << duration_cast<milliseconds>(stop - start).count() / 1000.0f << " sec";

        // 2nd pass: process max coverage using the sorted order by density
        std::unordered_set<uint64_t> step_mers; //can be prefilled?
        step_mers.max_load_factor(0.1);

        //prefill

        std::vector<uint64_t> picked_blocks;
        LOG(INFO) << "["<<name<<"] " << "second pass: perform ordered max coverage...";
        start = hrclock::now();
        double norm = (double)t_norm::num / t_norm::den;
        LOG(INFO) << "["<<name<<"] " << "computing norm = " << norm;
                      
        fixed_hasher<t_estimator_block_size> rk;
        std::unordered_set<uint64_t> best_local_mers;
        std::unordered_set<uint64_t> local_mers;
        local_mers.max_load_factor(0.1);
        for (const auto& i : step_indices) { //steps
            double sum_weights_max = std::numeric_limits<double>::min();
            uint64_t step_pos = i * sample_step_adjusted;
            uint64_t best_block_no = step_pos;
            best_local_mers.clear();

            for (size_t j = 0; j < sample_step_adjusted; j = j + t_block_size) { //blocks
                local_mers.clear();
                double sum_weights_current = 0;

                //computational expensive place
                const uint8_t* start = (const uint8_t*) input.data();
                const uint8_t* ptr = start + step_pos + j;
                for (size_t k = 0; k < t_block_size; k++) { //bytes?k=k+2?
                    auto hash = rk.compute_hash(ptr);
                    ptr++;
                    if (local_mers.find(hash) == local_mers.end() && step_mers.find(hash) == step_mers.end()) //continues rolling
                    { //expensive checking
                        if (mers_counts.find(hash) != mers_counts.end()) {
                            local_mers.emplace(hash);
                            auto freq = mers_counts[hash];
                            //compute norms
                            sum_weights_current += std::pow(freq, norm); //L0.5
                        }
                    }
                }
                //sum_weights_current = std::sqrt(sum_weights_current);
                if (norm > 0)
                    sum_weights_current = std::pow(sum_weights_current, 1 / norm);
                if (sum_weights_current >= sum_weights_max) {
                    sum_weights_max = sum_weights_current;
                    best_block_no = step_pos + j;
                    best_local_mers = std::move(local_mers);
                }
            }

            picked_blocks.push_back(best_block_no);
            if (picked_blocks.size() >= num_samples)
                break; //breakout if dict is filled since adjusted is bigger
            step_mers.insert(best_local_mers.begin(), best_local_mers.end());
        }
        LOG(INFO) << "["<<name<<"] " << "blocks size to check = " << step_mers.size();
        step_mers.clear(); //save mem
        step_indices.clear(); //
        mers_counts.clear();
        
        std::sort(picked_blocks.begin(), picked_blocks.end());
        stop = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "picked blocks = " << picked_blocks;
        LOG(INFO) << "["<<name<<"] " << "2nd pass runtime = " << duration_cast<milliseconds>(stop - start).count() / 1000.0f << " sec";

        // last pass: writing to dict
        LOG(INFO) << "["<<name<<"] " << "last: creating dictionary...";
        sdsl::int_vector<8> dict(dict_size_bytes);
        {
            size_t current = 0;
            for (const auto& pb : picked_blocks) {
                auto itr = input.begin() + pb;
                for(size_t i=0;i<t_block_size;i++) {
                    if(current == dict_size_bytes) break;
                    dict[current++] = *itr;
                    ++itr;
                }
                if(current == dict_size_bytes) break;
            }
        }

        LOG(INFO) << "["<<name<<"] " << "final dictionary size = " << dict.size() / (1024 * 1024) << " MiB";
        auto end_total = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << type() << " total time = " << duration_cast<milliseconds>(end_total - start_total).count() / 1000.0f << " sec";
        
        return dict;
    }
};
