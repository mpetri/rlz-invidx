#pragma once

#include "utils.hpp"
#include "collection.hpp"

template <uint32_t t_block_size_bytes>
class dict_uniform_sample_budget {
public:
    static std::string type()
    {
        return "dict_uniform_sample_budget-"
            + std::to_string(t_block_size_bytes);
    }

public:
    static sdsl::int_vector<8> create(std::string input_file,size_t dict_size_bytes,std::string name)
    {
        const uint32_t block_size = t_block_size_bytes;
        uint64_t budget_bytes = dict_size_bytes;
        uint64_t budget_mb = budget_bytes / (1024 * 1024);
        auto start_total = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "\tcreate dictionary with budget " << budget_mb << " MiB";
        // memory map the text and iterate over it
        sdsl::int_vector<8> dict(dict_size_bytes);
        {
            sdsl::read_only_mapper<8> text(input_file,true);
            auto num_samples = budget_bytes / block_size;
            LOG(INFO) << "["<<name<<"] " << "\tdictionary samples = " << num_samples;
            auto n = text.size();
            size_t sample_step = n / num_samples;
            LOG(INFO) << "["<<name<<"] " << "\tsample steps = " << sample_step;
            size_t idx = 0;
            for (size_t i = 0; i < n; i += sample_step) {
                for (size_t j = 0; j < block_size; j++) {
                    if (i + j >= n)
                        break;
                    dict[idx++] = text[i + j];
                }
            }
        }
        auto end_total = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "\t" << type() + " total time = " << duration_cast<milliseconds>(end_total - start_total).count() / 1000.0f << " sec";
        return dict;
    }
};
