#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "rlz_store.hpp"

template <class t_dictionary_creation_strategy,
    uint32_t t_factorization_block_size,
    class t_factor_coder>
class rlz_store<t_dictionary_creation_strategy,
    t_factorization_block_size,
    t_factor_coder>::builder {
public:
    using dictionary_creation_strategy = t_dictionary_creation_strategy;
    using factor_encoder = t_factor_coder;
    using factorization_strategy = factorizor<t_factorization_block_size, factor_encoder>;
    using block_map_type = block_map_uncompressed<true>;
    enum { block_size = t_factorization_block_size };

public:
    builder& set_rebuild(bool r)
    {
        rebuild = r;
        return *this;
    };
    builder& set_threads(uint8_t nt)
    {
        num_threads = nt;
        return *this;
    };
    builder& set_dict_size(uint64_t ds)
    {
        dict_size_bytes = ds;
        return *this;
    };

    rlz_store build_or_load(collection& col,std::string input_file,std::string name) const
    {
        auto start = hrclock::now();
        auto input_hash = utils::crc(input_file);

        // (1) create dictionary based on parametrized dictionary creation strategy if necessary
        auto dict_file_name = col.file_name(input_hash,dictionary_creation_strategy::type());
        sdsl::int_vector<8> dict;
        if (rebuild || !utils::file_exists(dict_file_name)) {
            dict = dictionary_creation_strategy::create(input_file, dict_size_bytes,name);
            sdsl::store_to_file(dict,dict_file_name);
        } else {
            sdsl::load_from_file(dict,dict_file_name);
        }
        auto dict_hash = utils::crc(dict);

        // (1) create factorized text using the dict
        auto hash = dict_hash xor input_hash;
        auto factor_file_name = col.file_name(hash,factorization_strategy::type());
        if (rebuild || !utils::file_exists(factor_file_name)) {
            factorization_strategy::parallel_factorize(col,input_file,dict,hash,num_threads,name);
        }
        else {
            LOG(INFO) << "["<<name<<"] " << "factorized text exists.";
        }

        auto stop = hrclock::now();
        LOG(INFO) << "["<<name<<"] " << "rlz construction complete. time = " << duration_cast<seconds>(stop - start).count() << " sec";

        return rlz_store(col,input_file,dict_hash,input_hash,name);
    }
private:
    bool rebuild = false;
    uint32_t num_threads = 1;
    uint64_t dict_size_bytes = 0;
};
