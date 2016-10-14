#pragma once

#include "utils.hpp"
#include "collection.hpp"

#include "rlz_store_static.hpp"

template <class t_dictionary_creation_strategy,
    class t_dictionary_index,
    uint32_t t_factorization_block_size,
    bool t_search_local_block_context,
    class t_factor_selection_strategy,
    class t_factor_coder,
    class t_block_map>
class rlz_store_static<t_dictionary_creation_strategy,
    t_dictionary_index,
    t_factorization_block_size,
    t_search_local_block_context,
    t_factor_selection_strategy,
    t_factor_coder,
    t_block_map>::builder {
public:
    using dictionary_creation_strategy = t_dictionary_creation_strategy;
    using dictionary_index_type = t_dictionary_index;
    using factor_selection_strategy = t_factor_selection_strategy;
    using factor_encoder = t_factor_coder;
    using factorization_strategy = factorizor<t_factorization_block_size, t_search_local_block_context, dictionary_index, factor_selection_strategy, factor_encoder>;
    using block_map = t_block_map;
    enum { block_size = t_factorization_block_size };
    enum { search_local_block_context = t_search_local_block_context };

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

    static std::string blockmap_file_name(collection& col)
    {
        return col.path + "/index/" + KEY_BLOCKMAP + "-"
            + block_map_type::type() + "-" + factorization_strategy::type() + "-"
            + col.param_map[PARAM_DICT_HASH] + ".sdsl";
    }

    rlz_store_static build_or_load(collection& col,std::string name) const
    {
        auto start = hrclock::now();

        // (1) create dictionary based on parametrized
        // dictionary creation strategy if necessary
        LOG(INFO) << "Create dictionary for docids (" << dictionary_creation_strategy::type() << ")";
        dictionary_creation_strategy::create(col, rebuild, dict_size_bytes);
        LOG(INFO) << "Dictionary hash '" << col.param_map[PARAM_DICT_HASH] << "'";

        LOG(INFO) << "Create dictionary for freqs (" << dictionary_creation_strategy::type() << ")";
        dictionary_creation_strategy::create(col, rebuild, dict_size_bytes);
        LOG(INFO) << "Dictionary hash '" << col.param_map[PARAM_DICT_HASH] << "'";

        // (3) create factorized text using the dict
        auto factor_file_name = factorization_strategy::factor_file_name(col);
        if (rebuild || !utils::file_exists(factor_file_name)) {
            factorization_strategy::template parallel_factorize<factor_storage>(col, rebuild, num_threads);
        }
        else {
            LOG(INFO) << "Factorized text exists.";
            col.file_map[KEY_FACTORIZED_TEXT] = factor_file_name;
            col.file_map[KEY_BLOCKMAP] = factorization_strategy::boffsets_file_name(col);
        }

        // (4) encode document start pos
        LOG(INFO) << "Create block map (" << block_map_type::type() << ")";
        auto blockmap_file = blockmap_file_name(col);
        if (rebuild || !utils::file_exists(blockmap_file)) {
            block_map_type tmp(col);
            sdsl::store_to_file(tmp, blockmap_file);
        }
        col.file_map[KEY_BLOCKMAP] = blockmap_file;

        auto stop = hrclock::now();
        LOG(INFO) << "RLZ construction complete. time = " << duration_cast<seconds>(stop - start).count() << " sec";

        return rlz_store_static(col);
    }

private:
    bool rebuild = false;
    uint32_t num_threads = 1;
    uint64_t dict_size_bytes = 0;
};
