#pragma once

#include "utils.hpp"
#include "logging.hpp"

#include "sdsl/io.hpp"
#include "sdsl/construct.hpp"
#include "sdsl/int_vector_mapped_buffer.hpp"
#include "sdsl/int_vector_mapper.hpp"

const std::string KEY_SUFFIX = ".rlzi";
const std::string KEY_DOCIDS = "docs";
const std::string KEY_FREQS = "freqs";
const std::string KEY_DICT_DOCIDS = "docsdict";
const std::string KEY_DICT_FREQS = "freqsdict";
const std::string KEY_FACTORIZED_DOCS = "docfactors";
const std::string KEY_BLOCKMAP_DOCS = "blockmap_docs";
const std::string KEY_BLOCKMAP_FREQS = "blockmap_freqs";
const std::string KEY_LZ_DOCS = "lzdocs";
const std::string KEY_LZ_FREQS = "lzfreqs";
const std::string KEY_COL_STATS = "col_stats.txt";

struct collection {
    uint64_t num_postings;
    std::string path;
    std::map<std::string, std::string> param_map;
    std::map<std::string, std::string> file_map;
    collection(const std::string& p)
        : path(p + "/")
    {
        if (!utils::directory_exists(path)) {
            throw std::runtime_error("Collection path not found.");
        }
        // make sure all other dirs exist
        std::string index_directory = path + "/index/";
        utils::create_directory(index_directory);
        std::string tmp_directory = path + "/tmp/";
        utils::create_directory(tmp_directory);
        std::string results_directory = path + "/results/";
        utils::create_directory(results_directory);

        /* make sure the necessary files are present */
        auto docs_file_name = path + "/" + KEY_DOCIDS + KEY_SUFFIX;
        file_map[KEY_DOCIDS] = docs_file_name;
        if (!utils::file_exists(path + "/" + KEY_DOCIDS + KEY_SUFFIX)) {
            LOG(FATAL) << "Collection path does not contain docids.";
            throw std::runtime_error("Collection path does not contain docids.");
        }
        else {
            sdsl::int_vector_mapped_buffer<8> docids(file_map[KEY_DOCIDS]);
            LOG(INFO) << "Found input docids with size " << docids.size() / (1024.0 * 1024.0) << " MiB";
        }
        
        auto freqs_file_name = path + "/" + KEY_FREQS + KEY_SUFFIX;
        file_map[KEY_FREQS] = freqs_file_name;
        if (!utils::file_exists(path + "/" + KEY_FREQS + KEY_SUFFIX)) {
            LOG(FATAL) << "Collection path does not contain freqs.";
            throw std::runtime_error("Collection path does not contain freqs.");
        }
        else {
            sdsl::int_vector_mapped_buffer<8> freqs(file_map[KEY_FREQS]);
            LOG(INFO) << "Found input freqs with size " << freqs.size() / (1024.0 * 1024.0) << " MiB";
        }
        
        {
            std::ifstream statsfs(path + "/" + KEY_COL_STATS);
            statsfs >> num_postings;
            LOG(INFO) << "Num postings = " << num_postings;
        }
    }

    std::string compute_dict_hash_docids(std::string key)
    {
        auto file_name = file_map[key];
        sdsl::read_only_mapper<8> dict(file_name);
        auto crc32 = utils::crc((const uint8_t*)dict.data(), dict.size());
        return std::to_string(crc32);
    }

    std::string temp_file_name(const std::string& key, size_t offset)
    {
        auto file_name = path + "/tmp/" + key + "-" + std::to_string(offset) + "-" + std::to_string(getpid()) + ".sdsl";
        return file_name;
    }

    void clear()
    {
        auto tmp_folder = path + "/tmp/";
        utils::remove_all_files_in_dir(tmp_folder);
    }
};
