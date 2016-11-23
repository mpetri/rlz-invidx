#pragma once

#include "utils.hpp"

#include "sdsl/int_vector_mapper.hpp"

struct collection {
    std::string path;
    std::map<std::string, std::string> file_map;
    collection(const std::string& p)
        : path(p + "/")
    {
        if (!utils::directory_exists(path)) {
            throw std::runtime_error("collection path not found.");
        }
    }

    std::string file_name(uint32_t hash,std::string key)
    {
        return path + "/" + std::to_string(hash) + "." + key;
    }

};

#define DOCS_NAME "raw_data.docs"
#define FREQS_NAME "raw_data.freqs"
#define META_NAME "raw_data.meta"

struct invidx_collection : public collection {
    invidx_collection(const std::string& p)
        : collection(p)
    {
        docs_file = path + "/" + DOCS_NAME;
        freqs_file = path + "/" + FREQS_NAME;
        meta_file = path + "/" + META_NAME;
        if( !utils::file_exists(docs_file) ) {
            throw std::runtime_error("docs file not found.");
        }
        if( !utils::file_exists(freqs_file) ) {
            throw std::runtime_error("freqs file not found.");
        }
        if( !utils::file_exists(meta_file) ) {
            throw std::runtime_error("stats file not found.");
        }
    }
    std::string docs_file;
    std::string freqs_file;
    std::string meta_file;
};
