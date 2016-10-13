#pragma once

#include <chrono>

#include "utils.hpp"

using namespace std::chrono;

template <class t_idx>
bool verify_index(collection& col, t_idx& idx,std::string name )
{
    LOG(INFO) << "["<<name<<"] " << "Verify that factorization is correct.";
    sdsl::read_only_mapper<8> docids(col.file_map[KEY_DOCIDS]);
    auto num_blocks = docids.size() / t_idx::block_size;

    bool derror = false;
    for (size_t i = 0; i < num_blocks; i++) {
        auto block_content = idx.block_docs(i);
        auto block_start = i * t_idx::block_size;
        if (block_content.size() != t_idx::block_size) {
            derror = true;
            LOG_N_TIMES(100, ERROR) << "["<<name<<"] " << "Error in doc block " << i
                                    << " block size = " << block_content.size()
                                    << " encoding block_size = " << t_idx::block_size;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), docids.begin() + block_start);
        if (!eq) {
            derror = true;
            LOG(ERROR) << "["<<name<<"] " << "DOC BLOCK " << i << " NOT EQUAL";
            for (size_t j = 0; j < t_idx::block_size; j++) {
                if (docids[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)docids[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
            exit(EXIT_FAILURE);
        }
    }
    auto left = docids.size() % t_idx::block_size;
    if (left) {
        auto block_content = idx.block_docs(num_blocks);
        auto block_start = num_blocks * t_idx::block_size;
        if (block_content.size() != left) {
            derror = true;
            LOG(ERROR) << "["<<name<<"] " << "Error in LAST DOC block "
                       << " block size = " << block_content.size()
                       << " left  = " << left;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), docids.begin() + block_start);
        if (!eq) {
            derror = true;
            LOG(ERROR) << "["<<name<<"] " << "LAST DOC BLOCK IS NOT EQUAL";
            for (size_t j = 0; j < left; j++) {
                if (docids[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "["<<name<<"] " << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)docids[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
        }
    }
    if (!derror) {
        LOG(INFO) << "["<<name<<"] " << "SUCCESS! docids sucessfully recovered.";
    }
    
    sdsl::read_only_mapper<8> freqs(col.file_map[KEY_FREQS]);
    num_blocks = freqs.size() / t_idx::block_size;

    bool ferror = false;
    for (size_t i = 0; i < num_blocks; i++) {
        auto block_content = idx.block_freqs(i);
        auto block_start = i * t_idx::block_size;
        if (block_content.size() != t_idx::block_size) {
            ferror = true;
            LOG_N_TIMES(100, ERROR) << "["<<name<<"] " << "Error in freq block " << i
                                    << " block size = " << block_content.size()
                                    << " encoding block_size = " << t_idx::block_size;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), docids.begin() + block_start);
        if (!eq) {
            ferror = true;
            LOG(ERROR) << "["<<name<<"] " << "FREQ BLOCK " << i << " NOT EQUAL";
            for (size_t j = 0; j < t_idx::block_size; j++) {
                if (freqs[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)freqs[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
            exit(EXIT_FAILURE);
        }
    }
    left = freqs.size() % t_idx::block_size;
    if (left) {
        auto block_content = idx.block_freqs(num_blocks);
        auto block_start = num_blocks * t_idx::block_size;
        if (block_content.size() != left) {
            ferror = true;
            LOG(ERROR) << "["<<name<<"] " << "Error in  LAST FREQ block "
                       << " block size = " << block_content.size()
                       << " left  = " << left;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), freqs.begin() + block_start);
        if (!eq) {
            ferror = true;
            LOG(ERROR) << "["<<name<<"] " << "LAST FREQ BLOCK IS NOT EQUAL";
            for (size_t j = 0; j < left; j++) {
                if (freqs[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)freqs[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
        }
    }
    if (!ferror) {
        LOG(INFO) << "["<<name<<"] " << "SUCCESS! freqs sucessfully recovered.";
        return true;
    }
        
    return !ferror && !derror;
}
