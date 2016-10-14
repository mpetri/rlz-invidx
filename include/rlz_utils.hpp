#pragma once

#include <chrono>

#include "utils.hpp"

using namespace std::chrono;

template <class t_idx>
bool verify_index(std::string input_file,t_idx& idx)
{
    LOG(INFO) << "[" << idx.name << "] Verify that factorization is correct.";
    sdsl::read_only_mapper<8> text(input_file,true);
    auto num_blocks = text.size() / t_idx::block_size;

    bool error = false;
    for (size_t i = 0; i < num_blocks; i++) {
        auto block_content = idx.block(i);
        auto block_start = i * t_idx::block_size;
        if (block_content.size() != t_idx::block_size) {
            error = true;
            LOG_N_TIMES(100, ERROR) << "Error in block " << i
                                    << " block size = " << block_content.size()
                                    << " encoding block_size = " << t_idx::block_size;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), text.begin() + block_start);
        if (!eq) {
            error = true;
            LOG(ERROR) << "[" << idx.name << "]BLOCK " << i << " NOT EQUAL";
            for (size_t j = 0; j < t_idx::block_size; j++) {
                if (text[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)text[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
            exit(EXIT_FAILURE);
        }
    }
    auto left = text.size() % t_idx::block_size;
    if (left) {
        auto block_content = idx.block(num_blocks);
        auto block_start = num_blocks * t_idx::block_size;
        if (block_content.size() != left) {
            error = true;
            LOG(ERROR) << "[" << idx.name << "] Error in  LAST block "
                       << " block size = " << block_content.size()
                       << " left  = " << left;
        }
        auto eq = std::equal(block_content.begin(), block_content.end(), text.begin() + block_start);
        if (!eq) {
            error = true;
            LOG(ERROR) << "[" << idx.name << "] LAST BLOCK IS NOT EQUAL";
            for (size_t j = 0; j < left; j++) {
                if (text[block_start + j] != block_content[j]) {
                    LOG_N_TIMES(100, ERROR) << "Error at pos " << j << "(" << block_start + j << ") should be '"
                                            << (int)text[block_start + j] << "' is '" << (int)block_content[j] << "'";
                }
            }
        }
    }
    if (!error) {
        LOG(INFO) << "[" << idx.name << "] SUCCESS! Text sucessfully recovered.";
        return true;
    }
    return false;
}


