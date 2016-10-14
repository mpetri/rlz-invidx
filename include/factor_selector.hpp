#pragma once

#include "utils.hpp"
#include "collection.hpp"

struct factor_select_first {
    static std::string type()
    {
        return "factor_select_first";
    }

    template <class t_index, class t_itr>
    static uint32_t pick_offset(const t_index& idx, const t_itr factor_itr)
    {
        if (idx.is_reverse()) {
            return idx.sa.size() - (idx.sa[factor_itr.sp] + factor_itr.len) - 1;
        }
        return idx.sa[factor_itr.sp];
    }
};
