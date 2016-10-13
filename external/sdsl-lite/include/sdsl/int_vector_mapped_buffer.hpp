/* sdsl - succinct data structures library
    Copyright (C) 2008-2013 Simon Gog

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see http://www.gnu.org/licenses/ .
*/
/*! \file int_vector_mapped_buffer.hpp
    \brief int_vector_mapped_buffer.hpp contains the sdsl::int_vector_mapped_buffer class.
    \author Matthias Petri
*/
#ifndef INCLUDED_INT_VECTOR_MAPPED_BUFFER
#define INCLUDED_INT_VECTOR_MAPPED_BUFFER

#include "int_vector.hpp"
#include "iterators.hpp"
#include <cassert>
#include <fstream>
#include <iostream>
#include <stdio.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace sdsl
{

template <
uint8_t t_width = 0,
std::ios_base::openmode t_mode = std::ios_base::in, // default in only use std::ios_base::in|std::ios_base::out for R&W
uint64_t t_buffer_elems = 1024*1024
>
class int_vector_mapped_buffer
{
    static_assert(t_width <= 64,
                  "int_vector_mapped_buffer: width must be at most 64 bits.");
    public:
        typedef typename int_vector<t_width>::difference_type difference_type;
        typedef typename int_vector<t_width>::value_type value_type;
        typedef typename int_vector<t_width>::size_type size_type;
        typedef typename int_vector<t_width>::int_width_type width_type;
        typedef random_access_const_iterator<int_vector_mapped_buffer<t_width,t_mode,t_buffer_elems>> const_iterator;
        typedef typename int_vector<t_width>::const_reference const_reference;
        typedef typename int_vector<t_width>::reference reference;
    public:
        const size_type append_block_size = 1000000;
    private:
        mutable uint8_t* m_mapped_data = nullptr;
        uint64_t m_file_size_bytes = 0;
        off_t m_data_offset = 0;
        int m_fd = -1;
        mutable int_vector<t_width> m_buffer;
        std::string m_file_name;
        bool m_is_plain = false;
        uint64_t m_buffer_size = 0;
        mutable size_type m_mapped_start_index = 0;
        mutable size_type m_mapped_start_byte_pos = 0;
        mutable size_type m_mapped_start_byte_pos_aligned = 0;
        mutable size_type m_mapped_size_bytes = 0;
        mutable size_type m_mapped_byte_offset = 0;
        mutable size_type m_page_size = 1;
    private:
    	void ensure_is_mapped(size_type idx) const {
    		if(m_mapped_data) {
    			if(idx >= m_mapped_start_index && 
    				idx < m_mapped_start_index + m_buffer_size
    			) {
    				/* already mapped */
    				return;
    			}
    			/* unmap old block */
    			munmap(m_mapped_data, m_mapped_size_bytes);
    		}
    		/* map block */
    		m_mapped_start_index = (idx/m_buffer_size)*m_buffer_size;
    		m_mapped_start_byte_pos = (m_mapped_start_index * width())/8;
    		m_mapped_start_byte_pos_aligned = (m_mapped_start_byte_pos/m_page_size)*m_page_size;
    		m_mapped_byte_offset = m_mapped_start_byte_pos - m_mapped_start_byte_pos_aligned; 
    		m_mapped_size_bytes = (m_buffer_size*width())/8 + m_mapped_byte_offset + 1;
    		/* end of file? */
    		if(m_mapped_start_byte_pos + m_mapped_size_bytes > m_file_size_bytes) {
    			m_mapped_size_bytes = m_file_size_bytes - m_mapped_start_byte_pos_aligned;
    		}

            if (!(t_mode&std::ios_base::out)) {  // read only
                m_mapped_data = (uint8_t*)mmap(NULL,
                                               m_mapped_size_bytes,
                                               PROT_READ,
                                               MAP_SHARED,
                                               m_fd,m_mapped_start_byte_pos_aligned);
            } else {
                m_mapped_data = (uint8_t*)mmap(NULL,
                                               m_mapped_size_bytes,
                                               PROT_READ|PROT_WRITE,
                                               MAP_SHARED,
                                               m_fd,m_mapped_start_byte_pos_aligned);
            }
            if (m_mapped_data == MAP_FAILED) {
                std::string mmap_error
                    = std::string("int_vector_mapped_buffer: mmap error. ")
                      + std::string(strerror(errno));
                throw std::runtime_error(mmap_error);
            }
            /* prepare the buffer wrapper */
    		if(!m_is_plain) {
				m_mapped_byte_offset += m_data_offset;
    		}

    		m_buffer.m_data = (uint64_t*)(m_mapped_data+m_mapped_byte_offset);
    	}
    public:
        int_vector_mapped_buffer() = delete;
        int_vector_mapped_buffer(const int_vector_mapped_buffer&) = delete;
        int_vector_mapped_buffer& operator=(const int_vector_mapped_buffer&) = delete;
    public:
        int_vector_mapped_buffer(const std::string filename,const bool is_plain=false) 
        : m_file_name(filename), m_is_plain(is_plain) {
            size_type size_in_bits = 0;
            uint8_t int_width = t_width;
            {
                std::ifstream f(m_file_name);
                if (!f.is_open()) {
                    throw std::runtime_error(
                        "int_vector_mapper: file does not exist.");
                }
                if (!m_is_plain) {
                    int_vector<t_width>::read_header(size_in_bits, int_width, f);
                }
            }
            m_file_size_bytes = util::file_size(m_file_name);

            if (!m_is_plain) {
                m_data_offset = t_width ? 8 : 9;
            } else {
                if (8 != t_width and 16 != t_width and 32 != t_width and 64
                    != t_width) {
                    throw std::runtime_error("int_vector_mapped_buffer: plain vector can "
                                             "only be of width 8, 16, 32, 64.");
                }
                size_in_bits = m_file_size_bytes * 8;
            }

            // open backend file depending on mode
            if (!(t_mode&std::ios_base::out))
                m_fd = open(m_file_name.c_str(), O_RDONLY);
            else
                m_fd = open(m_file_name.c_str(), O_RDWR);

            if (m_fd == -1) {
                std::string open_error
                    = std::string("int_vector_mapped_buffer: open file error.")
                      + std::string(strerror(errno));
                throw std::runtime_error(open_error);
            }

            // prepare for mmap
            m_page_size = sysconf(_SC_PAGE_SIZE);
            m_buffer_size = ((m_page_size*t_buffer_elems)/m_page_size) + m_page_size;
            size_type size_in_bytes = ((size_in_bits + 63) >> 6) << 3;
            m_file_size_bytes = size_in_bytes + m_data_offset;
            m_buffer.width(int_width);
            m_buffer.m_size = size_in_bits;
            if(m_buffer.m_data) memory_manager::free_mem(m_buffer.m_data); // clear default int_vector alloc

        }

        int_vector_mapped_buffer(int_vector_mapped_buffer<t_width,t_mode,t_buffer_elems>&& ivm) {
            m_buffer.m_data = m_buffer.m_data;
            m_buffer.m_size = m_buffer.m_size;
            m_buffer.width(m_buffer.width());
            m_file_name = ivm.m_file_name;
            ivm.m_buffer.m_data = nullptr;
            ivm.m_buffer.m_size = 0;
            ivm.m_mapped_data = nullptr;
            ivm.m_fd = -1;
        }


        int_vector_mapped_buffer<t_width,t_mode,t_buffer_elems>& operator=(int_vector_mapped_buffer<t_width,t_mode,t_buffer_elems>&& ivm) {
            m_buffer.m_data = ivm.m_buffer.m_data;
            m_buffer.m_size = ivm.m_buffer.m_size;
            m_buffer.width(ivm.m_buffer.width());
            m_file_name = ivm.m_file_name;
            ivm.m_buffer.m_data = nullptr;
            ivm.m_buffer.m_size = 0;
            ivm.m_mapped_data = nullptr;
            ivm.m_fd = -1;
            return (*this);
        }

        ~int_vector_mapped_buffer() {
        	if (m_fd != -1) {
            	if (t_mode&std::ios_base::out) { // write was possible
	                if (m_data_offset) { // did we append or change size?
	                	ensure_is_mapped(0); // map first part so we can update teh size
	                    // update size in the on disk representation and
	                    // truncate if necessary
	                    uint64_t* size_in_file = (uint64_t*)m_mapped_data;
	                    if (*size_in_file != m_buffer.m_size) {
	                        *size_in_file = m_buffer.m_size;
	                    }
	                    if (t_width==0) {
	                        // if size is variable and we map a sdsl vector
	                        // we might have to update the stored width
	                        uint8_t stored_width = m_mapped_data[8];
	                        if (stored_width != m_buffer.m_width) {
	                            m_mapped_data[8] = m_buffer.m_width;
	                        }
	                    }
	                }
	                // do we have to truncate?
	                size_type current_bit_size = m_buffer.m_size;
	                size_type data_size_in_bytes = ((current_bit_size + 63) >> 6) << 3;
	                if (m_file_size_bytes != data_size_in_bytes + m_data_offset) {
	                    int tret = ftruncate(m_fd, data_size_in_bytes + m_data_offset);
	                    if (tret == -1) {
	                        std::string truncate_error
	                            = std::string("int_vector_mapped_buffer: truncate error. ")
	                              + std::string(strerror(errno));
	                        throw std::runtime_error(truncate_error);
	                    }
	                }
	            }
	            close(m_fd);
	        }
            m_buffer.m_data = nullptr;
            m_buffer.m_size = 0;
        }


        uint8_t width() const {
            return m_buffer.width();
        }

        size_type size() const {
            return m_buffer.size();
        }

        bool empty() const {
            return m_buffer.size() == 0;
        }

        void bit_resize(const size_type bit_size)
        {
            static_assert(t_mode & std::ios_base::out,"int_vector_mapped_buffer: must be opened in in+out mode for 'bit_resize'");
            size_type new_size_in_bytes = ((bit_size + 63) >> 6) << 3;
            if (m_file_size_bytes != new_size_in_bytes + m_data_offset) {
                if (m_mapped_data) {
                	munmap(m_mapped_data, m_mapped_size_bytes);
                	m_mapped_data = nullptr;
                }
                int tret = ftruncate(m_fd, new_size_in_bytes + m_data_offset);
                if (tret == -1) {
                    std::string truncate_error
                        = std::string("int_vector_mapped_buffer: truncate error. ")
                          + std::string(strerror(errno));
                    throw std::runtime_error(truncate_error);
                }
                m_file_size_bytes = new_size_in_bytes + m_data_offset;
            }
        }

        void resize(const size_type size)
        {
            static_assert(t_mode & std::ios_base::out,"int_vector_mapped_buffer: must be opened in in+out mode for 'resize'");
            size_type size_in_bits = size * width();
            bit_resize(size_in_bits);
        }

        void push_back(value_type x)
        {
            static_assert(t_mode & std::ios_base::out,"int_vector_mapped_buffer: must be opened in in+out mode for 'push_back'");
            if (capacity() < size() + 1) {
                size_type old_size = m_buffer.m_size;
                size_type size_in_bits = (size() + append_block_size) * width();
                bit_resize(size_in_bits);
                m_buffer.m_size = old_size;
            }
            // update size in wrapper only
            m_buffer.m_size += width();
            operator[](size()-1) = x;
        }

        size_type capacity() const
        {
            size_t data_size_in_bits = 8 * (m_file_size_bytes - m_data_offset);
            return data_size_in_bits / width();
        }

        size_type bit_size() const
        {
            return m_buffer.bit_size();
        }

        const_reference operator[](size_type idx) const {
        	ensure_is_mapped(idx);
            return m_buffer[idx-m_mapped_start_index];
        }

        reference operator[](size_type idx) {
        	static_assert(t_mode & std::ios_base::out,"int_vector_mapped_buffer: must be opened in in+out mode for 'non-const []'");
        	ensure_is_mapped(idx);
            return m_buffer[idx-m_mapped_start_index];
        }

        const_iterator begin() const {
        	return const_iterator(this,0);
        }

        const_iterator end() const {
        	return const_iterator(this,size());
        }
};



template<uint8_t t_width = 0>
using read_only_mapped_buffer = const int_vector_mapped_buffer<t_width,std::ios_base::in>;

// creates emtpy int_vector<> that will not be deleted
template <uint8_t t_width = 0>
class mapped_write_out_buffer
{
public:
    static int_vector_mapped_buffer<t_width> create(const std::string& key,cache_config& config)
    {
        auto file_name = cache_file_name(key,config);
        auto tmp = create(file_name);
        register_cache_file(key,config);
        return std::move(tmp);
    }
    static int_vector_mapped_buffer<t_width,std::ios_base::out|std::ios_base::in> create(const std::string& file_name)
    {
        //write empty int_vector to init the file
        int_vector<t_width> tmp_vector;
        store_to_file(tmp_vector,file_name);
        return int_vector_mapped_buffer<t_width,std::ios_base::out|std::ios_base::in>(file_name);
    }
};

} // end of namespace

#endif
