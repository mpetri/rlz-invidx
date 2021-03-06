#pragma once

#include "sdsl/int_vector.hpp"


struct bit_nullstream {
public:
	const uint64_t min_bv_size = 1000000;
	using pos_type			   = sdsl::bit_vector::size_type;
	using size_type			   = sdsl::bit_vector::size_type;
	using offset_type		   = std::streamoff;
	using value_type		   = sdsl::bit_vector::value_type;
	// constructor
	explicit bit_nullstream(uint64_t start_offset = 0) : cur_offset(start_offset)
	{
		cur_size = min_bv_size;
	}

public:
	// write individual bits
	inline void put(value_type c) { put_int(c & 1, 1); }
	// write chunks of bits
	inline void write(const uint64_t* s, size_type count, uint8_t in_word_offset = 0)
	{
		expand_if_needed(count);
		while (count > 64) {
			auto x = sdsl::bits::read_int_and_move(s, in_word_offset, 64);
			put_int_no_size_check(x, 64);
			count -= 64;
		}
		auto x = sdsl::bits::read_int_and_move(s, in_word_offset, (uint8_t)count);
		put_int_no_size_check(x, (uint8_t)count);
	}
	// write individual unary encoded integer
	inline void put_unary(const uint64_t x)
	{
		expand_if_needed(x + 1);
		put_unary_no_size_check(x);
	}
	// write bitpacked integer
	inline void put_int(const uint64_t x, const uint8_t len = 64)
	{
		expand_if_needed(len);
		put_int_no_size_check(x, len);
	}
	inline void put_minbin_int(const uint64_t x, const uint64_t max)
	{
		auto len = sdsl::bits::hi(max) + 1;
		expand_if_needed(len);
		put_minbin_int_no_size_check(x, max);
	}
	template <typename t_coder>
	inline void encode_check_size(const uint64_t x)
	{
		t_coder::encode_check_size(*this, x);
	}
	template <typename t_coder>
	inline void encode(const uint64_t x)
	{
		t_coder::encode(*this, x);
	}
	template <class t_coder, class t_itr>
	inline void encode(t_itr begin, std::streamsize count)
	{
		t_coder e;
		e.encode(*this, begin, begin + count);
	}
	template <class t_coder, class t_itr>
	inline void encode(t_itr begin, t_itr end)
	{
		t_coder e;
		e.encode(*this, begin, end);
	}
	// write chunks unary integers
	template <class t_itr>
	void inline write_unary(const t_itr& itr, size_type count)
	{
		expand_if_needed(std::accumulate(itr, itr + count, 0) + count);
		auto tmp = itr;
		for (size_type i = 0; i < count; i++) {
			put_unary_no_size_check(*tmp++);
		}
	}
	// write chunks bitpacked integers
	template <class t_itr>
	void inline write_int(const t_itr& itr, size_type count, const uint8_t len = 64)
	{
		expand_if_needed(count * len);
		auto tmp = itr;
		for (size_type i = 0; i < count; i++) {
			put_int_no_size_check(*tmp++, len);
		}
	}
	// current pos in the bv
	pos_type inline tellp() const { return cur_offset; }
	//resize bv to correct size
	void inline flush() { resize(tellp()); }
	void inline expand_if_needed(size_type n)
	{
		auto needed_bits = tellp() + n;
		if (needed_bits >= cur_size) {
			auto size = std::max(cur_size * 2, needed_bits);
			resize(64 + size); // always have one extra 64bit word
		}
	}

	inline void align64()
	{
		auto mod = cur_offset % 64;
		if (mod != 0) {
			cur_offset += (64 - mod);
		}
	}

	inline void align128()
	{
		auto mod = cur_offset % 128;
		if (mod != 0) {
			cur_offset += (128 - mod);
		}
	}


	inline void align8()
	{
		auto mod = cur_offset % 8;
		if (mod != 0) {
			cur_offset += (8 - mod);
		}
	}

	inline void skip(uint64_t len) { cur_offset += len; }
	void seek(size_type of) { cur_offset = of; }

	void inline resize(size_type n) { cur_size = n; }

	void inline put_int_no_size_check(const uint64_t, const uint8_t len = 64) { cur_offset += len; }
	void inline put_minbin_int_no_size_check(uint64_t x, const uint64_t max)
	{
		auto b = sdsl::bits::hi(max) + 1;
		auto d = (1ULL << b) - max;
		if (x >= d)
			cur_offset += b;
		else
			cur_offset += (b - 1);
	}
	void inline put_unary_no_size_check(uint64_t x) { cur_offset += x + 1; }
	uint64_t*									 data() { return nullptr; }
	uint64_t*									 cur_data() { return nullptr; }
	uint8_t*									 cur_data8() { return nullptr; }
	uint8_t										 bit_at(size_t) const { return 0; }
private:
	uint64_t  cur_offset = 0;
	size_type cur_size   = 0;
};

template <class t_bv>
struct bit_ostream {
public:
	const uint64_t min_bv_size = 1000000;
	using pos_type			   = sdsl::bit_vector::size_type;
	using size_type			   = sdsl::bit_vector::size_type;
	using offset_type		   = std::streamoff;
	using value_type		   = sdsl::bit_vector::value_type;
	// constructor
	explicit bit_ostream(t_bv& bv, size_t start_offset = 0) : m_bv(bv)
	{
		if (m_bv.size() < min_bv_size) m_bv.resize(min_bv_size);
		data_ptr	   = m_bv.data() + (start_offset >> 6);
		in_word_offset = start_offset & 63;
		cur_size	   = m_bv.size();
	}
	~bit_ostream() { flush(); }

public:
	// write individual bits
	inline void put(value_type c) { put_int(c & 1, 1); }
	// write chunks of bits
	inline void write(const uint64_t* s, size_type count, uint8_t in_word_offset = 0)
	{
		expand_if_needed(count);
		while (count > 64) {
			auto x = sdsl::bits::read_int_and_move(s, in_word_offset, 64);
			put_int_no_size_check(x, 64);
			count -= 64;
		}
		auto x = sdsl::bits::read_int_and_move(s, in_word_offset, (uint8_t)count);
		put_int_no_size_check(x, (uint8_t)count);
	}

	// write individual gamma encoded integer
	inline void put_gamma(const uint64_t x)
	{
		uint64_t len = sdsl::bits::hi(x);
		expand_if_needed(2 * len + 1);
		uint64_t hb = uint64_t(1) << len;
		put_unary_no_size_check(len);
		put_int_no_size_check(x ^ hb, len);
	}

	// write individual unary encoded integer
	inline void put_unary(const uint64_t x)
	{
		expand_if_needed(x + 1);
		put_unary_no_size_check(x);
	}
	// write bitpacked integer
	inline void put_int(const uint64_t x, const uint8_t len = 64)
	{
		expand_if_needed(len);
		put_int_no_size_check(x, len);
	}
	template <typename t_coder>
	inline void encode_check_size(const uint64_t x)
	{
		t_coder::encode_check_size(*this, x);
	}
	template <typename t_coder>
	inline void encode(const uint64_t x)
	{
		t_coder::encode(*this, x);
	}
	template <class t_coder, class t_itr>
	inline void encode(t_itr begin, std::streamsize count)
	{
		t_coder e;
		e.encode(*this, begin, begin + count);
	}
	template <class t_coder, class t_itr>
	inline void encode(t_itr begin, t_itr end)
	{
		t_coder e;
		e.encode(*this, begin, end);
	}
	// write chunks unary integers
	template <class t_itr>
	void inline write_unary(const t_itr& itr, size_type count)
	{
		expand_if_needed(std::accumulate(itr, itr + count, 0) + count);
		auto tmp = itr;
		for (size_type i = 0; i < count; i++) {
			put_unary_no_size_check(*tmp++);
		}
	}
	// write chunks bitpacked integers
	template <class t_itr>
	void inline write_int(const t_itr& itr, size_type count, const uint8_t len = 64)
	{
		expand_if_needed(count * len);
		auto tmp = itr;
		for (size_type i = 0; i < count; i++) {
			put_int_no_size_check(*tmp++, len);
		}
	}

	// append data stored in a bitvector
	void inline append_aligned64(const sdsl::bit_vector& bv, size_t n = 0)
	{
		if (n == 0) n = bv.size();
		expand_if_needed(n);
		size_t			num_bytes   = n / 8;
		const uint64_t* bv_data_ptr = bv.data();
		memcpy(data_ptr, bv_data_ptr, num_bytes);
		size_t num_u64 = n / 64;
		data_ptr += num_u64;
		uint64_t left = n % 64;
		put_int_no_size_check(*bv_data_ptr, left);
	}

	void inline append(const sdsl::bit_vector& bv)
	{
		expand_if_needed(bv.size());
		const uint64_t* bv_data_ptr = bv.data();
		size_t			num_u64		= bv.size() / 64;
		for (size_t i = 0; i < num_u64; i++) {
			put_int_no_size_check(*bv_data_ptr, 64);
			bv_data_ptr++;
		}
		uint64_t left = bv.size() % 64;
		put_int_no_size_check(*bv_data_ptr, left);
	}

	// current pos in the bv
	pos_type inline tellp() const { return ((data_ptr - m_bv.data()) << 6) + in_word_offset; }
	//resize bv to correct size
	void inline flush() { resize(tellp()); }
	void inline expand_if_needed(size_type n)
	{
		auto needed_bits = tellp() + n;
		if (needed_bits >= cur_size) {
			auto size = std::max(cur_size * 2, needed_bits);
			resize(64 + size); // always have one extra 64bit word
		}
	}

	inline void align64()
	{
		if (in_word_offset != 0) {
			data_ptr++;
			in_word_offset = 0;
		}
	}

	inline void align128()
	{
		// align 64 first
		if (in_word_offset != 0) {
			data_ptr++;
			in_word_offset = 0;
		}
		// align 128 if necessary
		if (((uintptr_t)data_ptr & 0x0F) != 0) {
			data_ptr++;
		}
	}

	inline void align8()
	{
		auto mod = in_word_offset % 8;
		if (mod != 0) {
			in_word_offset += mod;
			if (in_word_offset >= 64) {
				data_ptr++;
				in_word_offset = 0;
			}
		}
	}

	inline void skip(uint64_t len)
	{
		data_ptr += (len / 64);
		len -= 64 * (len / 64);
		if ((in_word_offset += len) & 0xC0) { // if offset >= 65
			in_word_offset &= 0x3F;
			++data_ptr;
		}
	}
	void seek(size_type offset)
	{
		data_ptr	   = m_bv.data() + (offset >> 6);
		in_word_offset = offset & 0x3F;
	}

	void inline resize(size_type n)
	{
		std::ptrdiff_t cur_word_offset = data_ptr - m_bv.data();
		m_bv.resize(n);
		data_ptr = m_bv.data() + cur_word_offset;
		cur_size = m_bv.size();
	}

	void inline put_int_no_size_check(const uint64_t x, const uint8_t len = 64)
	{
		sdsl::bits::write_int_and_move(data_ptr, x, in_word_offset, len);
	}
	void inline put_unary_no_size_check(uint64_t x)
	{
		while (x >= 64) {
			sdsl::bits::write_int_and_move(data_ptr, 0, in_word_offset, 64);
			x -= 64;
		}
		sdsl::bits::write_int_and_move(data_ptr, 1ULL << x, in_word_offset, x + 1);
	}


	uint64_t* data() { return m_bv.data(); }
	uint64_t* cur_data() { return data_ptr; }
	uint8_t*  cur_data8()
	{
		uint8_t* ptr = (uint8_t*)data_ptr;
		return ptr + (in_word_offset >> 3);
	}
	const t_bv& bitvector() const { return m_bv; }

private:
	t_bv&	 m_bv;
	uint64_t* data_ptr		 = nullptr;
	uint8_t   in_word_offset = 0;
	size_type cur_size		 = 0;
};

template <class t_bv>
struct bit_istream {
public:
	using pos_type	= sdsl::bit_vector::size_type;
	using size_type   = sdsl::bit_vector::size_type;
	using offset_type = std::streamoff;
	using value_type  = sdsl::bit_vector::value_type;
	// constructor
	explicit bit_istream(const t_bv& bv, size_t start_offset = 0)
		: m_bv(bv), data_ptr(bv.data() + (start_offset >> 6)), in_word_offset(start_offset % 64)
	{
	}
	explicit bit_istream(const bit_istream& is)
		: m_bv(is.m_bv), data_ptr(is.data_ptr), in_word_offset(is.in_word_offset)
	{
	}
	// get a bit
	value_type get() const { return get_int(1); }
	template <class t_itr>
	void get(t_itr itr, std::streamsize count) const
	{
		for (auto i = 0; i < count; i++) {
			*itr = get_int(1);
			++itr;
		}
	}


	inline value_type get_unary() const
	{
		value_type x = 0;
		while (peek_int(64) == 0) {
			x += 64;
			skip(64);
		}
		return x + sdsl::bits::read_unary_and_move(data_ptr, in_word_offset);
	}

	inline value_type get_gamma() const
	{
		value_type len = get_unary();
		value_type x   = get_int(len);
		return x | (uint64_t(1) << len);
	}

	template <class t_itr>
	void get_unary(t_itr itr, std::streamsize count) const
	{
		for (auto i = 0; i < count; i++) {
			*itr = get_unary();
			++itr;
		}
	}
	inline value_type get_int(const uint8_t len = 64) const
	{
		return sdsl::bits::read_int_and_move(data_ptr, in_word_offset, len);
	}

	template <class t_coder>
	inline value_type decode() const
	{
		t_coder c;
		return c.decode(*this);
	}
	template <class t_coder, class t_itr>
	inline void decode(t_itr itr, std::streamsize count) const
	{
		t_coder c;
		c.decode(*this, itr, count);
	}
	template <class t_itr>
	void get_int(t_itr itr, std::streamsize count, const uint8_t len = 64)
	{
		for (auto i = 0; i < count; i++) {
			*itr = sdsl::bits::read_int_and_move(data_ptr, in_word_offset, len);
			++itr;
		}
	}
	// peak at the next bit
	inline void skip(uint64_t len) const
	{
		data_ptr += (len / 64);
		len -= 64 * (len / 64);
		if ((in_word_offset += len) & 0xC0) { // if offset >= 65
			in_word_offset &= 0x3F;
			++data_ptr;
		}
	}

	inline void align64() const
	{
		if (in_word_offset != 0) {
			data_ptr++;
			in_word_offset = 0;
		}
	}


	inline void align128() const
	{
		if (in_word_offset != 0) {
			data_ptr++;
			in_word_offset = 0;
		}
		// align 128 if necessary
		if (((uintptr_t)data_ptr & 0x0F) != 0) {
			data_ptr++;
		}
	}


	inline void align8() const
	{
		auto mod = in_word_offset % 8;
		if (mod != 0) {
			in_word_offset += (8 - mod);
			if (in_word_offset >= 64) {
				data_ptr++;
				in_word_offset = 0;
			}
		}
	}

	value_type peek() const { return sdsl::bits::read_int(data_ptr, in_word_offset, 1); }
	value_type peek_int(const uint8_t len = 64) const
	{
		return sdsl::bits::read_int(data_ptr, in_word_offset, len);
	}
	value_type peek_unary() const { return sdsl::bits::read_unary(data_ptr, in_word_offset); }
	pos_type   tellg() const
	{
		std::ptrdiff_t cur_word_offset = data_ptr - m_bv.data();
		return (cur_word_offset << 6) + in_word_offset;
	}
	void seek(size_type offset) const
	{
		data_ptr	   = m_bv.data() + (offset >> 6);
		in_word_offset = offset & 0x3F;
	}
	bool eof() const { return tellg() == m_bv.size(); }
	explicit operator bool() const { return !eof(); }
	const uint64_t* data() const { return m_bv.data(); }
	const uint64_t* cur_data() const { return data_ptr; }
	const uint8_t*  cur_data8() const
	{
		uint8_t* ptr = (uint8_t*)data_ptr;
		return ptr + (in_word_offset >> 3);
	}
	void refresh() const { seek(0); }

private:
	const t_bv&				m_bv;
	mutable const uint64_t* data_ptr	   = nullptr;
	mutable uint8_t			in_word_offset = 0;
};
