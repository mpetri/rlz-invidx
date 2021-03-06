#pragma once

#include "bit_streams.hpp"
#include "bzlib.h"
#include "lz4.h"
#include "lz4hc.h"
#include "lzma.h"
#include "sdsl/int_vector.hpp"
#include "zlib.h"

// brotli
#include "decode.h"
#include "encode.h"

#include "logging.hpp"

#include "zstd.h"

#include "simple16.h"
#include "variablebyte.h"

#include <cassert>
namespace coder {

struct vbyte {
	static std::string type() { return "vbyte"; }

	template <typename T>
	inline uint64_t encoded_length(const T& x) const
	{
		const uint8_t vbyte_len[64] = {1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3,
									   3, 3, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 6, 6, 6,
									   6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8};
		return 8 * vbyte_len[sdsl::bits::hi(x) + 1];
	}
	template <class t_bit_ostream, typename T>
	inline void encode_check_size(t_bit_ostream& os, T y) const
	{
		static_assert(std::numeric_limits<T>::is_signed == false,
					  "can only encode unsigned integers");
		os.expand_if_needed(encoded_length(y));
		uint64_t x = y;
		uint8_t  w = x & 0x7F;
		x >>= 7;
		while (x > 0) {
			w |= 0x80; // mark overflow bit
			os.put_int_no_size_check(w, 8);
			w = x & 0x7F;
			x >>= 7;
		}
		os.put_int_no_size_check(w, 8);
	}
	template <class t_bit_ostream, typename T>
	inline void encode(t_bit_ostream& os, T y) const
	{
		static_assert(std::numeric_limits<T>::is_signed == false,
					  "can only encode unsigned integers");
		uint64_t x = y;
		uint8_t  w = x & 0x7F;
		x >>= 7;
		while (x > 0) {
			w |= 0x80; // mark overflow bit
			os.put_int_no_size_check(w, 8);
			w = x & 0x7F;
			x >>= 7;
		}
		os.put_int_no_size_check(w, 8);
	}
	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, T* in_buf, size_t n) const
	{
		uint64_t bits_required = 0;
		auto	 tmp		   = in_buf;
		for (size_t i = 0; i < n; i++) {
			bits_required += encoded_length(*tmp);
			++tmp;
		}
		os.expand_if_needed(bits_required);
		tmp = in_buf;
		for (size_t i = 0; i < n; i++) {
			encode(os, *tmp);
			++tmp;
		}
	}
	template <class t_bit_istream>
	inline uint64_t decode(const t_bit_istream& is) const
	{
		uint64_t ww	= 0;
		uint8_t  w	 = 0;
		uint64_t shift = 0;
		do {
			w = is.get_int(8);
			ww |= (((uint64_t)(w & 0x7F)) << shift);
			shift += 7;
		} while ((w & 0x80) > 0);
		return ww;
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		for (size_t i = 0; i < n; i++) {
			*out_buf = decode(is);
			++out_buf;
		}
	}
};

struct vbyte_fastpfor {
	static std::string type() { return "vbyte"; }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, T* in_buf, size_t n) const
	{
		os.expand_if_needed(8 * n * (2 + sizeof(T)));
		static FastPForLib::VByte vbyte_coder;
		os.align64();
		size_t			in_size		  = n;
		const uint32_t* input_ptr	 = (const uint32_t*)in_buf;
		uint32_t*		output_ptr	= (uint32_t*)os.cur_data8();
		size_t			bytes_written = 0;
		while (in_size) {
			size_t chunk_size					 = 1024 * 1024 * 1024;
			if (in_size < chunk_size) chunk_size = in_size;

			uint32_t* output_size = output_ptr;
			output_ptr++;
			bytes_written += sizeof(uint32_t);

			size_t written_ints = 2 * n;
			vbyte_coder.encodeArray(input_ptr, chunk_size, output_ptr, written_ints);

			*output_size = written_ints;
			input_ptr += chunk_size;
			in_size -= chunk_size;
			bytes_written += (written_ints * sizeof(uint32_t));
			output_ptr += written_ints;
		}
		os.skip(bytes_written * 8ULL);
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		static FastPForLib::VByte vbyte_coder;
		is.align64();

		size_t	to_decode		 = n;
		uint32_t* input_ptr		 = (uint32_t*)is.cur_data8();
		uint32_t* output_ptr	 = (uint32_t*)out_buf;
		uint64_t  processed_ints = 0;
		while (to_decode) {
			size_t chunk_size					   = 1024 * 1024 * 1024;
			if (to_decode < chunk_size) chunk_size = to_decode;

			uint32_t encoded_size = *input_ptr;
			input_ptr++;
			processed_ints++;

			vbyte_coder.decodeArray(input_ptr, encoded_size, output_ptr, chunk_size);

			output_ptr += chunk_size;
			to_decode -= chunk_size;
			input_ptr += encoded_size;
			processed_ints += encoded_size;
		}
		is.skip(processed_ints * sizeof(uint32_t) * 8);
	}
};

struct simple16 {
	static std::string type() { return "simple16"; }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, T* in_buf, size_t n) const
	{
		os.expand_if_needed(8 * n * (2 + sizeof(T)));
		static FastPForLib::Simple16<0> s16coder;
		os.align64();
		size_t			in_size		  = n;
		const uint32_t* input_ptr	 = (const uint32_t*)in_buf;
		uint32_t*		output_ptr	= (uint32_t*)os.cur_data8();
		size_t			bytes_written = 0;
		while (in_size) {
			size_t chunk_size					 = 1024 * 1024 * 1024;
			if (in_size < chunk_size) chunk_size = in_size;

			uint32_t* output_size = output_ptr;
			output_ptr++;
			bytes_written += sizeof(uint32_t);

			size_t written_ints = 2 * n;
			s16coder.encodeArray(input_ptr, chunk_size, output_ptr, written_ints);

			*output_size = written_ints;
			input_ptr += chunk_size;
			in_size -= chunk_size;
			bytes_written += (written_ints * sizeof(uint32_t));
			output_ptr += written_ints;
		}
		os.skip(bytes_written * 8ULL);
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		static FastPForLib::Simple16<0> s16coder;
		is.align64();

		size_t	to_decode		 = n;
		uint32_t* input_ptr		 = (uint32_t*)is.cur_data8();
		uint32_t* output_ptr	 = (uint32_t*)out_buf;
		uint64_t  processed_ints = 0;
		while (to_decode) {
			size_t chunk_size					   = 1024 * 1024 * 1024;
			if (to_decode < chunk_size) chunk_size = to_decode;

			uint32_t encoded_size = *input_ptr;
			input_ptr++;
			processed_ints++;

			s16coder.decodeArray(input_ptr, encoded_size, output_ptr, chunk_size);

			output_ptr += chunk_size;
			to_decode -= chunk_size;
			input_ptr += encoded_size;
			processed_ints += encoded_size;
		}
		is.skip(processed_ints * sizeof(uint32_t) * 8);
	}
};

template <uint8_t t_width>
struct fixed {
public:
	static std::string type() { return "u" + std::to_string(t_width); }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, T* in_buf, size_t n) const
	{
		os.expand_if_needed(t_width * n);
		for (size_t i = 0; i < n; i++)
			os.put_int_no_size_check(in_buf[i], t_width);
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		for (size_t i  = 0; i < n; i++)
			out_buf[i] = is.get_int(t_width);
	}
};

template <class t_int_type>
struct aligned_fixed {
public:
	static std::string type() { return "u" + std::to_string(8 * sizeof(t_int_type)) + "a"; }

	template <class t_bit_ostream>
	inline void encode(t_bit_ostream& os, const t_int_type* in_buf, size_t n) const
	{
		os.expand_if_needed(8 + 8 * sizeof(t_int_type) * n);
		os.align8();
		uint8_t*	out  = os.cur_data8();
		t_int_type* outA = (t_int_type*)out;
		std::copy(in_buf, in_buf + n, outA);
		os.skip(sizeof(t_int_type) * 8 * n);
	}
	template <class t_bit_istream>
	inline void decode(const t_bit_istream& is, t_int_type* out_buf, size_t n) const
	{
		is.align8();
		const uint8_t*	in  = is.cur_data8();
		const t_int_type* isA = (t_int_type*)in;
		std::copy(isA, isA + n, out_buf);
		is.skip(sizeof(t_int_type) * 8 * n);
	}
};

template <uint8_t t_level = 6>
struct zlib {
public:
	static const uint32_t mem_level   = 9;
	static const uint32_t window_bits = 15;

private:
	mutable z_stream dstrm;
	mutable z_stream istrm;

public:
	zlib()
	{
		dstrm.zalloc = Z_NULL;
		dstrm.zfree  = Z_NULL;
		dstrm.opaque = Z_NULL;
		deflateInit2(&dstrm, t_level, Z_DEFLATED, window_bits, mem_level, Z_DEFAULT_STRATEGY);
		istrm.zalloc = Z_NULL;
		istrm.zfree  = Z_NULL;
		istrm.opaque = Z_NULL;
		inflateInit2(&istrm, window_bits);
	}
	~zlib()
	{
		deflateEnd(&dstrm);
		inflateEnd(&istrm);
	}

public:
	static std::string type() { return "zlib-" + std::to_string(t_level); }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
	{
		uint64_t bits_required = 10000ULL + n * sizeof(T) * 8ULL; // upper bound
		os.expand_if_needed(bits_required);
		os.align8(); // align to bytes if needed

		/* space for writing the encoding size */
		uint32_t* out_size = (uint32_t*)os.cur_data8();
		os.skip(32);

		/* encode */
		uint8_t* out_buf = os.cur_data8();
		uint64_t in_size = n * sizeof(T);

		uint32_t out_buf_bytes = bits_required >> 3;

		dstrm.avail_in  = in_size;
		dstrm.avail_out = out_buf_bytes;
		dstrm.next_in   = (uint8_t*)in_buf;
		dstrm.next_out  = out_buf;

		auto error = deflate(&dstrm, Z_FINISH);
		deflateReset(&dstrm); // after finish we have to reset

		/* If the parameter flush is set to Z_FINISH, pending input
is processed, pending output is flushed and deflate returns
with Z_STREAM_END if there was enough output space; if
deflate returns with Z_OK, this function must be called
again with Z_FINISH and more output spac */
		if (error != Z_STREAM_END) {
			switch (error) {
				case Z_MEM_ERROR:
					LOG(FATAL) << "zlib-encode: Memory error!";
					break;
				case Z_BUF_ERROR:
					LOG(FATAL) << "zlib-encode: Buffer error!";
					break;
				case Z_OK:
					LOG(FATAL) << "zlib-encode: need to call deflate again!";
					break;
				case Z_DATA_ERROR:
					LOG(FATAL) << "zlib-encode: Invalid or incomplete deflate data!";
					break;
				default:
					LOG(FATAL) << "zlib-encode: Unknown error: " << error;
					break;
			}
		}
		if (dstrm.avail_in != 0) {
			LOG(FATAL) << "zlib-encode: not everything was encoded!";
		}
		// write the len. assume it fits in 32bits
		uint32_t written_bytes = out_buf_bytes - dstrm.avail_out;
		*out_size			   = (uint32_t)written_bytes;
		os.skip(written_bytes * 8); // skip over the written content
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		is.align8(); // align to bytes if needed

		/* read the encoding size */
		uint32_t* pin_size = (uint32_t*)is.cur_data8();
		uint32_t  in_size  = *pin_size;
		is.skip(32);

		/* decode */
		auto	 in_buf   = is.cur_data8();
		uint64_t out_size = n * sizeof(T);

		istrm.avail_in  = in_size;
		istrm.next_in   = (uint8_t*)in_buf;
		istrm.avail_out = out_size;
		istrm.next_out  = (uint8_t*)out_buf;

		auto error = inflate(&istrm, Z_FINISH);
		inflateReset(&istrm); // after finish we need to reset
		if (error != Z_STREAM_END) {
			switch (error) {
				case Z_MEM_ERROR:
					LOG(FATAL) << "zlib-decode: Memory error!";
					break;
				case Z_BUF_ERROR:
					LOG(FATAL) << "zlib-decode: Buffer error!";
					break;
				case Z_DATA_ERROR:
					LOG(FATAL) << "zlib-decode: Data error!";
					break;
				case Z_STREAM_END:
					LOG(FATAL) << "zlib-decode: Stream end error!";
					break;
				case Z_NEED_DICT:
					LOG(FATAL) << "zlib-decode: NEED DICT!";
					break;
				default:
					LOG(FATAL) << "zlib-decode: Unknown error: " << error;
					break;
			}
		}

		is.skip(in_size * 8); // skip over the read content
	}
};

template <uint8_t t_level = 9>
struct lz4hc {
private:
	mutable void* lz4_state = nullptr;

public:
	lz4hc()
	{
		auto state_size = LZ4_sizeofStateHC();
		lz4_state		= malloc(state_size);
	}
	~lz4hc() { free(lz4_state); }

public:
	static std::string type() { return "lz4hc-" + std::to_string(t_level); }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
	{
		uint64_t bits_required = 32 + n * 128; // upper bound
		os.expand_if_needed(bits_required);
		os.align8(); // align to bytes if needed
		/* compress */
		char*	out_buf = (char*)os.cur_data8();
		uint64_t in_size = n * sizeof(T);
		LZ4_resetStreamHC((LZ4_streamHC_t*)lz4_state, t_level);
		auto bytes_written = LZ4_compress_HC_continue(
		(LZ4_streamHC_t*)lz4_state, (const char*)in_buf, out_buf, in_size, bits_required >> 3);
		os.skip(bytes_written * 8);
		// } else {
		//     auto bytes_written = LZ4_compress_HC_extStateHC(lz4_state,(const
		//     char*)in_buf,out_buf,in_size,bits_required >> 3,t_level);
		//     os.skip(bytes_written * 8); // skip over the written content
		// }
	}

	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		is.align8(); // align to bytes if needed
		const char* in_buf	= (const char*)is.cur_data8();
		uint64_t	out_size  = n * sizeof(T);
		int			comp_size = 0;
		comp_size			  = LZ4_decompress_fast(in_buf, (char*)out_buf, out_size);
		is.skip(comp_size * 8); // skip over the read content
	}
};

template <uint8_t t_level = 6>
struct bzip2 {
public:
	static const int bzip_verbose_level = 0;
	static const int bzip_work_factor   = 0;
	static const int bzip_use_small_mem = 0;

public:
	static std::string type() { return "bzip2-" + std::to_string(t_level); }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
	{
		uint64_t bits_required = 16ULL * 1024ULL + (n * sizeof(T) * 9ULL); // upper bound
		os.expand_if_needed(bits_required);
		os.align8(); // align to bytes if needed

		/* encode */
		uint8_t* out_buf			 = os.cur_data8();
		uint64_t in_size			 = n * sizeof(T);
		char*	input_ptr			 = (char*)in_buf;
		char*	output_ptr			 = (char*)out_buf;
		uint64_t total_written_bytes = 0;
		while (in_size) {
			uint32_t written_bytes				 = (bits_required >> 3ULL);
			uint32_t chunk_size					 = 1024 * 1024 * 1024;
			if (in_size < chunk_size) chunk_size = in_size;

			uint32_t* cur_out_size = (uint32_t*)output_ptr;
			output_ptr += sizeof(uint32_t);
			total_written_bytes += sizeof(uint32_t);

			auto ret = BZ2_bzBuffToBuffCompress(output_ptr,
												&written_bytes,
												input_ptr,
												chunk_size,
												t_level,
												bzip_verbose_level,
												bzip_work_factor);

			if (ret != BZ_OK) {
				LOG(ERROR) << "n = " << chunk_size;
				LOG(ERROR) << "bits_required = " << bits_required;
				LOG(ERROR) << "written bytes = " << written_bytes;
				LOG(FATAL) << "bzip2-encode: encoding error: " << ret;
			}

			*cur_out_size = written_bytes;
			total_written_bytes += written_bytes;
			output_ptr += written_bytes;
			input_ptr += chunk_size;
			in_size -= chunk_size;
		}
		os.skip(total_written_bytes * 8); // skip over the written content
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		is.align8(); // align to bytes if needed
		char*	in_buf			= (char*)is.cur_data8();
		char*	out_ptr		= (char*)out_buf;
		uint64_t to_recover		= n * sizeof(T);
		uint64_t data_processed = 0;
		while (to_recover) {
			uint32_t* cur_input_size = (uint32_t*)in_buf;
			uint32_t  cur_in_size	= *cur_input_size;
			in_buf += sizeof(uint32_t);
			data_processed += cur_in_size + sizeof(uint32_t);
			uint32_t out_size = 2ULL * 1024ULL * 1024ULL * 1024ULL;
			auto	 ret	  = BZ2_bzBuffToBuffDecompress(
			out_ptr, &out_size, in_buf, cur_in_size, bzip_use_small_mem, bzip_verbose_level);
			if (ret != BZ_OK) {
				LOG(FATAL) << "bzip2-decode: decode error: " << ret;
			}
			in_buf += cur_in_size;
			out_ptr += out_size;
			to_recover -= out_size;
		}
		is.skip(data_processed * 8); // skip over the read content
	}
};

template <uint8_t t_level = 6>
struct brotlih {
private:
	brotli::BrotliParams brotli_params;

public:
	brotlih()
	{
		brotli_params.quality = t_level;
		BrotliErrorString((BrotliErrorCode)0); // to get rid of the unused warning
	}
	~brotlih() {}

public:
	static std::string type() { return "brotli-" + std::to_string(t_level); }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
	{
		uint64_t bits_required = 32 * 1024ULL + n * sizeof(T) * 8; // upper bound
		os.expand_if_needed(bits_required);
		os.align8(); // align to bytes if needed

		/* space for writing the encoding size */
		uint32_t* out_size = (uint32_t*)os.cur_data8();
		os.skip(32);

		/* encode */
		const uint8_t* in_u8   = (uint8_t*)in_buf;
		uint8_t*	   out_buf = os.cur_data8();
		uint64_t	   in_size = n * sizeof(T);

		size_t out_buf_bytes = bits_required >> 3;

		auto ret =
		brotli::BrotliCompressBuffer(brotli_params, in_size, in_u8, &out_buf_bytes, out_buf);

		if (ret != 1) {
			LOG(FATAL) << "brotli-encode: error occured!";
		}
		// write the len. assume it fits in 32bits
		uint32_t written_bytes = out_buf_bytes;
		*out_size			   = (uint32_t)written_bytes;
		os.skip(written_bytes * 8); // skip over the written content
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		is.align8(); // align to bytes if needed

		/* read the encoding size */
		uint32_t* pin_size = (uint32_t*)is.cur_data8();
		uint32_t  in_size  = *pin_size;
		is.skip(32);

		/* decode */
		auto		   in_buf		  = is.cur_data8();
		uint64_t	   out_size		  = n * sizeof(T);
		const uint8_t* encoded_buffer = (const uint8_t*)in_buf;
		uint8_t*	   decoded_buffer = (uint8_t*)out_buf;

		size_t dec_out_size = out_size;
		auto   ret = BrotliDecompressBuffer(in_size, encoded_buffer, &dec_out_size, decoded_buffer);

		if (ret != BROTLI_RESULT_SUCCESS) {
			LOG(FATAL) << "brotli-decode: error occured. " << (int)ret;
		}

		if (out_size != dec_out_size) {
			LOG(FATAL) << "brotli-decode: incorrect out size: " << dec_out_size << " should be "
					   << out_size;
		}

		is.skip(in_size * 8); // skip over the read content
	}
};

template <uint8_t t_level = 3>
struct lzma {
public:
	static const uint32_t lzma_mem_limit	 = 128 * 1024 * 1024;
	static const uint32_t lzma_max_mem_limit = 1024 * 1024 * 1024;

private:
	mutable lzma_stream strm_enc;
	mutable lzma_stream strm_dec;

public:
	lzma()
	{
		strm_enc = LZMA_STREAM_INIT;
		strm_dec = LZMA_STREAM_INIT;
	}
	~lzma()
	{
		lzma_end(&strm_enc);
		lzma_end(&strm_dec);
	}

public:
	static std::string type() { return "lzma-" + std::to_string(t_level); }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
	{
		uint64_t bits_required = 10000ULL + n * sizeof(T) * 8ULL; // upper bound
		os.expand_if_needed(bits_required);
		os.align8(); // align to bytes if needed

		/* space for writing the encoding size */
		uint64_t* out_size = (uint64_t*)os.cur_data8();
		os.skip(64);

		/* init compressor */
		uint64_t osize	 = bits_required >> 3;
		uint8_t* out_buf   = os.cur_data8();
		uint64_t in_size   = n * sizeof(T);
		strm_enc.next_in   = (uint8_t*)in_buf;
		strm_enc.avail_in  = in_size;
		strm_enc.total_out = 0;
		strm_enc.total_in  = 0;

		/* compress */
		int res;
		if ((res = lzma_easy_encoder(&strm_enc, t_level, LZMA_CHECK_NONE)) != LZMA_OK) {
			LOG(FATAL) << "lzma-encode: error init LMZA encoder < n!";
		}

		auto total_written_bytes = 0ULL;
		while (res != LZMA_STREAM_END) {
			strm_enc.next_out  = out_buf;
			strm_enc.avail_out = osize;
			if ((res = lzma_code(&strm_enc, LZMA_FINISH)) != LZMA_OK && res != LZMA_STREAM_END) {
				LOG(FATAL) << "lzma-encode: error code LZMA_FINISH < n!: " << res;
			}
			auto written_bytes = osize - strm_enc.avail_out;
			total_written_bytes += written_bytes;
			osize -= written_bytes;
			out_buf += written_bytes;
		}

		*out_size = (uint64_t)total_written_bytes;
		os.skip(total_written_bytes * 8); // skip over the written content
	}

	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		is.align8(); // align to bytes if needed

		/* read the encoding size */
		uint64_t* pin_size = (uint64_t*)is.cur_data8();
		uint64_t  in_size  = *pin_size;
		is.skip(64);

		/* setup decoder */
		auto	 in_buf   = is.cur_data8();
		uint64_t out_size = n * sizeof(T);
		int		 res;
		if ((res = lzma_auto_decoder(&strm_dec, lzma_mem_limit, 0)) != LZMA_OK) {
			LOG(FATAL) << "lzma-decode: error init LMZA decoder:" << res;
		}

		strm_dec.next_in   = in_buf;
		strm_dec.avail_in  = in_size;
		auto total_decoded = 0ULL;
		while (res != LZMA_STREAM_END) {
			strm_dec.next_out  = (uint8_t*)out_buf;
			strm_dec.avail_out = out_size;
			/* decode */
			res			 = lzma_code(&strm_dec, LZMA_RUN);
			auto decoded = out_size - strm_dec.avail_out;
			total_decoded += decoded;
			out_buf += decoded;
			out_size -= decoded;
			if (res != LZMA_OK && res != LZMA_STREAM_END) {
				lzma_end(&strm_dec);
				LOG(FATAL) << "lzma-decode: error decoding LZMA_RUN: " << res;
			}
			if (res == LZMA_STREAM_END) break;
		}

		if (total_decoded != n * sizeof(T)) {
			LOG(ERROR) << "lzma-decode: decoded bytes = " << total_decoded
					   << " should be = " << n * sizeof(T);
		}

		/* finish decoding */
		// lzma_end(&strm);
		is.skip(in_size * 8); // skip over the read content
	}
};

template <uint8_t t_level = 6>
struct zstd {
private:
	ZSTD_DStream* dstream;

public:
	static std::string type() { return "zstd-" + std::to_string(t_level); }

	zstd() { dstream = ZSTD_createDStream(); }

	~zstd() { ZSTD_freeDStream(dstream); }


	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
	{
		uint64_t bits_required = 8ULL * 1024ULL * 1024ULL + n * sizeof(T) * 8ULL; // upper bound
		os.expand_if_needed(bits_required);
		os.align8(); // align to bytes if needed

		/* space for writing the encoding size */
		uint64_t* out_size = (uint64_t*)os.cur_data8();
		os.skip(64);

		/* encode */
		uint8_t* out_buf = os.cur_data8();
		uint64_t in_size = n * sizeof(T);

		uint64_t	   out_buf_bytes = bits_required >> 3;
		const uint8_t* in			 = (uint8_t*)in_buf;
		auto		   cSize		 = ZSTD_compress(out_buf, out_buf_bytes, in, in_size, t_level);

		if (ZSTD_isError(cSize)) {
			LOG(FATAL) << "zstd-encode: error compressing! " << ZSTD_getErrorName(cSize);
		}
		// write the len. assume it fits in 32bits
		uint64_t written_bytes = cSize;
		*out_size			   = written_bytes;
		os.skip(written_bytes * 8); // skip over the written content
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		is.align8(); // align to bytes if needed

		/* read the encoding size */
		uint64_t* pin_size = (uint64_t*)is.cur_data8();
		uint64_t  in_size  = *pin_size;
		is.skip(64);

		/* decode */
		auto	 in_buf   = is.cur_data8();
		uint64_t out_size = n * sizeof(T);
		auto	 src	  = (uint8_t*)in_buf;
		auto	 out	  = (uint8_t*)out_buf;

		size_t const initResult = ZSTD_initDStream(dstream);
		if (ZSTD_isError(initResult)) {
			fprintf(stderr, "ZSTD_initDStream() error : %s \n", ZSTD_getErrorName(initResult));
			exit(EXIT_FAILURE);
		}

		ZSTD_inBuffer input				 = {src, in_size, 0};
		size_t		  decompressed_bytes = 0;
		size_t		  toRead			 = initResult;
		while (input.pos < input.size) {
			ZSTD_outBuffer output = {out, out_size, 0};
			/* toRead : size of next compressed block */
			toRead = ZSTD_decompressStream(dstream, &output, &input);
			if (ZSTD_isError(toRead)) {
				fprintf(stderr, "ZSTD_decompressStream() error : %s \n", ZSTD_getErrorName(toRead));
				exit(EXIT_FAILURE);
			}
			out += output.pos;
			decompressed_bytes += output.pos;
		}


		// auto dSize = ZSTD_decompress(out, out_size, src, in_size);

		// if (decompressed_bytes != out_size) {
		// 	LOG(FATAL) << "zstd-decode: error decoding! " << ZSTD_getErrorName(dSize);
		// }
		is.skip(in_size * 8); // skip over the read content
	}
};

template <uint8_t t_level = 6>
struct zstd_dict {
private:
	ZSTD_CDict* cdict = nullptr;
	ZSTD_DDict* ddict = nullptr;
	ZSTD_CCtx*  ctx   = nullptr;
	ZSTD_DCtx*  dctx  = nullptr;

public:
	zstd_dict()
	{
		ctx  = ZSTD_createCCtx();
		dctx = ZSTD_createDCtx();
	}

	~zstd_dict()
	{
		ZSTD_freeCCtx(ctx);
		ZSTD_freeDCtx(dctx);
	}

	void set_cdict(ZSTD_CDict* const cd) { cdict = cd; }
	void set_ddict(ZSTD_DDict* const dd) { ddict = dd; }

	static std::string type() { return "zstd_dict-" + std::to_string(t_level); }

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, const T* in_buf, size_t n) const
	{
		uint64_t bits_required = 10000ULL + n * sizeof(T) * 8ULL; // upper bound
		os.expand_if_needed(bits_required);
		os.align8(); // align to bytes if needed

		/* space for writing the encoding size */
		uint64_t* out_size = (uint64_t*)os.cur_data8();
		os.skip(64);

		/* encode */
		uint8_t* out_buf = os.cur_data8();
		uint64_t in_size = n * sizeof(T);

		uint64_t	   out_buf_bytes = bits_required >> 3;
		const uint8_t* in			 = (uint8_t*)in_buf;
		auto cSize = ZSTD_compress_usingCDict(ctx, out_buf, out_buf_bytes, in, in_size, cdict);

		if (ZSTD_isError(cSize)) {
			LOG(FATAL) << "zstd-encode: error compressing! " << ZSTD_getErrorName(cSize);
		}
		// write the len. assume it fits in 32bits
		uint64_t written_bytes = cSize;
		*out_size			   = written_bytes;
		os.skip(written_bytes * 8); // skip over the written content
	}
	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n) const
	{
		is.align8(); // align to bytes if needed

		/* read the encoding size */
		uint64_t* pin_size = (uint64_t*)is.cur_data8();
		uint64_t  in_size  = *pin_size;
		is.skip(64);

		/* decode */
		auto	 in_buf   = is.cur_data8();
		uint64_t out_size = n * sizeof(T);

		auto src   = (uint8_t*)in_buf;
		auto out   = (uint8_t*)out_buf;
		auto dSize = ZSTD_decompress_usingDDict(dctx, out, out_size, src, in_size, ddict);

		if (dSize != out_size) {
			LOG(FATAL) << "zstd-decode: error decoding! " << ZSTD_getErrorName(dSize);
		}
		is.skip(in_size * 8); // skip over the read content
	}
};

struct interpolative {
private:
	template <class t_bit_ostream>
	inline void write_center_mid(t_bit_ostream& os, uint64_t val, uint64_t u) const
	{
		if (u == 1) return;
		auto b = sdsl::bits::hi(u - 1) + 1ULL;
		auto d = 2ULL * u - (1ULL << b);
		val	= val + (u - d / 2);
		if (val > u) val -= u;
		uint64_t m = (1ULL << b) - u;
		if (val <= m) {
			os.put_int_no_size_check(val - 1, b - 1);
		} else {
			val += m;
			os.put_int_no_size_check((val - 1) >> 1, b - 1);
			os.put_int_no_size_check((val - 1) & 1, 1);
		}
	}
	template <class t_bit_istream>
	inline uint64_t read_center_mid(t_bit_istream& is, uint64_t u) const
	{
		auto	 b   = u == 1 ? 0 : sdsl::bits::hi(u - 1) + 1ULL;
		auto	 d   = 2ULL * u - (1ULL << b);
		uint64_t val = 1;
		if (u != 1) {
			uint64_t m = (1ULL << b) - u;
			val		   = is.get_int(b - 1) + 1;
			if (val > m) {
				val = (2ULL * val + is.get_int(1)) - m - 1;
			}
		}
		val = val + d / 2;
		if (val > u) val -= u;
		return val;
	}

	template <class t_bit_ostream, class T>
	inline void
	encode_interpolative(t_bit_ostream& os, T* in_buf, size_t n, size_t low, size_t high) const
	{
		if (n == 0) return;
		size_t   h  = (n + 1) >> 1;
		size_t   n1 = h - 1;
		size_t   n2 = n - h;
		uint64_t v  = in_buf[h - 1] + 1; // we don't encode 0
		write_center_mid(os, v - low - n1 + 1, high - n2 - low - n1 + 1);
		encode_interpolative(os, in_buf, n1, low, v - 1);
		encode_interpolative(os, in_buf + h, n2, v + 1, high);
	}

	template <class t_bit_istream, class T>
	inline void
	decode_interpolative(t_bit_istream& is, T* out_buf, size_t n, size_t low, size_t high) const
	{
		if (n == 0) return;
		size_t   h	 = (n + 1) >> 1;
		size_t   n1	= h - 1;
		size_t   n2	= n - h;
		uint64_t v	 = low + n1 - 1 + read_center_mid(is, high - n2 - low - n1 + 1);
		out_buf[h - 1] = v - 1; // we don't encode 0
		if (n1) decode_interpolative(is, out_buf, n1, low, v - 1);
		if (n2) decode_interpolative(is, out_buf + h, n2, v + 1, high);
	}

public:
	static std::string type() { return "ip"; }

	template <class T>
	inline uint64_t determine_size(T* in_buf, size_t n, size_t u) const
	{
		bit_nullstream bns;
		encode(bns, in_buf, n, u);
		return bns.tellp();
	}

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, T* in_buf, size_t n, size_t u) const
	{
		os.expand_if_needed(n * (sdsl::bits::hi(u + 1) + 1));
		size_t low  = 1;
		size_t high = u + 1;
		encode_interpolative(os, in_buf, n, low, high);
	}

	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n, size_t u) const
	{
		size_t low  = 1;
		size_t high = u + 1;
		decode_interpolative(is, out_buf, n, low, high);
	}
};

struct elias_fano {
public:
	static std::string type() { return "ef"; }

	template <class T>
	inline uint64_t determine_size(T*, size_t n, size_t u) const
	{
		uint8_t logm	  = sdsl::bits::hi(n) + 1;
		uint8_t logu	  = sdsl::bits::hi(u) + 1;
		uint8_t width_low = 0;
		if (logu < logm) {
			width_low = 1;
		} else {
			if (logm == logu) logm--;
			width_low = logu - logm;
		}
		return n * width_low + 2 * n;
	}

	template <class t_bit_ostream, class T>
	inline void encode(t_bit_ostream& os, T* in_buf, size_t n, size_t u) const
	{
		uint8_t logm	  = sdsl::bits::hi(n) + 1;
		uint8_t logu	  = sdsl::bits::hi(u) + 1;
		uint8_t width_low = 0;
		if (logu < logm) {
			width_low = 1;
		} else {
			if (logm == logu) logm--;
			width_low = logu - logm;
		}

		// write low
		os.expand_if_needed(n * width_low);
		for (size_t i = 0; i < n; i++) {
			os.put_int_no_size_check(in_buf[i], width_low);
		}
		// write high
		size_t num_zeros = (u >> width_low);
		os.expand_if_needed(num_zeros + n + 1);
		size_t last_high = 0;
		for (size_t i = 0; i < n; i++) {
			auto   position = in_buf[i];
			size_t cur_high = position >> width_low;
			os.put_unary_no_size_check(cur_high - last_high);
			last_high = cur_high;
		}
	}

	template <class t_bit_istream, class T>
	inline void decode(const t_bit_istream& is, T* out_buf, size_t n, size_t u) const
	{
		uint8_t logm	  = sdsl::bits::hi(n) + 1;
		uint8_t logu	  = sdsl::bits::hi(u) + 1;
		uint8_t width_low = 0;

		// read low
		if (logu < logm) {
			width_low = 1;
		} else {
			if (logm == logu) logm--;
			width_low = logu - logm;
		}
		for (size_t i = 0; i < n; i++) {
			auto low   = is.get_int(width_low);
			out_buf[i] = low;
		}
		// read high
		size_t last_high = 0;
		for (size_t i = 0; i < n; i++) {
			auto cur_high = is.get_unary();
			auto high	 = last_high + cur_high;
			last_high	 = high;
			out_buf[i]	= (high << width_low) + out_buf[i];
		}
	}
};
}
