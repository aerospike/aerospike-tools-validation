/*
 * Copyright 2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include <getopt.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>

//==========================================================
// Typedefs & constants.
//

#define AS_STR_TYPE 3
#define AS_BLOB_TYPE 4
#define ORDERED_FLAG 1
#define MAX_ERR_SHOWN 5

static uint32_t g_errors_shown = 0;

typedef struct wbuf_s {
	uint8_t *buf;
	size_t sz;
	size_t cap;
} wbuf;

typedef enum {
	DS_SCALAR,
	DS_MAP,
	DS_LIST,
	DS_ORDERED,
	DS_MIXED,
	DS_DEEP
} dataset;

typedef enum {
	SHAPE_LIST_CHAIN,
	SHAPE_PAIR_CHAIN,
	SHAPE_MAP_CHAIN,
	SHAPE_EMPTY_AT,
	SHAPE_FLAGS_LIST_AT,
	SHAPE_FLAGS_MAP_AT,
	SHAPE_FLAGS0_AT,
	SHAPE_MAP_VALUE_CHAIN
} shape;

typedef struct deep_case_s {
	shape shape;
	uint32_t depth;
} deep_case;

typedef struct load_args_s {
	aerospike *as;
	const char *ns;
	const char *set;
	dataset ds;
	uint64_t n_records;
	uint64_t violator_stride;
	uint32_t deep_depth;
	uint64_t seed;
	uint32_t n_threads;
	uint32_t thread_ix;

	uint64_t written;
	uint64_t failed;
	uint64_t bytes;
} load_args;

typedef struct walk_result_s {
	uint32_t server_depth;
	uint32_t strict_depth;
	bool invalid_key;
	bool corrupt;
	bool padding;
} walk_result;

typedef struct expect_stats_s {
	uint32_t max_depth;
	FILE *digest_fd;

	uint64_t records;
	uint64_t records_flagged;
	uint64_t lists;
	uint64_t maps;
	uint64_t over;
	uint64_t strict_only;
	uint64_t invalid_key;
	uint64_t corrupt;
	uint64_t padding;
	uint32_t max_server_depth;
	uint32_t max_strict_depth;
	uint64_t hist[7];
} expect_stats;

static const deep_case violators[] = {
	{ SHAPE_LIST_CHAIN, 63 }, { SHAPE_LIST_CHAIN, 64 }, { SHAPE_LIST_CHAIN, 65 },
	{ SHAPE_LIST_CHAIN, 66 }, { SHAPE_LIST_CHAIN, 200 },
	{ SHAPE_PAIR_CHAIN, 63 }, { SHAPE_PAIR_CHAIN, 64 }, { SHAPE_PAIR_CHAIN, 65 },
	{ SHAPE_PAIR_CHAIN, 66 }, { SHAPE_PAIR_CHAIN, 200 },
	{ SHAPE_MAP_CHAIN, 63 }, { SHAPE_MAP_CHAIN, 64 }, { SHAPE_MAP_CHAIN, 65 },
	{ SHAPE_MAP_CHAIN, 66 }, { SHAPE_MAP_CHAIN, 200 },
	{ SHAPE_EMPTY_AT, 64 }, { SHAPE_EMPTY_AT, 65 }, { SHAPE_EMPTY_AT, 66 },
	{ SHAPE_FLAGS_LIST_AT, 65 },
	{ SHAPE_FLAGS_MAP_AT, 65 },
	{ SHAPE_FLAGS0_AT, 65 }
};

#define N_VIOLATORS (sizeof(violators) / sizeof(violators[0]))

static const shape deep_shapes[] = {
	SHAPE_LIST_CHAIN, SHAPE_PAIR_CHAIN, SHAPE_MAP_CHAIN, SHAPE_MAP_VALUE_CHAIN
};

#define N_DEEP_SHAPES (sizeof(deep_shapes) / sizeof(deep_shapes[0]))

static const char *dataset_names[] = {
	"scalar", "map", "list", "ordered", "mixed", "deep"
};

static const char *hist_names[] = {
	"<=62", "63", "64", "65", "66", "67-199", ">=200"
};

//==========================================================
// Helpers.
//

static void
atomic_max_u32(uint32_t *target, uint32_t v)
{
	uint32_t cur = __atomic_load_n(target, __ATOMIC_RELAXED);

	while (v > cur && ! __atomic_compare_exchange_n(target, &cur, v, true,
			__ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
	}
}

static as_bytes_type
shape_type(shape s)
{
	switch (s) {
	case SHAPE_MAP_CHAIN:
	case SHAPE_MAP_VALUE_CHAIN:
		return AS_BYTES_MAP;
	default:
		return AS_BYTES_LIST;
	}
}

//==========================================================
// Random numbers (splitmix64).
//

static uint64_t
rng_next(uint64_t *state)
{
	uint64_t z = (*state += 0x9e3779b97f4a7c15ULL);

	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
	z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;

	return z ^ (z >> 31);
}

static uint32_t
rng_range(uint64_t *state, uint32_t lo, uint32_t hi)
{
	return lo + (uint32_t)(rng_next(state) % (hi - lo + 1));
}

//==========================================================
// Msgpack writer.
//

static void
wb_reserve(wbuf *wb, size_t extra)
{
	if (wb->sz + extra <= wb->cap) {
		return;
	}

	size_t cap = wb->cap == 0 ? 256 : wb->cap;

	while (cap < wb->sz + extra) {
		cap *= 2;
	}

	wb->buf = realloc(wb->buf, cap);

	if (wb->buf == NULL) {
		fprintf(stderr, "out of memory\n");
		exit(1);
	}

	wb->cap = cap;
}

static void
wb_u8(wbuf *wb, uint8_t b)
{
	wb_reserve(wb, 1);
	wb->buf[wb->sz++] = b;
}

static void
wb_be(wbuf *wb, uint64_t v, uint32_t n_bytes)
{
	wb_reserve(wb, n_bytes);

	for (uint32_t i = 0; i < n_bytes; i++) {
		wb->buf[wb->sz++] = (uint8_t)(v >> (8 * (n_bytes - 1 - i)));
	}
}

static void
wb_raw(wbuf *wb, const void *p, size_t sz)
{
	wb_reserve(wb, sz);
	memcpy(wb->buf + wb->sz, p, sz);
	wb->sz += sz;
}

static void
wb_list_hdr(wbuf *wb, uint32_t n)
{
	if (n < 16) {
		wb_u8(wb, (uint8_t)(0x90 | n));
	}
	else if (n < 0x10000) {
		wb_u8(wb, 0xdc);
		wb_be(wb, n, 2);
	}
	else {
		wb_u8(wb, 0xdd);
		wb_be(wb, n, 4);
	}
}

static void
wb_map_hdr(wbuf *wb, uint32_t n)
{
	if (n < 16) {
		wb_u8(wb, (uint8_t)(0x80 | n));
	}
	else if (n < 0x10000) {
		wb_u8(wb, 0xde);
		wb_be(wb, n, 2);
	}
	else {
		wb_u8(wb, 0xdf);
		wb_be(wb, n, 4);
	}
}

static void
wb_int(wbuf *wb, int64_t v)
{
	if (v >= 0) {
		uint64_t u = (uint64_t)v;

		if (u < 0x80) {
			wb_u8(wb, (uint8_t)u);
		}
		else if (u <= 0xff) {
			wb_u8(wb, 0xcc);
			wb_be(wb, u, 1);
		}
		else if (u <= 0xffff) {
			wb_u8(wb, 0xcd);
			wb_be(wb, u, 2);
		}
		else if (u <= 0xffffffff) {
			wb_u8(wb, 0xce);
			wb_be(wb, u, 4);
		}
		else {
			wb_u8(wb, 0xcf);
			wb_be(wb, u, 8);
		}
	}
	else if (v >= -32) {
		wb_u8(wb, (uint8_t)(int8_t)v);
	}
	else if (v >= INT8_MIN) {
		wb_u8(wb, 0xd0);
		wb_be(wb, (uint64_t)v, 1);
	}
	else if (v >= INT16_MIN) {
		wb_u8(wb, 0xd1);
		wb_be(wb, (uint64_t)v, 2);
	}
	else if (v >= INT32_MIN) {
		wb_u8(wb, 0xd2);
		wb_be(wb, (uint64_t)v, 4);
	}
	else {
		wb_u8(wb, 0xd3);
		wb_be(wb, (uint64_t)v, 8);
	}
}

static void
wb_typed_raw(wbuf *wb, uint8_t as_type, const char *s, uint32_t len)
{
	uint32_t n = len + 1;

	if (n < 32) {
		wb_u8(wb, (uint8_t)(0xa0 | n));
	}
	else if (n < 0x100) {
		wb_u8(wb, 0xd9);
		wb_be(wb, n, 1);
	}
	else if (n < 0x10000) {
		wb_u8(wb, 0xda);
		wb_be(wb, n, 2);
	}
	else {
		wb_u8(wb, 0xdb);
		wb_be(wb, n, 4);
	}

	wb_u8(wb, as_type);
	wb_raw(wb, s, len);
}

static void
wb_str(wbuf *wb, const char *s, uint32_t len)
{
	wb_typed_raw(wb, AS_STR_TYPE, s, len);
}

static void
wb_ext_flags(wbuf *wb, uint8_t flags)
{
	wb_u8(wb, 0xc7);
	wb_u8(wb, 0);
	wb_u8(wb, flags);
}

static void
wb_rand_str(wbuf *wb, uint64_t *rng, uint32_t min_len, uint32_t max_len)
{
	char s[256];
	uint32_t len = rng_range(rng, min_len, max_len);

	for (uint32_t i = 0; i < len; i++) {
		s[i] = (char)('a' + rng_next(rng) % 26);
	}

	wb_str(wb, s, len);
}

static void
wb_rand_int(wbuf *wb, uint64_t *rng)
{
	switch (rng_next(rng) % 4) {
	case 0:
		wb_int(wb, (int64_t)(rng_next(rng) % 128));
		break;
	case 1:
		wb_int(wb, (int64_t)(rng_next(rng) % 100000));
		break;
	case 2:
		wb_int(wb, -(int64_t)(rng_next(rng) % 100000) - 1);
		break;
	default:
		wb_int(wb, (int64_t)(rng_next(rng) >> 1));
		break;
	}
}

static void
wb_rand_value(wbuf *wb, uint64_t *rng)
{
	uint32_t pick = (uint32_t)(rng_next(rng) % 100);

	if (pick < 40) {
		wb_rand_int(wb, rng);
	}
	else if (pick < 70) {
		wb_rand_str(wb, rng, 4, 24);
	}
	else if (pick < 85) {
		uint32_t n = rng_range(rng, 1, 5);

		wb_list_hdr(wb, n);

		for (uint32_t i = 0; i < n; i++) {
			wb_rand_int(wb, rng);
		}
	}
	else {
		uint32_t n = rng_range(rng, 1, 3);

		wb_map_hdr(wb, n);

		for (uint32_t i = 0; i < n; i++) {
			wb_int(wb, (int64_t)i);
			wb_rand_str(wb, rng, 1, 8);
		}
	}
}

//==========================================================
// Bin shapes.
//

static void
build_unordered_map(wbuf *wb, uint64_t *rng, uint32_t n)
{
	wb_map_hdr(wb, n);

	for (uint32_t j = 0; j < n; j++) {
		if (rng_next(rng) % 2 == 0) {
			wb_int(wb, (int64_t)j * 1000 + (int64_t)(rng_next(rng) % 1000));
		}
		else {
			char key[32];
			int len = snprintf(key, sizeof(key), "k%u_%x", j,
					(uint32_t)(rng_next(rng) & 0xffff));

			wb_str(wb, key, (uint32_t)len);
		}

		wb_rand_value(wb, rng);
	}
}

static void
build_unordered_list(wbuf *wb, wbuf *tmp, uint64_t *rng, size_t target_sz)
{
	uint32_t n = 0;

	tmp->sz = 0;

	while (tmp->sz < target_sz) {
		wb_rand_value(tmp, rng);
		n++;
	}

	wb_list_hdr(wb, n);
	wb_raw(wb, tmp->buf, tmp->sz);
}

static void
build_ordered_list(wbuf *wb, uint64_t *rng, uint32_t n)
{
	int64_t v = (int64_t)(rng_next(rng) % 1000);

	wb_list_hdr(wb, n + 1);
	wb_ext_flags(wb, ORDERED_FLAG);

	for (uint32_t j = 0; j < n; j++) {
		wb_int(wb, v);
		v += 1 + (int64_t)(rng_next(rng) % 100);
	}
}

static void
build_kordered_map(wbuf *wb, uint64_t *rng, uint32_t n)
{
	int64_t k = (int64_t)(rng_next(rng) % 1000);

	wb_map_hdr(wb, n + 1);
	wb_ext_flags(wb, ORDERED_FLAG);
	wb_u8(wb, 0xc0);

	for (uint32_t j = 0; j < n; j++) {
		wb_int(wb, k);
		wb_rand_int(wb, rng);
		k += 1 + (int64_t)(rng_next(rng) % 100);
	}
}

static void
build_deep(wbuf *wb, shape s, uint32_t depth)
{
	switch (s) {
	case SHAPE_LIST_CHAIN:
		for (uint32_t i = 0; i < depth; i++) {
			wb_list_hdr(wb, 1);
		}

		wb_int(wb, 1);
		break;
	case SHAPE_PAIR_CHAIN:
		for (uint32_t i = 1; i < depth; i++) {
			wb_list_hdr(wb, 2);
			wb_int(wb, 1);
		}

		wb_list_hdr(wb, 1);
		wb_int(wb, 1);
		break;
	case SHAPE_MAP_CHAIN:
		for (uint32_t i = 0; i < depth; i++) {
			wb_map_hdr(wb, 1);
			wb_int(wb, 1);
		}

		wb_int(wb, 1);
		break;
	case SHAPE_EMPTY_AT:
		for (uint32_t i = 1; i < depth; i++) {
			wb_list_hdr(wb, 1);
		}

		wb_list_hdr(wb, 0);
		break;
	case SHAPE_FLAGS_LIST_AT:
		for (uint32_t i = 1; i < depth; i++) {
			wb_list_hdr(wb, 1);
		}

		wb_list_hdr(wb, 1);
		wb_ext_flags(wb, ORDERED_FLAG);
		break;
	case SHAPE_FLAGS_MAP_AT:
		for (uint32_t i = 1; i < depth; i++) {
			wb_list_hdr(wb, 1);
		}

		wb_map_hdr(wb, 1);
		wb_ext_flags(wb, ORDERED_FLAG);
		wb_u8(wb, 0xc0);
		break;
	case SHAPE_FLAGS0_AT:
		for (uint32_t i = 1; i < depth; i++) {
			wb_list_hdr(wb, 1);
		}

		wb_list_hdr(wb, 1);
		wb_ext_flags(wb, 0);
		break;
	case SHAPE_MAP_VALUE_CHAIN:
		wb_map_hdr(wb, 1);
		wb_str(wb, "deep", 4);
		build_deep(wb, SHAPE_LIST_CHAIN, depth - 1);
		break;
	}
}

//==========================================================
// Loader.
//

static bool
put_bins(aerospike *as, const char *ns, const char *set, int64_t key_ix,
		as_record *rec, load_args *la)
{
	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;
	wp.base.total_timeout = 30000;
	wp.base.max_retries = 2;

	as_key key;
	as_key_init_int64(&key, ns, set, key_ix);

	as_error err;

	if (aerospike_key_put(as, &err, &wp, &key, rec) != AEROSPIKE_OK) {
		la->failed++;

		if (__atomic_add_fetch(&g_errors_shown, 1, __ATOMIC_RELAXED) <=
				MAX_ERR_SHOWN) {
			fprintf(stderr, "key %" PRId64 ": error %d: %s\n", key_ix, err.code,
					err.message);
		}

		return false;
	}

	la->written++;
	return true;
}

static void *
load_thread(void *udata)
{
	load_args *la = udata;
	wbuf main_wb = { 0 };
	wbuf deep_wb = { 0 };
	wbuf tmp = { 0 };

	for (uint64_t i = la->thread_ix; i < la->n_records; i += la->n_threads) {
		uint64_t rng = la->seed ^ (i * 0x2545f4914f6cdd1dULL);
		char s[64];
		as_record rec;

		main_wb.sz = 0;
		deep_wb.sz = 0;
		as_record_init(&rec, 2);

		switch (la->ds) {
		case DS_SCALAR: {
			uint32_t len = rng_range(&rng, 16, 48);

			for (uint32_t c = 0; c < len; c++) {
				s[c] = (char)('a' + rng_next(&rng) % 26);
			}

			s[len] = 0;
			as_record_set_int64(&rec, "i", (int64_t)(rng_next(&rng) >> 1));
			as_record_set_str(&rec, "s", s);
			break;
		}
		case DS_MAP:
			build_unordered_map(&main_wb, &rng, rng_range(&rng, 50, 500));
			as_record_set_raw_typep(&rec, "m", main_wb.buf, (uint32_t)main_wb.sz,
					AS_BYTES_MAP, false);
			break;
		case DS_LIST:
			build_unordered_list(&main_wb, &tmp, &rng, 1000);
			as_record_set_raw_typep(&rec, "l", main_wb.buf, (uint32_t)main_wb.sz,
					AS_BYTES_LIST, false);
			break;
		case DS_ORDERED:
			if (i % 2 == 0) {
				build_ordered_list(&main_wb, &rng, 10000);
				as_record_set_raw_typep(&rec, "ol", main_wb.buf,
						(uint32_t)main_wb.sz, AS_BYTES_LIST, false);
			}
			else {
				build_kordered_map(&main_wb, &rng, 10000);
				as_record_set_raw_typep(&rec, "om", main_wb.buf,
						(uint32_t)main_wb.sz, AS_BYTES_MAP, false);
			}
			break;
		case DS_MIXED:
			if (i % 2 == 0) {
				build_unordered_map(&main_wb, &rng, rng_range(&rng, 50, 500));
				as_record_set_raw_typep(&rec, "m", main_wb.buf,
						(uint32_t)main_wb.sz, AS_BYTES_MAP, false);
			}
			else {
				build_unordered_list(&main_wb, &tmp, &rng, 1000);
				as_record_set_raw_typep(&rec, "l", main_wb.buf,
						(uint32_t)main_wb.sz, AS_BYTES_LIST, false);
			}

			if (la->violator_stride != 0 && i % la->violator_stride == 0) {
				const deep_case *dc =
						&violators[(i / la->violator_stride) % N_VIOLATORS];

				build_deep(&deep_wb, dc->shape, dc->depth);
				as_record_set_raw_typep(&rec, "deep", deep_wb.buf,
						(uint32_t)deep_wb.sz, shape_type(dc->shape), false);
			}
			break;
		case DS_DEEP: {
			shape sh = deep_shapes[i % N_DEEP_SHAPES];

			build_deep(&deep_wb, sh, la->deep_depth);
			as_record_set_raw_typep(&rec, "deep", deep_wb.buf,
					(uint32_t)deep_wb.sz, shape_type(sh), false);
			break;
		}
		}

		la->bytes += main_wb.sz + deep_wb.sz;
		put_bins(la->as, la->ns, la->set, (int64_t)i, &rec, la);
		as_record_destroy(&rec);
	}

	free(main_wb.buf);
	free(deep_wb.buf);
	free(tmp.buf);

	return NULL;
}

//==========================================================
// Reference depth walk.
//
// Deliberately simple: heap-grown stack, no depth cap, no shortcuts. Used to
// compute the expected class of every stored bin.
//

typedef struct frame_s {
	uint64_t remaining;
	bool is_map;
} frame;

static bool
rd_be(const uint8_t **p, const uint8_t *end, uint32_t n_bytes, uint64_t *v)
{
	if ((size_t)(end - *p) < n_bytes) {
		return false;
	}

	*v = 0;

	for (uint32_t i = 0; i < n_bytes; i++) {
		*v = (*v << 8) | *(*p)++;
	}

	return true;
}

static bool
skip_bytes(const uint8_t **p, const uint8_t *end, uint64_t n)
{
	if ((uint64_t)(end - *p) < n) {
		return false;
	}

	*p += n;
	return true;
}

typedef enum {
	EL_SCALAR,
	EL_LIST,
	EL_MAP,
	EL_EXT,
	EL_ERROR
} el_kind;

// Parses one element header. Containers return their element count, scalars
// are skipped whole. key_ok reports whether a scalar may be a map key.
static el_kind
parse_el(const uint8_t **p, const uint8_t *end, uint64_t *count, bool *key_ok)
{
	uint64_t len = 0;

	*key_ok = false;

	if (*p >= end) {
		return EL_ERROR;
	}

	uint8_t b = *(*p)++;

	if (b < 0x80 || b >= 0xe0) {
		*key_ok = true;
		return EL_SCALAR;
	}

	if ((b & 0xf0) == 0x80) {
		*count = b & 0x0f;
		return EL_MAP;
	}

	if ((b & 0xf0) == 0x90) {
		*count = b & 0x0f;
		return EL_LIST;
	}

	uint32_t raw_hdr = 0;

	if ((b & 0xe0) == 0xa0) {
		len = b & 0x1f;
	}
	else {
		switch (b) {
		case 0xc0:
		case 0xc2:
		case 0xc3:
			return EL_SCALAR;
		case 0xcc:
		case 0xcd:
		case 0xce:
		case 0xcf:
		case 0xd0:
		case 0xd1:
		case 0xd2:
		case 0xd3: {
			uint32_t w = 1u << ((b & 0x03));

			*key_ok = true;
			return skip_bytes(p, end, w) ? EL_SCALAR : EL_ERROR;
		}
		case 0xca:
			return skip_bytes(p, end, 4) ? EL_SCALAR : EL_ERROR;
		case 0xcb:
			return skip_bytes(p, end, 8) ? EL_SCALAR : EL_ERROR;
		case 0xc4:
		case 0xd9:
			raw_hdr = 1;
			break;
		case 0xc5:
		case 0xda:
			raw_hdr = 2;
			break;
		case 0xc6:
		case 0xdb:
			raw_hdr = 4;
			break;
		case 0xdc:
			return rd_be(p, end, 2, count) ? EL_LIST : EL_ERROR;
		case 0xdd:
			return rd_be(p, end, 4, count) ? EL_LIST : EL_ERROR;
		case 0xde:
			return rd_be(p, end, 2, count) ? EL_MAP : EL_ERROR;
		case 0xdf:
			return rd_be(p, end, 4, count) ? EL_MAP : EL_ERROR;
		case 0xd4:
		case 0xd5:
		case 0xd6:
		case 0xd7:
		case 0xd8:
			return skip_bytes(p, end, 1 + (1u << (b - 0xd4))) ? EL_EXT : EL_ERROR;
		case 0xc7:
			if (! rd_be(p, end, 1, &len)) {
				return EL_ERROR;
			}
			return skip_bytes(p, end, 1 + len) ? EL_EXT : EL_ERROR;
		case 0xc8:
			if (! rd_be(p, end, 2, &len)) {
				return EL_ERROR;
			}
			return skip_bytes(p, end, 1 + len) ? EL_EXT : EL_ERROR;
		case 0xc9:
			if (! rd_be(p, end, 4, &len)) {
				return EL_ERROR;
			}
			return skip_bytes(p, end, 1 + len) ? EL_EXT : EL_ERROR;
		default:
			return EL_ERROR;
		}

		if (! rd_be(p, end, raw_hdr, &len)) {
			return EL_ERROR;
		}
	}

	if ((uint64_t)(end - *p) < len) {
		return EL_ERROR;
	}

	*key_ok = len != 0 && ((*p)[0] == AS_STR_TYPE || (*p)[0] == AS_BLOB_TYPE);
	*p += len;

	return EL_SCALAR;
}

static void
reference_walk(const uint8_t *buf, uint32_t sz, walk_result *res)
{
	const uint8_t *p = buf;
	const uint8_t *end = buf + sz;
	frame *stack = NULL;
	size_t sp = 0;
	size_t cap = 0;
	bool started = false;

	memset(res, 0, sizeof(*res));

	while (true) {
		while (sp > 0 && stack[sp - 1].remaining == 0) {
			sp--;
		}

		if (sp == 0 && started) {
			break;
		}

		started = true;

		bool is_key = sp > 0 && stack[sp - 1].is_map &&
				stack[sp - 1].remaining % 2 == 0;

		if (sp > 0) {
			stack[sp - 1].remaining--;
		}

		uint64_t count = 0;
		bool key_ok;
		el_kind kind = parse_el(&p, end, &count, &key_ok);

		if (kind == EL_ERROR) {
			res->corrupt = true;
			break;
		}

		if (kind != EL_LIST && kind != EL_MAP) {
			if (is_key && ! key_ok) {
				res->invalid_key = true;
			}

			continue;
		}

		if (is_key) {
			res->invalid_key = true;
		}

		uint32_t level = (uint32_t)sp + 1;

		if (level > res->strict_depth) {
			res->strict_depth = level;
		}

		if (count != 0 && level > res->server_depth) {
			res->server_depth = level;
		}

		if (count == 0) {
			continue;
		}

		if (sp == cap) {
			cap = cap == 0 ? 64 : cap * 2;
			stack = realloc(stack, cap * sizeof(frame));

			if (stack == NULL) {
				fprintf(stderr, "out of memory\n");
				exit(1);
			}
		}

		bool is_map = kind == EL_MAP;

		stack[sp].remaining = is_map ? 2 * count : count;
		stack[sp].is_map = is_map;
		sp++;

		if (is_map && p < end && (*p == 0xc7 || *p == 0xc8 || *p == 0xc9 ||
				(*p >= 0xd4 && *p <= 0xd8))) {
			if (parse_el(&p, end, &count, &key_ok) != EL_EXT) {
				res->corrupt = true;
				break;
			}

			kind = parse_el(&p, end, &count, &key_ok);

			if (kind != EL_SCALAR && kind != EL_EXT) {
				res->corrupt = true;
				break;
			}

			stack[sp - 1].remaining -= 2;
		}
	}

	if (! res->corrupt && p < end) {
		res->padding = true;
	}

	free(stack);
}

//==========================================================
// Expect (scan stored bytes and classify).
//

static uint32_t
hist_bucket(uint32_t depth)
{
	if (depth <= 62) {
		return 0;
	}

	if (depth <= 66) {
		return depth - 62;
	}

	return depth < 200 ? 5 : 6;
}

static bool
expect_callback(const as_val *val, void *udata)
{
	if (val == NULL) {
		return true;
	}

	expect_stats *st = udata;
	as_record *rec = as_record_fromval(val);

	if (rec == NULL) {
		return true;
	}

	__atomic_add_fetch(&st->records, 1, __ATOMIC_RELAXED);

	bool flagged = false;

	for (uint16_t i = 0; i < rec->bins.size; i++) {
		as_bin *bin = &rec->bins.entries[i];
		as_val *v = (as_val *)bin->valuep;

		if (v == NULL || v->type != AS_BYTES) {
			continue;
		}

		as_bytes *b = (as_bytes *)v;
		as_bytes_type type = as_bytes_get_type(b);

		if (type != AS_BYTES_LIST && type != AS_BYTES_MAP) {
			continue;
		}

		__atomic_add_fetch(type == AS_BYTES_LIST ? &st->lists : &st->maps, 1,
				__ATOMIC_RELAXED);

		walk_result wr;

		reference_walk(as_bytes_get(b), as_bytes_size(b), &wr);

		bool over = wr.server_depth > st->max_depth;
		bool strict_only = ! over && wr.strict_depth > st->max_depth;

		if (over) {
			__atomic_add_fetch(&st->over, 1, __ATOMIC_RELAXED);
		}

		if (strict_only) {
			__atomic_add_fetch(&st->strict_only, 1, __ATOMIC_RELAXED);
		}

		if (wr.invalid_key) {
			__atomic_add_fetch(&st->invalid_key, 1, __ATOMIC_RELAXED);
		}

		if (wr.corrupt) {
			__atomic_add_fetch(&st->corrupt, 1, __ATOMIC_RELAXED);
		}

		if (wr.padding) {
			__atomic_add_fetch(&st->padding, 1, __ATOMIC_RELAXED);
		}

		__atomic_add_fetch(&st->hist[hist_bucket(wr.strict_depth)], 1,
				__ATOMIC_RELAXED);
		atomic_max_u32(&st->max_server_depth, wr.server_depth);
		atomic_max_u32(&st->max_strict_depth, wr.strict_depth);

		if (! (over || strict_only || wr.invalid_key || wr.corrupt ||
				wr.padding)) {
			continue;
		}

		flagged = true;

		if (st->digest_fd != NULL) {
			as_digest *d = as_key_digest(&rec->key);
			char hex[2 * AS_DIGEST_VALUE_SIZE + 1];

			for (uint32_t k = 0; k < AS_DIGEST_VALUE_SIZE; k++) {
				sprintf(hex + 2 * k, "%02x", d->value[k]);
			}

			flockfile(st->digest_fd);
			fprintf(st->digest_fd, "%s\t%s\t%u\t%u\t%s%s%s%s%s\n", hex, bin->name,
					wr.server_depth, wr.strict_depth, over ? "over," : "",
					strict_only ? "strict_only," : "",
					wr.invalid_key ? "invalid_key," : "",
					wr.corrupt ? "corrupt," : "", wr.padding ? "padding," : "");
			funlockfile(st->digest_fd);
		}
	}

	if (flagged) {
		__atomic_add_fetch(&st->records_flagged, 1, __ATOMIC_RELAXED);
	}

	return true;
}

//==========================================================
// Dump (show the stored bytes of one record).
//

static void
print_hex(const uint8_t *p, uint32_t n)
{
	for (uint32_t i = 0; i < n; i++) {
		printf("%02x%s", p[i], i + 1 < n ? " " : "");
	}
}

static int
dump_record(aerospike *as, const char *ns, const char *set, int64_t key_ix)
{
	as_policy_read rp;
	as_policy_read_init(&rp);
	rp.deserialize = false;

	as_key key;
	as_key_init_int64(&key, ns, set, key_ix);

	as_error err;
	as_record *rec = NULL;

	if (aerospike_key_get(as, &err, &rp, &key, &rec) != AEROSPIKE_OK) {
		fprintf(stderr, "get: %d %s\n", err.code, err.message);
		return 1;
	}

	for (uint16_t i = 0; i < rec->bins.size; i++) {
		as_bin *bin = &rec->bins.entries[i];
		as_val *v = (as_val *)bin->valuep;

		if (v == NULL || v->type != AS_BYTES) {
			printf("%s: not raw bytes\n", bin->name);
			continue;
		}

		as_bytes *b = (as_bytes *)v;
		uint32_t sz = as_bytes_size(b);
		const uint8_t *p = as_bytes_get(b);
		walk_result wr;

		reference_walk(p, sz, &wr);
		printf("%s: type %d size %u server_depth %u strict_depth %u\n  ",
				bin->name, (int)as_bytes_get_type(b), sz, wr.server_depth,
				wr.strict_depth);

		if (sz <= 64) {
			print_hex(p, sz);
		}
		else {
			print_hex(p, 24);
			printf(" ... ");
			print_hex(p + sz - 24, 24);
		}

		printf("\n");
	}

	as_record_destroy(rec);
	return 0;
}

//==========================================================
// Main.
//

static void
usage(void)
{
	fprintf(stderr,
			"usage:\n"
			"  gen_cdt load   -h host -n ns -d dataset -r records [-S seed] [-t threads]\n"
			"                 [--violators N | --violator-pct P] [--depth D] [--set S]\n"
			"  gen_cdt expect -h host -n ns [--max-depth 64] [--digests file]\n"
			"  gen_cdt dump   -h host -n ns -k key\n"
			"\n"
			"datasets: scalar (D1), map (D2M), list (D2L), ordered (D3), mixed (D4),\n"
			"          deep (D5)\n");
	exit(2);
}

static bool
parse_dataset(const char *s, dataset *ds)
{
	for (uint32_t i = 0; i < sizeof(dataset_names) / sizeof(dataset_names[0]);
			i++) {
		if (strcmp(s, dataset_names[i]) == 0) {
			*ds = (dataset)i;
			return true;
		}
	}

	return false;
}

int
main(int argc, char **argv)
{
	if (argc < 2) {
		usage();
	}

	const char *mode = argv[1];
	const char *host = "127.0.0.1";
	const char *ns = NULL;
	const char *set = "";
	const char *digests = NULL;
	dataset ds = DS_SCALAR;
	bool have_ds = false;
	uint64_t n_records = 0;
	uint64_t seed = 1;
	uint32_t n_threads = 16;
	uint64_t n_violators = 0;
	uint32_t violator_pct = 0;
	uint32_t deep_depth = 200000;
	uint32_t max_depth = 64;
	int64_t dump_key = -1;

	static struct option opts[] = {
		{ "host", required_argument, NULL, 'h' },
		{ "namespace", required_argument, NULL, 'n' },
		{ "dataset", required_argument, NULL, 'd' },
		{ "records", required_argument, NULL, 'r' },
		{ "seed", required_argument, NULL, 'S' },
		{ "threads", required_argument, NULL, 't' },
		{ "set", required_argument, NULL, 's' },
		{ "violators", required_argument, NULL, 'v' },
		{ "violator-pct", required_argument, NULL, 'p' },
		{ "depth", required_argument, NULL, 'D' },
		{ "max-depth", required_argument, NULL, 'm' },
		{ "digests", required_argument, NULL, 'o' },
		{ "key", required_argument, NULL, 'k' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 2;

	int opt;

	while ((opt = getopt_long(argc, argv, "h:n:d:r:S:t:s:k:", opts, NULL)) != -1) {
		switch (opt) {
		case 'h':
			host = optarg;
			break;
		case 'n':
			ns = optarg;
			break;
		case 'd':
			if (! parse_dataset(optarg, &ds)) {
				usage();
			}
			have_ds = true;
			break;
		case 'r':
			n_records = strtoull(optarg, NULL, 10);
			break;
		case 'S':
			seed = strtoull(optarg, NULL, 10);
			break;
		case 't':
			n_threads = (uint32_t)strtoul(optarg, NULL, 10);
			break;
		case 's':
			set = optarg;
			break;
		case 'v':
			n_violators = strtoull(optarg, NULL, 10);
			break;
		case 'p':
			violator_pct = (uint32_t)strtoul(optarg, NULL, 10);
			break;
		case 'D':
			deep_depth = (uint32_t)strtoul(optarg, NULL, 10);
			break;
		case 'm':
			max_depth = (uint32_t)strtoul(optarg, NULL, 10);
			break;
		case 'o':
			digests = optarg;
			break;
		case 'k':
			dump_key = strtoll(optarg, NULL, 10);
			break;
		default:
			usage();
		}
	}

	if (ns == NULL || n_threads == 0 || deep_depth < 2) {
		usage();
	}

	as_config cfg;
	as_config_init(&cfg);
	as_config_add_host(&cfg, host, 3000);

	aerospike as;
	as_error err;

	aerospike_init(&as, &cfg);

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		fprintf(stderr, "connect: %d %s\n", err.code, err.message);
		return 1;
	}

	int rc = 0;

	if (strcmp(mode, "load") == 0) {
		if (! have_ds || n_records == 0) {
			usage();
		}

		uint64_t stride = 0;

		if (n_violators != 0) {
			stride = n_records / n_violators;
		}
		else if (violator_pct != 0) {
			stride = 100 / violator_pct;
		}

		if (n_violators != 0 && stride == 0) {
			stride = 1;
		}

		pthread_t threads[n_threads];
		load_args args[n_threads];

		for (uint32_t t = 0; t < n_threads; t++) {
			args[t] = (load_args){
				.as = &as, .ns = ns, .set = set, .ds = ds, .n_records = n_records,
				.violator_stride = stride, .deep_depth = deep_depth, .seed = seed,
				.n_threads = n_threads, .thread_ix = t
			};
			pthread_create(&threads[t], NULL, load_thread, &args[t]);
		}

		uint64_t written = 0;
		uint64_t failed = 0;
		uint64_t bytes = 0;

		for (uint32_t t = 0; t < n_threads; t++) {
			pthread_join(threads[t], NULL);
			written += args[t].written;
			failed += args[t].failed;
			bytes += args[t].bytes;
		}

		printf("loaded ns=%s dataset=%s records=%" PRIu64 " written=%" PRIu64
				" failed=%" PRIu64 " cdt_bytes=%" PRIu64 " violator_stride=%" PRIu64
				"\n", ns, dataset_names[ds], n_records, written,
				failed, bytes, stride);

		rc = failed == 0 ? 0 : 3;
	}
	else if (strcmp(mode, "expect") == 0) {
		expect_stats st = { .max_depth = max_depth };

		if (digests != NULL && (st.digest_fd = fopen(digests, "w")) == NULL) {
			perror(digests);
			return 1;
		}

		as_scan scan;
		as_scan_init(&scan, ns, set);
		scan.deserialize_list_map = false;

		as_policy_scan sp;
		as_policy_scan_init(&sp);
		sp.base.socket_timeout = 600000;

		if (aerospike_scan_foreach(&as, &err, &sp, &scan, expect_callback, &st) !=
				AEROSPIKE_OK) {
			fprintf(stderr, "scan: %d %s\n", err.code, err.message);
			rc = 1;
		}

		as_scan_destroy(&scan);

		if (st.digest_fd != NULL) {
			fclose(st.digest_fd);
		}

		printf("expect ns=%s max_depth=%u\n", ns, max_depth);
		printf("  records          %" PRIu64 "\n", st.records);
		printf("  records_flagged  %" PRIu64 "\n", st.records_flagged);
		printf("  list_bins        %" PRIu64 "\n", st.lists);
		printf("  map_bins         %" PRIu64 "\n", st.maps);
		printf("  over             %" PRIu64 "\n", st.over);
		printf("  strict_only      %" PRIu64 "\n", st.strict_only);
		printf("  invalid_key      %" PRIu64 "\n", st.invalid_key);
		printf("  corrupt          %" PRIu64 "\n", st.corrupt);
		printf("  padding          %" PRIu64 "\n", st.padding);
		printf("  max_server_depth %u\n", st.max_server_depth);
		printf("  max_strict_depth %u\n", st.max_strict_depth);

		for (uint32_t i = 0; i < 7; i++) {
			printf("  strict_depth %-6s %" PRIu64 "\n", hist_names[i], st.hist[i]);
		}
	}
	else if (strcmp(mode, "dump") == 0 && dump_key >= 0) {
		rc = dump_record(&as, ns, set, dump_key);
	}
	else {
		usage();
	}

	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return rc;
}
