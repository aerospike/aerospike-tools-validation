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

#include "ref_walk.h"

// Loads list/map bins nested around the 64-level limit, and checks a namespace
// against the classes computed from its stored bytes.
//
//   gen_cdt load    -h host -n ns -d mixed|deep -r records [...]
//   gen_cdt expect  -h host -n ns [--digests file]
//   gen_cdt rewrite -h host -n ns

//==========================================================
// Typedefs & constants.
//

#define ORDERED_FLAG 1
#define MAX_DEPTH 64
#define N_THREADS 8

typedef struct wbuf_s {
	uint8_t *buf;
	size_t sz;
	size_t cap;
} wbuf;

typedef enum {
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
	SHAPE_MAP_VALUE_CHAIN,
	SHAPE_ORDERED_CHAIN,
	SHAPE_KORDERED_CHAIN,
	SHAPE_MIXED_CHAIN,
	SHAPE_MIXED_EMPTY_AT,
	SHAPE_MIXED_FLAGS_AT
} shape;

typedef struct deep_case_s {
	shape shape;
	uint32_t depth;
} deep_case;

// Every other record of the mixed dataset gets a bin with the next of these
// shapes; the depth 63/64 ones and the empty list at 64 are within the limit.
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
	{ SHAPE_FLAGS0_AT, 65 },
	{ SHAPE_ORDERED_CHAIN, 64 }, { SHAPE_ORDERED_CHAIN, 65 },
	{ SHAPE_KORDERED_CHAIN, 64 }, { SHAPE_KORDERED_CHAIN, 65 },
	{ SHAPE_MIXED_CHAIN, 64 }, { SHAPE_MIXED_CHAIN, 65 },
	{ SHAPE_MIXED_CHAIN, 200 },
	{ SHAPE_MIXED_EMPTY_AT, 65 },
	{ SHAPE_MIXED_FLAGS_AT, 65 }
};

// One level of a mixed chain: the bytes before the child, which is always the
// last element of the level.
static const struct {
	uint8_t prefix[16];
	uint32_t sz;
} mixed_levels[] = {
	{ { 0x91 }, 1 },
	{ { 0x92, 0xc7, 0x00, 0x01 }, 4 },
	{ { 0x82, 0xc7, 0x00, 0x01, 0xc0, 0x01 }, 6 },
	{ { 0x82, 0xc7, 0x00, 0x03, 0xc0, 0xa2, 0x03, 'a' }, 8 },
	{ { 0xdc, 0x00, 0x01 }, 3 },
	{ { 0xdf, 0x00, 0x00, 0x00, 0x01, 0xcd, 0x01, 0x00 }, 8 },
	{ { 0x93, 0xa3, 0x17, '{', '}', 0xcb, 0, 0, 0, 0, 0, 0, 0, 0 }, 14 },
	{ { 0x82, 0xc4, 0x02, 0x04, 0xff, 0xc0, 0xa2, 0x03, 'b' }, 9 },
	{ { 0x92, 0xa5, 0x03, 0x91, 0x91, 0x91, 0x91 }, 7 }
};

#define N_MIXED_LEVELS (sizeof(mixed_levels) / sizeof(mixed_levels[0]))

#define N_VIOLATORS (sizeof(violators) / sizeof(violators[0]))

static const shape deep_shapes[] = {
	SHAPE_LIST_CHAIN, SHAPE_PAIR_CHAIN, SHAPE_MAP_CHAIN, SHAPE_MAP_VALUE_CHAIN
};

#define N_DEEP_SHAPES (sizeof(deep_shapes) / sizeof(deep_shapes[0]))

typedef struct load_args_s {
	aerospike *as;
	const char *ns;
	dataset ds;
	uint64_t n_records;
	uint64_t key_offset;
	uint32_t deep_depth;
	uint32_t thread_ix;
	uint64_t failed;
} load_args;

typedef struct expect_stats_s {
	FILE *digest_fd;
	uint64_t records;
	uint64_t flagged;
	uint64_t over;
	uint64_t strict_only;
	uint64_t invalid_key;
	uint64_t corrupt;
	uint32_t deepest;
} expect_stats;

typedef struct rewrite_stats_s {
	aerospike *as;
	uint64_t n[N_REF_CLASSES][2]; // [class][rejected]
	uint64_t other_errors;
} rewrite_stats;

static uint32_t g_errors_shown = 0;


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
	case SHAPE_KORDERED_CHAIN:
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
	case SHAPE_ORDERED_CHAIN:
		for (uint32_t i = 0; i < depth; i++) {
			wb_list_hdr(wb, 2);
			wb_ext_flags(wb, ORDERED_FLAG);
		}

		wb_int(wb, 1);
		break;
	case SHAPE_KORDERED_CHAIN:
		for (uint32_t i = 0; i < depth; i++) {
			wb_map_hdr(wb, 2);
			wb_ext_flags(wb, ORDERED_FLAG);
			wb_u8(wb, 0xc0);
			wb_int(wb, 1);
		}

		wb_int(wb, 1);
		break;
	case SHAPE_MIXED_CHAIN:
	case SHAPE_MIXED_EMPTY_AT:
	case SHAPE_MIXED_FLAGS_AT: {
		uint32_t n_levels = s == SHAPE_MIXED_CHAIN ? depth : depth - 1;

		for (uint32_t i = 0; i < n_levels; i++) {
			wb_raw(wb, mixed_levels[i % N_MIXED_LEVELS].prefix,
					mixed_levels[i % N_MIXED_LEVELS].sz);
		}

		if (s == SHAPE_MIXED_CHAIN) {
			wb_typed_raw(wb, 0x17, "{}", 2);
		}
		else if (s == SHAPE_MIXED_EMPTY_AT) {
			wb_list_hdr(wb, 0);
		}
		else {
			wb_map_hdr(wb, 1);
			wb_ext_flags(wb, 3);
			wb_u8(wb, 0xc0);
		}
		break;
	}
	}
}

//==========================================================
// Load.
//

static void *
load_thread(void *udata)
{
	load_args *la = udata;
	wbuf main_wb = { 0 };
	wbuf deep_wb = { 0 };
	wbuf tmp = { 0 };

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.key = AS_POLICY_KEY_SEND;
	wp.base.total_timeout = 30000;

	for (uint64_t i = la->thread_ix; i < la->n_records; i += N_THREADS) {
		uint64_t rng = 1 ^ (i * 0x2545f4914f6cdd1dULL);
		as_record rec;

		main_wb.sz = 0;
		deep_wb.sz = 0;
		as_record_init(&rec, 2);

		if (la->ds == DS_MIXED) {
			if (i % 2 == 0) {
				build_unordered_map(&main_wb, &rng, rng_range(&rng, 5, 50));
				as_record_set_raw_typep(&rec, "m", main_wb.buf,
						(uint32_t)main_wb.sz, AS_BYTES_MAP, false);
			}
			else {
				build_unordered_list(&main_wb, &tmp, &rng, 200);
				as_record_set_raw_typep(&rec, "l", main_wb.buf,
						(uint32_t)main_wb.sz, AS_BYTES_LIST, false);
			}

			if (i % 2 == 0) {
				const deep_case *dc = &violators[(i / 2) % N_VIOLATORS];

				build_deep(&deep_wb, dc->shape, dc->depth);
				as_record_set_raw_typep(&rec, "deep", deep_wb.buf,
						(uint32_t)deep_wb.sz, shape_type(dc->shape), false);
			}
		}
		else {
			shape sh = deep_shapes[i % N_DEEP_SHAPES];

			build_deep(&deep_wb, sh, la->deep_depth);
			as_record_set_raw_typep(&rec, "deep", deep_wb.buf,
					(uint32_t)deep_wb.sz, shape_type(sh), false);
		}

		as_key key;
		as_error err;

		as_key_init_int64(&key, la->ns, "", (int64_t)(i + la->key_offset));

		if (aerospike_key_put(la->as, &err, &wp, &key, &rec) != AEROSPIKE_OK) {
			la->failed++;

			if (__atomic_add_fetch(&g_errors_shown, 1, __ATOMIC_RELAXED) <= 5) {
				fprintf(stderr, "put %" PRIu64 ": %d %s\n", i, err.code,
						err.message);
			}
		}

		as_record_destroy(&rec);
	}

	free(main_wb.buf);
	free(deep_wb.buf);
	free(tmp.buf);

	return NULL;
}

static int
load(aerospike *as, const char *ns, dataset ds, uint64_t n_records,
		uint64_t key_offset, uint32_t deep_depth)
{
	pthread_t threads[N_THREADS];
	load_args args[N_THREADS];
	uint64_t failed = 0;

	for (uint32_t t = 0; t < N_THREADS; t++) {
		args[t] = (load_args){
			.as = as, .ns = ns, .ds = ds, .n_records = n_records,
			.key_offset = key_offset, .deep_depth = deep_depth, .thread_ix = t
		};
		pthread_create(&threads[t], NULL, load_thread, &args[t]);
	}

	for (uint32_t t = 0; t < N_THREADS; t++) {
		pthread_join(threads[t], NULL);
		failed += args[t].failed;
	}

	printf("loaded %" PRIu64 " record(s), %" PRIu64 " failed\n", n_records,
			failed);

	return failed == 0 ? 0 : 1;
}


//==========================================================
// Expect.
//

static bool
list_map_bytes(const as_bin *bin, const uint8_t **buf, uint32_t *sz)
{
	as_val *v = (as_val *)bin->valuep;

	if (v == NULL || v->type != AS_BYTES) {
		return false;
	}

	as_bytes *b = (as_bytes *)v;
	as_bytes_type type = as_bytes_get_type(b);

	if (type != AS_BYTES_LIST && type != AS_BYTES_MAP) {
		return false;
	}

	*buf = as_bytes_get(b);
	*sz = as_bytes_size(b);

	return true;
}

static bool
expect_callback(const as_val *val, void *udata)
{
	as_record *rec = as_record_fromval(val);

	if (rec == NULL) {
		return true;
	}

	expect_stats *st = udata;
	bool flagged = false;

	__atomic_add_fetch(&st->records, 1, __ATOMIC_RELAXED);

	for (uint16_t i = 0; i < rec->bins.size; i++) {
		const uint8_t *buf;
		uint32_t sz;

		if (! list_map_bytes(&rec->bins.entries[i], &buf, &sz)) {
			continue;
		}

		walk_result wr;

		reference_walk(buf, sz, &wr);

		ref_class c = ref_classify(&wr, MAX_DEPTH);

		atomic_max_u32(&st->deepest, wr.strict_depth);

		if (c == REF_OVER) {
			__atomic_add_fetch(&st->over, 1, __ATOMIC_RELAXED);
		}
		else if (c == REF_STRICT_ONLY) {
			__atomic_add_fetch(&st->strict_only, 1, __ATOMIC_RELAXED);
		}

		if (wr.invalid_key) {
			__atomic_add_fetch(&st->invalid_key, 1, __ATOMIC_RELAXED);
		}

		if (wr.corrupt) {
			__atomic_add_fetch(&st->corrupt, 1, __ATOMIC_RELAXED);
		}

		flagged = flagged || c != REF_OK || wr.invalid_key || wr.corrupt;
	}

	if (flagged) {
		__atomic_add_fetch(&st->flagged, 1, __ATOMIC_RELAXED);

		if (st->digest_fd != NULL) {
			as_digest *d = as_key_digest(&rec->key);

			flockfile(st->digest_fd);

			for (uint32_t k = 0; k < AS_DIGEST_VALUE_SIZE; k++) {
				fprintf(st->digest_fd, "%02x", d->value[k]);
			}

			fprintf(st->digest_fd, "\n");
			funlockfile(st->digest_fd);
		}
	}

	return true;
}


//==========================================================
// Rewrite.
//

// Puts every record with a list/map bin back with exactly its stored bytes,
// and counts which ones the server rejects, per expected class.
static bool
rewrite_callback(const as_val *val, void *udata)
{
	as_record *rec = as_record_fromval(val);

	if (rec == NULL) {
		return true;
	}

	rewrite_stats *st = udata;
	ref_class worst = REF_OK;
	bool has_cdt = false;

	for (uint16_t i = 0; i < rec->bins.size; i++) {
		const uint8_t *buf;
		uint32_t sz;

		if (! list_map_bytes(&rec->bins.entries[i], &buf, &sz)) {
			continue;
		}

		walk_result wr;

		reference_walk(buf, sz, &wr);
		has_cdt = true;

		ref_class c = ref_classify(&wr, MAX_DEPTH);

		if (c > worst) {
			worst = c;
		}
	}

	if (! has_cdt) {
		return true;
	}

	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.base.max_retries = 0;

	as_error err;
	as_status status = aerospike_key_put(st->as, &err, &wp, &rec->key, rec);

	if (status == AEROSPIKE_ERR_SERVER) {
		__atomic_add_fetch(&st->n[worst][1], 1, __ATOMIC_RELAXED);
	}
	else if (status == AEROSPIKE_OK) {
		__atomic_add_fetch(&st->n[worst][0], 1, __ATOMIC_RELAXED);
	}
	else {
		__atomic_add_fetch(&st->other_errors, 1, __ATOMIC_RELAXED);
	}

	return true;
}


//==========================================================
// Main.
//

static void
usage(void)
{
	fprintf(stderr,
			"usage: gen_cdt load    -h host -n ns -d mixed|deep -r records\n"
			"                       [--depth D] [--key-offset K]\n"
			"       gen_cdt expect  -h host -n ns [--digests file]\n"
			"       gen_cdt rewrite -h host -n ns\n");
	exit(2);
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
	const char *digests = NULL;
	dataset ds = DS_MIXED;
	uint64_t n_records = 0;
	uint64_t key_offset = 0;
	uint32_t deep_depth = 200000;

	static struct option opts[] = {
		{ "host", required_argument, NULL, 'h' },
		{ "namespace", required_argument, NULL, 'n' },
		{ "dataset", required_argument, NULL, 'd' },
		{ "records", required_argument, NULL, 'r' },
		{ "depth", required_argument, NULL, 'D' },
		{ "key-offset", required_argument, NULL, 'K' },
		{ "digests", required_argument, NULL, 'o' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 2;

	int opt;

	while ((opt = getopt_long(argc, argv, "h:n:d:r:", opts, NULL)) != -1) {
		switch (opt) {
		case 'h':
			host = optarg;
			break;
		case 'n':
			ns = optarg;
			break;
		case 'd':
			if (strcmp(optarg, "mixed") == 0) {
				ds = DS_MIXED;
			}
			else if (strcmp(optarg, "deep") == 0) {
				ds = DS_DEEP;
			}
			else {
				usage();
			}
			break;
		case 'r':
			n_records = strtoull(optarg, NULL, 10);
			break;
		case 'D':
			deep_depth = (uint32_t)strtoul(optarg, NULL, 10);
			break;
		case 'K':
			key_offset = strtoull(optarg, NULL, 10);
			break;
		case 'o':
			digests = optarg;
			break;
		default:
			usage();
		}
	}

	if (ns == NULL || deep_depth < 2) {
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
	as_scan scan;

	as_scan_init(&scan, ns, "");
	scan.deserialize_list_map = false;

	if (strcmp(mode, "load") == 0 && n_records != 0) {
		rc = load(&as, ns, ds, n_records, key_offset, deep_depth);
	}
	else if (strcmp(mode, "expect") == 0) {
		expect_stats st = { 0 };

		if (digests != NULL && (st.digest_fd = fopen(digests, "w")) == NULL) {
			perror(digests);
			return 1;
		}

		if (aerospike_scan_foreach(&as, &err, NULL, &scan, expect_callback,
				&st) != AEROSPIKE_OK) {
			fprintf(stderr, "scan: %d %s\n", err.code, err.message);
			rc = 1;
		}

		if (st.digest_fd != NULL) {
			fclose(st.digest_fd);
		}

		printf("records %" PRIu64 " flagged %" PRIu64 " over %" PRIu64
				" strict_only %" PRIu64 " invalid_key %" PRIu64 " corrupt %"
				PRIu64 " deepest %u\n", st.records, st.flagged, st.over,
				st.strict_only, st.invalid_key, st.corrupt,
				st.deepest > MAX_DEPTH ? MAX_DEPTH + 1 : st.deepest);
	}
	else if (strcmp(mode, "rewrite") == 0) {
		rewrite_stats st = { .as = &as };

		if (aerospike_scan_foreach(&as, &err, NULL, &scan, rewrite_callback,
				&st) != AEROSPIKE_OK) {
			fprintf(stderr, "scan: %d %s\n", err.code, err.message);
			rc = 1;
		}

		for (uint32_t c = 0; c < N_REF_CLASSES; c++) {
			printf("%s accepted %" PRIu64 " rejected %" PRIu64 "\n",
					ref_class_names[c], st.n[c][0], st.n[c][1]);
		}

		printf("other_errors %" PRIu64 "\n", st.other_errors);
	}
	else {
		usage();
	}

	as_scan_destroy(&scan);
	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return rc;
}
