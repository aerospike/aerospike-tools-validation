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

// Differential tests: cdt_walk() against two existing implementations on
// random input.
// - Strict rule: the client's msgpack deserializer (as_unpack_val) must fail
//   exactly when the walk reports OVER or EMPTY_OVER.
// - Key rule: the old recursive check_map_keys_internal() must fail exactly
//   when the walk reports INVALID_KEY. Inputs stay shallow so it cannot
//   overflow the stack.

#include <stdio.h>
#include <stdlib.h>

#include <aerospike/as_msgpack.h>
#include <aerospike/as_val.h>

#include <cdt_check.h>
#include <cdt_walk.h>

#include "test_buf.h"

//==========================================================
// Typedefs & constants.
//

#define N_STRICT_CASES 50000
#define N_KEY_CASES 50000
#define MAX_PRINTED 5

typedef struct gen_cfg_s {
	uint32_t spine_depth; // one path reaches exactly this level
	uint32_t max_depth; // nothing goes deeper than this
	bool any_keys; // keys of every type, otherwise unique ints and strings
} gen_cfg;

static uint64_t g_rng;
static uint32_t g_key_seq;


//==========================================================
// Random input.
//

static uint64_t
rnd(void)
{
	uint64_t z = (g_rng += 0x9e3779b97f4a7c15ULL);

	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
	z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;

	return z ^ (z >> 31);
}

static uint32_t
rnd_below(uint32_t n)
{
	return (uint32_t)(rnd() % n);
}

static void
put_list_hdr(tbuf *tb, uint32_t n)
{
	if (n < 16 && rnd_below(4) != 0) {
		tb_u8(tb, (uint8_t)(0x90 | n));
	}
	else if (rnd_below(2) == 0) {
		tb_u8(tb, 0xdc);
		tb_be(tb, n, 2);
	}
	else {
		tb_u8(tb, 0xdd);
		tb_be(tb, n, 4);
	}
}

static void
put_map_hdr(tbuf *tb, uint32_t n)
{
	if (n < 16 && rnd_below(4) != 0) {
		tb_u8(tb, (uint8_t)(0x80 | n));
	}
	else if (rnd_below(2) == 0) {
		tb_u8(tb, 0xde);
		tb_be(tb, n, 2);
	}
	else {
		tb_u8(tb, 0xdf);
		tb_be(tb, n, 4);
	}
}

// Ext is only allowed where the client deserializer accepts it (as a leading
// flags element), so callers outside the strict test can ask for it.
static void
put_scalar(tbuf *tb, bool allow_ext)
{
	switch (rnd_below(allow_ext ? 12 : 10)) {
	case 0:
		tb_u8(tb, (uint8_t)rnd_below(128));
		break;
	case 1:
		tb_u8(tb, (uint8_t)(0xe0 | rnd_below(32)));
		break;
	case 2:
		tb_u8(tb, 0xd1);
		tb_be(tb, rnd(), 2);
		break;
	case 3:
		tb_u8(tb, 0xcf);
		tb_be(tb, rnd(), 8);
		break;
	case 4: // string whose bytes look like nested lists
		tb_u8(tb, 0xa5);
		tb_u8(tb, 3);
		tb_raw(tb, (const uint8_t[]){ 0x91, 0x91, 0x81, 0x90 }, 4);
		break;
	case 5:
		tb_u8(tb, 0xc4);
		tb_u8(tb, 2);
		tb_u8(tb, 4);
		tb_u8(tb, (uint8_t)rnd());
		break;
	case 6: // geojson
		tb_raw(tb, (const uint8_t[]){ 0xa5, 0x17, '{', '"', '"', '}' }, 6);
		break;
	case 7:
		tb_u8(tb, rnd_below(2) == 0 ? 0xc2 : 0xc3);
		break;
	case 8:
		tb_u8(tb, 0xc0);
		break;
	case 9:
		tb_u8(tb, 0xcb);
		tb_be(tb, rnd(), 8);
		break;
	case 10:
		tb_raw(tb, (const uint8_t[]){ 0xd6, 0x05, 0x91, 0x91, 0x91, 0x91 }, 6);
		break;
	default:
		tb_raw(tb, (const uint8_t[]){ 0xc8, 0x00, 0x02, 0x07, 0x81, 0x90 }, 6);
		break;
	}
}

static void
put_unique_key(tbuf *tb)
{
	uint32_t k = g_key_seq++;

	if (rnd_below(2) == 0) {
		tb_u8(tb, 0xce);
		tb_be(tb, k, 4);
	}
	else {
		tb_u8(tb, 0xa5);
		tb_u8(tb, 3);
		tb_be(tb, k, 4);
	}
}

static void gen_value(tbuf *tb, const gen_cfg *cfg, uint32_t level, bool spine);

static void
put_any_key(tbuf *tb, const gen_cfg *cfg, uint32_t level)
{
	static const uint8_t invalid[][3] = {
		{ 0xc0 }, { 0xc2 }, { 0xa0 }, { 0xa2, 0x17, '{' }, { 0xc4, 0x01, 0x05 },
		{ 0xd4, 0x01, 0x00 }, { 0xca, 0, 0 }
	};
	static const uint32_t invalid_sz[] = { 1, 1, 1, 3, 3, 3, 3 };

	uint32_t pick = rnd_below(20);

	if (pick < 14) {
		put_unique_key(tb);
	}
	else if (pick < 18) {
		uint32_t i = rnd_below(7);

		tb_raw(tb, invalid[i], invalid_sz[i]);

		if (i == 6) {
			tb_u8(tb, 0);
			tb_u8(tb, 0);
		}
	}
	else {
		gen_value(tb, cfg, level, false);
	}
}

// One value at 'level'. A spine value is a list/map down to cfg->spine_depth;
// off the spine, lists/maps get rarer with depth and stop at cfg->max_depth.
static void
gen_value(tbuf *tb, const gen_cfg *cfg, uint32_t level, bool spine)
{
	bool container = spine ||
			(level <= cfg->max_depth && rnd_below(level + 2) < 2);

	if (! container) {
		put_scalar(tb, cfg->any_keys);
		return;
	}

	static const uint8_t map_flags[] = { 0x01, 0x03, 0x08 };

	bool is_map = rnd_below(2) == 0;
	bool at_end = spine && level == cfg->spine_depth;
	bool flags = rnd_below(3) == 0;
	// A persist index carries content in the ext; only the top level has one.
	uint8_t flag_bits = is_map ? map_flags[rnd_below(3)] : 0x01;
	bool persist = flags && level == 1 && rnd_below(2) == 0;
	uint32_t n = at_end && rnd_below(2) == 0 ? 0 : rnd_below(4);

	if (spine && ! at_end && n == 0) {
		n = 1;
	}

	uint32_t spine_ix = spine && ! at_end ? rnd_below(n) : UINT32_MAX;

	if (is_map) {
		put_map_hdr(tb, n + (flags ? 1 : 0));

		if (persist) {
			tb_raw(tb, (const uint8_t[]){ 0xc7, 0x02 }, 2);
			tb_u8(tb, (uint8_t)(0x10 | flag_bits));
			tb_raw(tb, (const uint8_t[]){ 0x91, 0x81, 0xc0 }, 3);
		}
		else if (flags) {
			tb_raw(tb, (const uint8_t[]){ 0xc7, 0x00 }, 2);
			tb_u8(tb, flag_bits);
			tb_u8(tb, 0xc0);
		}

		for (uint32_t i = 0; i < n; i++) {
			uint32_t key_off = tb->sz;

			if (cfg->any_keys) {
				put_any_key(tb, cfg, level + 1);
			}
			else {
				put_unique_key(tb);
			}

			// An ext as the first key is the ordered-map meta key; stored data
			// only ever has a scalar meta value.
			uint8_t kb = tb->buf[key_off];
			bool meta_key = i == 0 && ! flags && ! persist &&
					((kb >= 0xc7 && kb <= 0xc9) || (kb >= 0xd5 && kb <= 0xd8));

			if (meta_key) {
				put_scalar(tb, false);
			}
			else {
				gen_value(tb, cfg, level + 1, i == spine_ix);
			}
		}
	}
	else {
		put_list_hdr(tb, n + (flags ? 1 : 0));

		if (persist) {
			tb_raw(tb, (const uint8_t[]){ 0xc7, 0x02, 0x11, 0x91, 0x91 }, 5);
		}
		else if (flags) {
			tb_raw(tb, (const uint8_t[]){ 0xc7, 0x00, 0x01 }, 3);
		}

		for (uint32_t i = 0; i < n; i++) {
			gen_value(tb, cfg, level + 1, i == spine_ix);
		}
	}
}


//==========================================================
// Tests.
//

static void
print_hex(const tbuf *tb)
{
	for (uint32_t i = 0; i < tb->sz && i < 96; i++) {
		fprintf(stderr, "%02x", tb->buf[i]);
	}

	fprintf(stderr, tb->sz > 96 ? "...\n" : "\n");
}

static bool
unpack_ok(const tbuf *tb, uint32_t *consumed)
{
	as_unpacker pk = { .buffer = tb->buf, .offset = 0, .length = tb->sz };
	as_val *v = NULL;
	int rv = as_unpack_val(&pk, &v);

	if (v != NULL) {
		as_val_destroy(v);
	}

	*consumed = (uint32_t)pk.offset;

	return rv == 0;
}

static bool
client_has_depth_limit(void)
{
	tbuf tb = { 0 };

	for (uint32_t i = 0; i < 64; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x90);

	uint32_t consumed;
	bool ok = unpack_ok(&tb, &consumed);

	tb_free(&tb);

	return ! ok;
}

static uint32_t
test_strict(void)
{
	if (! client_has_depth_limit()) {
		printf("test_cdt_diff: SKIP strict rule, this client's deserializer has "
				"no depth limit (needs client 7.6.0+)\n");
		return 0;
	}

	uint32_t failures = 0;
	uint32_t n_fail = 0;

	for (uint32_t c = 0; c < N_STRICT_CASES; c++) {
		gen_cfg cfg = {
			.spine_depth = 55 + rnd_below(16),
			.max_depth = 70
		};
		tbuf tb = { 0 };

		gen_value(&tb, &cfg, 1, true);

		cdt_walk_result res;
		uint32_t consumed;
		bool client_ok = unpack_ok(&tb, &consumed);

		cdt_walk(tb.buf, tb.sz, 64, false, &res);

		bool walk_ok = (res.flags & (CDT_WALK_OVER | CDT_WALK_EMPTY_OVER)) == 0;

		n_fail += client_ok ? 0 : 1;

		if (client_ok != walk_ok || (res.flags & CDT_WALK_CORRUPT) != 0 ||
				(walk_ok && res.sz != consumed)) {
			if (failures++ < MAX_PRINTED) {
				fprintf(stderr, "FAIL strict case %u: client %s, walk flags 0x%x "
						"depth %u sz %u/%u\n  ", c, client_ok ? "ok" : "error",
						res.flags, res.depth, res.sz, consumed);
				print_hex(&tb);
			}
		}

		tb_free(&tb);
	}

	printf("test_cdt_diff: strict rule, %u cases (%u rejected by the client), "
			"%u mismatches\n", N_STRICT_CASES, n_fail, failures);

	return failures;
}

static uint32_t
test_keys(void)
{
	uint32_t failures = 0;
	uint32_t n_invalid = 0;

	for (uint32_t c = 0; c < N_KEY_CASES; c++) {
		gen_cfg cfg = {
			.spine_depth = 1 + rnd_below(12),
			.max_depth = 16,
			.any_keys = true
		};
		tbuf tb = { 0 };

		gen_value(&tb, &cfg, 1, true);

		cdt_walk_result res;
		const uint8_t *old_end = check_map_keys_internal(tb.buf, tb.buf + tb.sz);
		bool old_ok = old_end != NULL;

		cdt_walk(tb.buf, tb.sz, CDT_WALK_MAX_DEPTH, true, &res);

		bool walk_ok = (res.flags & CDT_WALK_INVALID_KEY) == 0;

		n_invalid += old_ok ? 0 : 1;

		if (old_ok != walk_ok || (res.flags & ~CDT_WALK_INVALID_KEY) != 0 ||
				(old_ok && old_end != tb.buf + res.sz)) {
			if (failures++ < MAX_PRINTED) {
				fprintf(stderr, "FAIL key case %u: old %s, walk flags 0x%x\n  ", c,
						old_ok ? "ok" : "invalid", res.flags);
				print_hex(&tb);
			}
		}

		tb_free(&tb);
	}

	printf("test_cdt_diff: key rule, %u cases (%u with invalid keys), "
			"%u mismatches\n", N_KEY_CASES, n_invalid, failures);

	return failures;
}


//==========================================================
// Main.
//

int
main(int argc, char **argv)
{
	g_rng = argc > 1 ? strtoull(argv[1], NULL, 10) : 1;

	uint32_t failures = test_strict() + test_keys();

	return failures == 0 ? 0 : 1;
}
