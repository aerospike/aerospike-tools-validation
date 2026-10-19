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

#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cdt_walk.h>

#include "test_buf.h"

//==========================================================
// Typedefs & constants.
//

#define OVER CDT_WALK_OVER
#define EMPTY_OVER CDT_WALK_EMPTY_OVER
#define INVALID_KEY CDT_WALK_INVALID_KEY
#define CORRUPT CDT_WALK_CORRUPT

static uint32_t g_checks;
static uint32_t g_failures;


//==========================================================
// Helpers.
//

static void
expect(const char *name, const tbuf *tb, uint32_t max_depth, bool check_keys,
		uint32_t flags, uint32_t depth, uint32_t sz)
{
	cdt_walk_result res;

	cdt_walk(tb->buf, tb->sz, max_depth, check_keys, &res);
	g_checks++;

	bool ok = res.flags == flags && res.depth == depth && res.sz <= tb->sz &&
			((flags & (OVER | CORRUPT)) != 0 || res.sz == sz);

	if (! ok) {
		g_failures++;
		fprintf(stderr, "FAIL %s: got flags 0x%x depth %u sz %u, want flags 0x%x "
				"depth %u sz %u\n", name, res.flags, res.depth, res.sz, flags,
				depth, sz);
	}
}

static void
expect_flags(const char *name, const tbuf *tb, uint32_t max_depth,
		bool check_keys, uint32_t flags)
{
	cdt_walk_result res;

	cdt_walk(tb->buf, tb->sz, max_depth, check_keys, &res);
	g_checks++;

	if (res.flags != flags) {
		g_failures++;
		fprintf(stderr, "FAIL %s: got flags 0x%x, want 0x%x\n", name, res.flags,
				flags);
	}
}

static const char *
fmt(const char *f, ...)
{
	static char buf[128];
	va_list ap;

	va_start(ap, f);
	vsnprintf(buf, sizeof(buf), f, ap);
	va_end(ap);

	return buf;
}

// Levels 1..depth-1 are single-element lists, level depth is 'inner'.
static void
chain_into(tbuf *tb, uint32_t depth, const uint8_t *inner, uint32_t inner_sz)
{
	for (uint32_t i = 1; i < depth; i++) {
		tb_u8(tb, 0x91);
	}

	tb_raw(tb, inner, inner_sz);
}


//==========================================================
// Tests.
//

static void
test_boundaries(void)
{
	for (uint32_t d = 1; d <= 70; d++) {
		tbuf tb = { 0 };

		for (uint32_t i = 0; i < d; i++) {
			tb_u8(&tb, 0x91);
		}

		tb_u8(&tb, 0x01);

		if (d <= 64) {
			expect(fmt("list chain %u", d), &tb, 64, true, 0, d, d + 1);
		}
		else {
			expect(fmt("list chain %u", d), &tb, 64, true, OVER, 65, 0);
		}

		tb.sz = 0;

		for (uint32_t i = 1; i < d; i++) {
			tb_u8(&tb, 0x92);
			tb_u8(&tb, 0x01);
		}

		tb_u8(&tb, 0x91);
		tb_u8(&tb, 0x01);

		if (d <= 64) {
			expect(fmt("pair chain %u", d), &tb, 64, true, 0, d, 2 * d);
		}
		else {
			expect(fmt("pair chain %u", d), &tb, 64, true, OVER, 65, 0);
		}

		tb.sz = 0;

		for (uint32_t i = 0; i < d; i++) {
			tb_u8(&tb, 0x81);
			tb_u8(&tb, 0x01);
		}

		tb_u8(&tb, 0x01);

		if (d <= 64) {
			expect(fmt("map chain %u", d), &tb, 64, true, 0, d, 2 * d + 1);
		}
		else {
			expect(fmt("map chain %u", d), &tb, 64, true, OVER, 65, 0);
		}

		tb_free(&tb);
	}
}

static void
test_empty_and_flags_only(void)
{
	static const uint8_t empty_list[] = { 0x90 };
	static const uint8_t empty_map[] = { 0x80 };
	static const uint8_t flags_list[] = { 0x91, 0xc7, 0x00, 0x01 };
	static const uint8_t flags_map[] = { 0x81, 0xc7, 0x00, 0x01, 0xc0 };
	static const uint8_t flags0_list[] = { 0x91, 0xc7, 0x00, 0x00 };
	tbuf tb = { 0 };

	chain_into(&tb, 64, empty_list, 1);
	expect("empty list at 64", &tb, 64, true, 0, 64, 64);

	tb.sz = 0;
	chain_into(&tb, 65, empty_list, 1);
	expect("empty list at 65", &tb, 64, true, EMPTY_OVER, 65, 65);

	tb.sz = 0;
	chain_into(&tb, 65, empty_map, 1);
	expect("empty map at 65", &tb, 64, true, EMPTY_OVER, 65, 65);

	tb.sz = 0;
	chain_into(&tb, 66, empty_list, 1);
	expect("empty list at 66", &tb, 64, true, OVER, 65, 0);

	tb.sz = 0;
	chain_into(&tb, 64, flags_list, sizeof(flags_list));
	expect("flags-only list at 64", &tb, 64, true, 0, 64, 67);

	tb.sz = 0;
	chain_into(&tb, 65, flags_list, sizeof(flags_list));
	expect("flags-only list at 65", &tb, 64, true, OVER, 65, 0);

	tb.sz = 0;
	chain_into(&tb, 65, flags_map, sizeof(flags_map));
	expect("flags-only map at 65", &tb, 64, true, OVER, 65, 0);

	tb.sz = 0;
	chain_into(&tb, 65, flags0_list, sizeof(flags0_list));
	expect("flags-0 ext list at 65", &tb, 64, true, OVER, 65, 0);

	// Two empty lists at 65 under one parent, then more data: the walk goes on.
	static const uint8_t two_empty[] = { 0x93, 0x90, 0x90, 0x01 };

	tb.sz = 0;
	chain_into(&tb, 64, two_empty, sizeof(two_empty));
	expect("two empties at 65", &tb, 64, true, EMPTY_OVER, 65, 67);

	tb_free(&tb);
}

static void
test_map_keys(void)
{
	static const uint8_t list_key[] = { 0x81, 0x91, 0x01, 0x01 };
	static const uint8_t map_key[] = { 0x81, 0x80, 0x01 };
	tbuf tb = { 0 };

	tb_raw(&tb, list_key, sizeof(list_key));
	expect("list as key", &tb, 64, true, INVALID_KEY, 2, 4);
	expect("list as key, keys off", &tb, 64, false, 0, 2, 4);

	tb.sz = 0;
	tb_raw(&tb, map_key, sizeof(map_key));
	expect("empty map as key", &tb, 64, true, INVALID_KEY, 2, 3);

	// The key itself is a chain reaching level 65.
	tb.sz = 0;
	tb_u8(&tb, 0x81);

	for (uint32_t i = 0; i < 64; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x01);
	tb_u8(&tb, 0x01);
	expect("key chain to 65", &tb, 64, true, INVALID_KEY | OVER, 65, 0);
	expect("key chain to 65, keys off", &tb, 64, false, OVER, 65, 0);

	// Empty list at level 65 inside a key.
	tb.sz = 0;
	tb_u8(&tb, 0x81);

	for (uint32_t i = 0; i < 63; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x90);
	tb_u8(&tb, 0x01);
	expect("key with empty at 65", &tb, 64, true, INVALID_KEY | EMPTY_OVER, 65,
			66);

	// An invalid key does not stop the walk: a later over-deep value is found.
	tb.sz = 0;
	tb_u8(&tb, 0x92);
	tb_u8(&tb, 0x81);
	tb_u8(&tb, 0xc0);
	tb_u8(&tb, 0x01);

	for (uint32_t i = 0; i < 64; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x01);
	expect("invalid key then deep", &tb, 64, true, INVALID_KEY | OVER, 65, 0);

	tb_free(&tb);
}

static void
test_key_types(void)
{
	static const struct {
		const char *name;
		uint8_t key[9];
		uint32_t key_sz;
		bool valid;
	} cases[] = {
		{ "fixint", { 0x05 }, 1, true },
		{ "negative fixint", { 0xff }, 1, true },
		{ "uint8", { 0xcc, 0x80 }, 2, true },
		{ "uint16", { 0xcd, 0x01, 0x00 }, 3, true },
		{ "uint32", { 0xce, 0, 1, 0, 0 }, 5, true },
		{ "uint64", { 0xcf, 0, 0, 0, 1, 0, 0, 0, 0 }, 9, true },
		{ "int8", { 0xd0, 0x80 }, 2, true },
		{ "int16", { 0xd1, 0x80, 0x00 }, 3, true },
		{ "int32", { 0xd2, 0x80, 0, 0, 0 }, 5, true },
		{ "int64", { 0xd3, 0x80, 0, 0, 0, 0, 0, 0, 0 }, 9, true },
		{ "string", { 0xa2, 0x03, 'a' }, 3, true },
		{ "str8 string", { 0xd9, 0x02, 0x03, 'a' }, 4, true },
		{ "blob", { 0xc4, 0x02, 0x04, 0xff }, 4, true },
		{ "fixstr blob", { 0xa2, 0x04, 0xff }, 3, true },
		{ "empty fixstr", { 0xa0 }, 1, false },
		{ "empty bin8", { 0xc4, 0x00 }, 2, false },
		{ "geojson", { 0xa2, 0x17, '{' }, 3, false },
		{ "other bytes type", { 0xc4, 0x02, 0x05, 0xff }, 4, false },
		{ "nil", { 0xc0 }, 1, false },
		{ "false", { 0xc2 }, 1, false },
		{ "true", { 0xc3 }, 1, false },
		{ "float", { 0xca, 0, 0, 0, 0 }, 5, false },
		{ "double", { 0xcb, 0, 0, 0, 0, 0, 0, 0, 0 }, 9, false },
		{ "fixext1", { 0xd4, 0x01, 0x00 }, 3, false }
	};

	tbuf tb = { 0 };

	for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		uint32_t key_sz = cases[i].key_sz;

		tb.sz = 0;
		tb_u8(&tb, 0x81);
		tb_raw(&tb, cases[i].key, key_sz);
		tb_u8(&tb, 0x01);

		expect(fmt("key %s", cases[i].name), &tb, 64, true,
				cases[i].valid ? 0 : INVALID_KEY, 1, key_sz + 2);

		// Same key three levels down, as a later key of a nested map.
		tbuf deep = { 0 };

		tb_u8(&deep, 0x91);
		tb_u8(&deep, 0x91);
		tb_u8(&deep, 0x82);
		tb_u8(&deep, 0x07);
		tb_u8(&deep, 0x07);
		tb_raw(&deep, tb.buf + 1, tb.sz - 1);

		expect(fmt("nested key %s", cases[i].name), &deep, 64, true,
				cases[i].valid ? 0 : INVALID_KEY, 3, deep.sz);
		tb_free(&deep);
	}

	tb_free(&tb);
}

static void
test_meta_and_ext(void)
{
	static const uint8_t meta_nil[] = {
		0x82, 0xc7, 0x00, 0x01, 0xc0, 0x01, 0x02
	};
	static const uint8_t meta_int[] = {
		0x82, 0xc7, 0x00, 0x01, 0x05, 0x01, 0x02
	};
	static const uint8_t meta_list[] = {
		0x82, 0xc7, 0x00, 0x01, 0x91, 0x01, 0x01, 0x02
	};
	static const uint8_t meta_only[] = { 0x81, 0xc7, 0x00, 0x01, 0xc0 };
	static const uint8_t fixext1_key[] = { 0x81, 0xd4, 0x01, 0x00, 0x01 };
	static const uint8_t ext_payload[] = {
		0x92, 0xc7, 0x04, 0x01, 0x91, 0x91, 0x91, 0x91, 0x01
	};
	static const uint8_t ordered_list[] = {
		0x93, 0xc7, 0x00, 0x01, 0x01, 0x02
	};
	static const uint8_t later_ext_key[] = {
		0x82, 0x01, 0x02, 0xd5, 0x01, 0x00, 0x00, 0x03
	};
	tbuf tb = { 0 };

	tb_raw(&tb, meta_nil, sizeof(meta_nil));
	expect("meta pair, nil value", &tb, 64, true, 0, 1, sizeof(meta_nil));

	tb.sz = 0;
	tb_raw(&tb, meta_int, sizeof(meta_int));
	expect("meta pair, int value", &tb, 64, true, 0, 1, sizeof(meta_int));

	tb.sz = 0;
	tb_raw(&tb, meta_list, sizeof(meta_list));
	expect("meta pair, list value", &tb, 64, true, CORRUPT, 1, 0);

	tb.sz = 0;
	tb_raw(&tb, meta_only, sizeof(meta_only));
	expect("meta pair only", &tb, 64, true, 0, 1, sizeof(meta_only));

	tb.sz = 0;
	tb_raw(&tb, fixext1_key, sizeof(fixext1_key));
	expect("fixext1 first key is not meta", &tb, 64, true, INVALID_KEY, 1,
			sizeof(fixext1_key));

	tb.sz = 0;
	tb_raw(&tb, ext_payload, sizeof(ext_payload));
	expect("ext payload is opaque", &tb, 64, true, 0, 1, sizeof(ext_payload));

	tb.sz = 0;
	tb_raw(&tb, ordered_list, sizeof(ordered_list));
	expect("ordered list", &tb, 64, true, 0, 1, sizeof(ordered_list));

	tb.sz = 0;
	tb_raw(&tb, later_ext_key, sizeof(later_ext_key));
	expect("ext as a later key", &tb, 64, true, INVALID_KEY, 1,
			sizeof(later_ext_key));

	tb_free(&tb);
}

static void
test_corrupt(void)
{
	static const uint8_t samples[][12] = {
		{ 0x93, 0x01, 0xa2, 0x03, 'a', 0x81, 0x01, 0x90 },
		{ 0x82, 0xc7, 0x00, 0x01, 0xc0, 0xcd, 0x01, 0x00, 0xc4, 0x02, 0x04, 0x01 },
		{ 0x92, 0xdc, 0x00, 0x01, 0xcd, 0x01, 0x00, 0xc3 },
		{ 0x91, 0xd8, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0 }
	};
	static const uint32_t sample_sz[] = { 8, 12, 8, 12 };

	// The last sample's fixext16 needs 17 bytes after its header byte, so even
	// the whole sample is short.
	for (uint32_t s = 0; s < 4; s++) {
		for (uint32_t len = 0; len < sample_sz[s]; len++) {
			tbuf tb = { 0 };

			tb_raw(&tb, samples[s], len);
			expect_flags(fmt("sample %u truncated to %u", s, len), &tb, 64, true,
					CORRUPT);
			tb_free(&tb);
		}
	}

	{
		tbuf tb = { 0 };

		tb_raw(&tb, samples[0], sample_sz[0]);
		expect("sample 0 whole", &tb, 64, true, 0, 3, 8);

		tb.sz = 0;
		tb_raw(&tb, samples[1], sample_sz[1]);
		expect("sample 1 whole", &tb, 64, true, 0, 1, 12);

		tb.sz = 0;
		tb_raw(&tb, samples[2], sample_sz[2]);
		expect("sample 2 whole", &tb, 64, true, 0, 2, 8);

		tb.sz = 0;
		tb_raw(&tb, samples[3], sample_sz[3]);
		expect("sample 3 whole", &tb, 64, true, CORRUPT, 1, 0);
		tb_free(&tb);
	}

	static const struct {
		const char *name;
		uint8_t b[16];
		uint32_t sz;
	} cases[] = {
		{ "empty buffer", { 0 }, 0 },
		{ "never-used 0xc1", { 0x91, 0xc1 }, 2 },
		{ "list32 huge count", { 0xdd, 0xff, 0xff, 0xff, 0xff, 0x01 }, 6 },
		{ "map32 count doubles past 2^32", { 0xdf, 0x80, 0x00, 0x00, 0x01, 0x01, 0x01 }, 7 },
		{ "map32 count just over half", { 0xdf, 0, 0, 0, 2, 1, 1, 1 }, 8 },
		{ "list16 count one too many", { 0xdc, 0x00, 0x03, 0x01, 0x01 }, 5 },
		{ "str32 huge length", { 0x91, 0xdb, 0xff, 0xff, 0xff, 0xff, 0x03 }, 7 },
		{ "ext32 huge length", { 0x91, 0xc9, 0xff, 0xff, 0xff, 0xff, 0x01 }, 7 },
		{ "meta key without value", { 0x81, 0xc7, 0x00, 0x01 }, 4 },
		{ "last uint16 short", { 0x91, 0xcd, 0x01 }, 3 },
		{ "last uint64 short", { 0x91, 0xcf, 0, 0, 0, 0, 0, 0, 0 }, 9 },
		{ "last double short", { 0x91, 0xcb, 0, 0, 0 }, 5 },
		{ "last fixext1 short", { 0x91, 0xd4, 0x01 }, 3 },
		{ "last ext8 short", { 0x91, 0xc7, 0x02, 0x01, 0x00 }, 5 },
		{ "last str8 short", { 0x91, 0xd9, 0x03, 0x03, 0x61 }, 5 }
	};

	for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		tbuf tb = { 0 };

		tb_raw(&tb, cases[i].b, cases[i].sz);
		expect_flags(cases[i].name, &tb, 64, true, CORRUPT);
		tb_free(&tb);
	}

	// Depth is charged before the count is checked, as on the server.
	tbuf tb = { 0 };
	static const uint8_t huge[] = { 0xdd, 0xff, 0xff, 0xff, 0xff };

	chain_into(&tb, 65, huge, sizeof(huge));
	expect_flags("huge count at 65", &tb, 64, true, OVER);

	// A count the remaining bytes cannot hold is corrupt at once, even when
	// the bytes that follow would reach level 65 before running out.
	tb.sz = 0;
	tb_u8(&tb, 0xde);
	tb_be(&tb, 40, 2);

	for (uint32_t i = 0; i < 64; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x01);
	expect_flags("map count over half the bytes", &tb, 64, true, CORRUPT);

	tb.sz = 0;
	tb_u8(&tb, 0xdc);
	tb_be(&tb, 80, 2);

	for (uint32_t i = 0; i < 64; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x01);
	expect_flags("list count over the bytes", &tb, 64, true, CORRUPT);
	tb_free(&tb);
}

static void
test_wide_headers(void)
{
	static const uint8_t map16[] = { 0xde, 0x00, 0x02, 0x01, 0x91, 0x01, 0x02, 0xc0 };
	static const uint8_t map32[] = { 0xdf, 0, 0, 0, 1, 0xc0, 0x01 };
	static const uint8_t list16[] = { 0xdc, 0x00, 0x02, 0x81, 0x01, 0x01, 0x01 };
	static const uint8_t list32[] = { 0xdd, 0, 0, 0, 1, 0x90 };
	tbuf tb = { 0 };

	tb_raw(&tb, map16, sizeof(map16));
	expect("map16", &tb, 64, true, 0, 2, sizeof(map16));

	tb.sz = 0;
	tb_raw(&tb, map32, sizeof(map32));
	expect("map32 with nil key", &tb, 64, true, INVALID_KEY, 1, sizeof(map32));

	tb.sz = 0;
	tb_raw(&tb, list16, sizeof(list16));
	expect("list16", &tb, 64, true, 0, 2, sizeof(list16));

	tb.sz = 0;
	tb_raw(&tb, list32, sizeof(list32));
	expect("list32", &tb, 64, true, 0, 2, sizeof(list32));

	tb_free(&tb);
}

static void
test_padding_and_scalars(void)
{
	static const uint8_t padded[] = { 0x91, 0x01, 0xff, 0xff };
	static const uint8_t scalar[] = { 0x01 };
	tbuf tb = { 0 };

	tb_raw(&tb, padded, sizeof(padded));
	expect("padding left alone", &tb, 64, true, 0, 1, 2);

	tb.sz = 0;
	tb_raw(&tb, scalar, sizeof(scalar));
	expect("top-level scalar", &tb, 64, true, 0, 0, 1);

	tb_free(&tb);
}

static void
test_max_depth(void)
{
	tbuf tb = { 0 };

	for (uint32_t i = 0; i < 10; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x01);
	expect("chain 10, max 10", &tb, 10, true, 0, 10, 11);
	expect("chain 10, max 9", &tb, 9, true, OVER, 10, 0);
	expect("chain 10, max 1", &tb, 1, true, OVER, 2, 0);

	tb.sz = 0;

	for (uint32_t i = 0; i < 255; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x01);
	expect("chain 255, max 300 clamps to 255", &tb, 300, true, 0, 255, 256);

	tb.sz = 0;

	for (uint32_t i = 0; i < 256; i++) {
		tb_u8(&tb, 0x91);
	}

	tb_u8(&tb, 0x01);
	expect("chain 256, max 300 clamps to 255", &tb, 300, true, OVER, 256, 0);

	tb_free(&tb);
}

typedef struct deep_args_s {
	const tbuf *tb;
	uint32_t max_depth;
	cdt_walk_result res;
} deep_args;

static void *
deep_thread(void *udata)
{
	deep_args *a = udata;

	cdt_walk(a->tb->buf, a->tb->sz, a->max_depth, true, &a->res);

	return NULL;
}

// Walks very deep input on a thread with a 64 KiB stack. A walker that recursed
// per level would need megabytes and crash.
static void
test_deep_small_stack(void)
{
	static const uint8_t prefixes[][2] = { { 0x91 }, { 0x81, 0x01 }, { 0x92, 0x01 } };
	static const uint32_t prefix_sz[] = { 1, 2, 2 };
	static const char *names[] = { "list", "map", "pair" };

	for (uint32_t s = 0; s < 3; s++) {
		for (uint32_t m = 0; m < 2; m++) {
			tbuf tb = { 0 };

			for (uint32_t i = 0; i < 1000000; i++) {
				tb_raw(&tb, prefixes[s], prefix_sz[s]);
			}

			tb_u8(&tb, 0x01);

			pthread_attr_t attr;
			pthread_t thread;
			deep_args a = { .tb = &tb, .max_depth = m == 0 ? 64 : 255 };

			pthread_attr_init(&attr);
			pthread_attr_setstacksize(&attr, 64 * 1024);

			if (pthread_create(&thread, &attr, deep_thread, &a) != 0) {
				fprintf(stderr, "pthread_create failed\n");
				exit(1);
			}

			pthread_join(thread, NULL);
			pthread_attr_destroy(&attr);

			g_checks++;

			if (a.res.flags != OVER || a.res.depth != a.max_depth + 1) {
				g_failures++;
				fprintf(stderr, "FAIL deep %s chain, max %u: flags 0x%x depth %u\n",
						names[s], a.max_depth, a.res.flags, a.res.depth);
			}

			tb_free(&tb);
		}
	}

	// A wide, flat list: one level, a million elements.
	tbuf tb = { 0 };

	tb_u8(&tb, 0xdd);
	tb_be(&tb, 1000000, 4);

	for (uint32_t i = 0; i < 1000000; i++) {
		tb_u8(&tb, (uint8_t)(i & 0x7f));
	}

	expect("flat 1M list", &tb, 64, true, 0, 1, tb.sz);
	tb_free(&tb);
}


//==========================================================
// Main.
//

int
main(void)
{
	test_boundaries();
	test_empty_and_flags_only();
	test_map_keys();
	test_key_types();
	test_meta_and_ext();
	test_corrupt();
	test_wide_headers();
	test_padding_and_scalars();
	test_max_depth();
	test_deep_small_stack();

	printf("test_cdt_walk: %u checks, %u failures\n", g_checks, g_failures);

	return g_failures == 0 ? 0 : 1;
}
