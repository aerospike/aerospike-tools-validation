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

	// After a nested value, the map's later keys are still checked.
	static const uint8_t key_after_child[] = {
		0x82, 0x01, 0x91, 0x01, 0xc0, 0x02
	};

	tb.sz = 0;
	tb_raw(&tb, key_after_child, sizeof(key_after_child));
	expect("invalid key after a nested value", &tb, 64, true, INVALID_KEY, 2,
			sizeof(key_after_child));

	// Empty string/blob keys whose next byte looks like a type byte.
	static const uint8_t empty_str_key[] = { 0x81, 0xa0, 0x03 };
	static const uint8_t empty_bin_key[] = { 0x81, 0xc4, 0x00, 0x04 };

	tb.sz = 0;
	tb_raw(&tb, empty_str_key, sizeof(empty_str_key));
	expect("empty fixstr key before 0x03", &tb, 64, true, INVALID_KEY, 1,
			sizeof(empty_str_key));

	tb.sz = 0;
	tb_raw(&tb, empty_bin_key, sizeof(empty_bin_key));
	expect("empty bin8 key before 0x04", &tb, 64, true, INVALID_KEY, 1,
			sizeof(empty_bin_key));

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
		{ "fixstr type byte only", { 0xa1, 0x03 }, 2, true },
		{ "bin8 type byte only", { 0xc4, 0x01, 0x04 }, 3, true },
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
	static const uint8_t wildcard_key[] = { 0x81, 0xc7, 0x01, 0xff, 0x00, 0x01 };
	static const uint8_t inf_key[] = { 0x81, 0xc7, 0x01, 0xff, 0x01, 0x01 };
	static const uint8_t ff_ext_meta[] = { 0x81, 0xc7, 0x01, 0xff, 0x02, 0xc0 };
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

	tb.sz = 0;
	tb_raw(&tb, wildcard_key, sizeof(wildcard_key));
	expect("wildcard first key is not meta", &tb, 64, true, INVALID_KEY, 1,
			sizeof(wildcard_key));

	tb.sz = 0;
	tb_raw(&tb, inf_key, sizeof(inf_key));
	expect("infinity first key is not meta", &tb, 64, true, INVALID_KEY, 1,
			sizeof(inf_key));

	tb.sz = 0;
	tb_raw(&tb, ff_ext_meta, sizeof(ff_ext_meta));
	expect("other type-0xff ext8 first key is meta", &tb, 64, true, 0, 1,
			sizeof(ff_ext_meta));

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

// Every scalar header form at its exact size inside a list, and the same cut
// short by one byte.
static void
test_all_scalar_headers(void)
{
	static const struct {
		const char *name;
		uint8_t hdr[6];
		uint32_t hdr_sz;
		uint32_t payload_sz;
	} cases[] = {
		{ "positive fixint", { 0x7f }, 1, 0 },
		{ "negative fixint", { 0xe0 }, 1, 0 },
		{ "nil", { 0xc0 }, 1, 0 },
		{ "false", { 0xc2 }, 1, 0 },
		{ "true", { 0xc3 }, 1, 0 },
		{ "fixstr 0", { 0xa0 }, 1, 0 },
		{ "fixstr 17", { 0xb1 }, 1, 17 },
		{ "fixstr 31", { 0xbf }, 1, 31 },
		{ "str8", { 0xd9, 40 }, 2, 40 },
		{ "str16", { 0xda, 0x01, 0x00 }, 3, 256 },
		{ "str32", { 0xdb, 0, 0, 0x01, 0x00 }, 5, 256 },
		{ "bin8", { 0xc4, 3 }, 2, 3 },
		{ "bin16", { 0xc5, 0x01, 0x01 }, 3, 257 },
		{ "bin32", { 0xc6, 0, 0, 0, 2 }, 5, 2 },
		{ "float", { 0xca }, 1, 4 },
		{ "double", { 0xcb }, 1, 8 },
		{ "uint8", { 0xcc }, 1, 1 },
		{ "uint16", { 0xcd }, 1, 2 },
		{ "uint32", { 0xce }, 1, 4 },
		{ "uint64", { 0xcf }, 1, 8 },
		{ "int8", { 0xd0 }, 1, 1 },
		{ "int16", { 0xd1 }, 1, 2 },
		{ "int32", { 0xd2 }, 1, 4 },
		{ "int64", { 0xd3 }, 1, 8 },
		{ "fixext1", { 0xd4, 0x07 }, 2, 1 },
		{ "fixext2", { 0xd5, 0x07 }, 2, 2 },
		{ "fixext4", { 0xd6, 0x07 }, 2, 4 },
		{ "fixext8", { 0xd7, 0x07 }, 2, 8 },
		{ "fixext16", { 0xd8, 0x07 }, 2, 16 },
		{ "ext8", { 0xc7, 3, 0x07 }, 3, 3 },
		{ "ext16", { 0xc8, 0x01, 0x00, 0x07 }, 4, 256 },
		{ "ext32", { 0xc9, 0, 0, 0x01, 0x01, 0x07 }, 6, 257 }
	};

	for (uint32_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
		tbuf tb = { 0 };

		tb_u8(&tb, 0x91);
		tb_raw(&tb, cases[i].hdr, cases[i].hdr_sz);

		for (uint32_t j = 0; j < cases[i].payload_sz; j++) {
			tb_u8(&tb, 0x91);
		}

		expect(fmt("%s exact", cases[i].name), &tb, 64, true, 0, 1, tb.sz);

		tb.sz--;

		if (tb.sz > 1) {
			expect_flags(fmt("%s short", cases[i].name), &tb, 64, true, CORRUPT);
		}

		tb_free(&tb);
	}
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

// One level of a nested chain: the bytes before the child, which is always the
// last element of the level.
typedef struct nest_kind_s {
	const char *name;
	uint8_t prefix[16];
	uint32_t prefix_sz;
} nest_kind;

static const nest_kind nest_kinds[] = {
	{ "list", { 0x91 }, 1 },
	{ "ordered list", { 0x92, 0xc7, 0x00, 0x01 }, 4 },
	{ "list16", { 0xdc, 0x00, 0x01 }, 3 },
	{ "list32", { 0xdd, 0x00, 0x00, 0x00, 0x01 }, 5 },
	{ "map", { 0x81, 0x01 }, 2 },
	{ "k-ordered map", { 0x82, 0xc7, 0x00, 0x01, 0xc0, 0x01 }, 6 },
	{ "kv-ordered map", { 0x82, 0xc7, 0x00, 0x03, 0xc0, 0xa2, 0x03, 'a' }, 8 },
	{ "map16", { 0xde, 0x00, 0x01, 0xd0, 0x80 }, 5 },
	{ "map32", { 0xdf, 0x00, 0x00, 0x00, 0x01, 0xcd, 0x01, 0x00 }, 8 },
	{ "list with geojson and double", { 0x93, 0xa3, 0x17, '{', '}', 0xcb, 0, 0, 0, 0, 0, 0, 0, 0 }, 14 },
	{ "map with blob key", { 0x82, 0xc4, 0x02, 0x04, 0xff, 0xc0, 0xa2, 0x03, 'b' }, 9 },
	{ "list with ext siblings", { 0x93, 0xd6, 0x05, 0, 0, 0, 0, 0xc8, 0x00, 0x01, 0x07, 0xaa }, 12 },
	{ "list with container-like string", { 0x92, 0xa5, 0x03, 0x91, 0x91, 0x91, 0x91 }, 7 },
	{ "list with wildcard ext", { 0x92, 0xd4, 0xff, 0x00 }, 4 }
};

#define N_NEST_KINDS (sizeof(nest_kinds) / sizeof(nest_kinds[0]))

// Builds 'depth' levels; level i uses kinds[i % n_kinds], the deepest level
// holds the leaf.
static void
nest(tbuf *tb, uint32_t depth, const uint32_t *kinds, uint32_t n_kinds,
		const uint8_t *leaf, uint32_t leaf_sz)
{
	for (uint32_t i = 0; i < depth; i++) {
		const nest_kind *k = &nest_kinds[kinds[i % n_kinds]];

		tb_raw(tb, k->prefix, k->prefix_sz);
	}

	tb_raw(tb, leaf, leaf_sz);
}

static void
test_complex_nesting(void)
{
	static const uint8_t leaf_int[] = { 0x01 };
	static const uint8_t leaf_geo[] = { 0xa3, 0x17, '{', '}' };
	static const uint8_t leaf_flags_list[] = { 0x91, 0xc7, 0x00, 0x01 };
	static const uint8_t leaf_flags_map[] = { 0x81, 0xc7, 0x00, 0x03, 0xc0 };
	static const uint8_t leaf_empty[] = { 0x90 };
	uint32_t all[N_NEST_KINDS];

	for (uint32_t i = 0; i < N_NEST_KINDS; i++) {
		all[i] = i;
	}

	tbuf tb = { 0 };

	// Each kind on its own, then all kinds mixed, at 63, 64 and 65 levels.
	for (uint32_t k = 0; k <= N_NEST_KINDS; k++) {
		const uint32_t *kinds = k < N_NEST_KINDS ? &all[k] : all;
		uint32_t n_kinds = k < N_NEST_KINDS ? 1 : N_NEST_KINDS;
		const char *name = k < N_NEST_KINDS ? nest_kinds[k].name : "mixed";

		for (uint32_t d = 63; d <= 65; d++) {
			tb.sz = 0;
			nest(&tb, d, kinds, n_kinds, leaf_int, sizeof(leaf_int));

			if (d <= 64) {
				expect(fmt("%s x%u, int leaf", name, d), &tb, 64, true, 0, d,
						tb.sz);
			}
			else {
				expect(fmt("%s x%u, int leaf", name, d), &tb, 64, true, OVER, 65,
						0);
			}

			tb.sz = 0;
			nest(&tb, d, kinds, n_kinds, leaf_geo, sizeof(leaf_geo));

			if (d <= 64) {
				expect(fmt("%s x%u, geojson leaf", name, d), &tb, 64, true, 0, d,
						tb.sz);
			}
			else {
				expect(fmt("%s x%u, geojson leaf", name, d), &tb, 64, true, OVER,
						65, 0);
			}
		}

		// 64 levels, then a flags-only list or map, or an empty list, at 65.
		tb.sz = 0;
		nest(&tb, 64, kinds, n_kinds, leaf_flags_list, sizeof(leaf_flags_list));
		expect(fmt("%s x64 + flags-only list", name), &tb, 64, true, OVER, 65, 0);

		tb.sz = 0;
		nest(&tb, 64, kinds, n_kinds, leaf_flags_map, sizeof(leaf_flags_map));
		expect(fmt("%s x64 + flags-only map", name), &tb, 64, true, OVER, 65, 0);

		tb.sz = 0;
		nest(&tb, 64, kinds, n_kinds, leaf_empty, sizeof(leaf_empty));
		expect(fmt("%s x64 + empty list", name), &tb, 64, true, EMPTY_OVER, 65,
				tb.sz);

		tb.sz = 0;
		nest(&tb, 63, kinds, n_kinds, leaf_flags_list, sizeof(leaf_flags_list));
		expect(fmt("%s x63 + flags-only list", name), &tb, 64, true, 0, 64,
				tb.sz);
	}

	// Ordered list at 64 whose siblings are empty containers at 65.
	static const uint8_t empty_siblings[] = {
		0x94, 0xc7, 0x00, 0x01, 0x80, 0x90, 0x01
	};

	tb.sz = 0;
	chain_into(&tb, 64, empty_siblings, sizeof(empty_siblings));
	expect("ordered list at 64 with empty siblings", &tb, 64, true, EMPTY_OVER,
			65, tb.sz);

	// Top-level ordered list carrying a persist index in its flags ext.
	static const uint8_t persist[] = {
		0x93, 0xc7, 0x03, 0x11, 0x91, 0x91, 0x91, 0x01, 0x91, 0x02
	};

	tb.sz = 0;
	tb_raw(&tb, persist, sizeof(persist));
	expect("persist index ext is opaque", &tb, 64, true, 0, 2, sizeof(persist));

	// Key-ordered map of ordered lists of geojson.
	static const uint8_t map_of_lists[] = {
		0x83, 0xc7, 0x00, 0x01, 0xc0,
		0x01, 0x93, 0xc7, 0x00, 0x01, 0xa3, 0x17, '{', '}', 0xa3, 0x17, '[', ']',
		0x02, 0x91, 0x80
	};

	tb.sz = 0;
	tb_raw(&tb, map_of_lists, sizeof(map_of_lists));
	expect("k-ordered map of ordered lists", &tb, 64, true, 0, 3,
			sizeof(map_of_lists));

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
	test_all_scalar_headers();
	test_padding_and_scalars();
	test_max_depth();
	test_complex_nesting();
	test_deep_small_stack();

	printf("test_cdt_walk: %u checks, %u failures\n", g_checks, g_failures);

	return g_failures == 0 ? 0 : 1;
}
