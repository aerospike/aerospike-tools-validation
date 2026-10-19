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

// Tests of the order, duplicate-key and padding checks in cdt_check.c. Each
// sample is copied into a heap buffer of exactly its size, as the client stores
// a bin, so AddressSanitizer stops on any read outside the bin.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cdt_check.h>

//==========================================================
// Typedefs & constants.
//

typedef struct expected_s {
	bool need_fix;
	uint64_t nf_order;
	uint64_t cf_dupkey;
} expected;

static uint32_t g_checks;
static uint32_t g_failures;


//==========================================================
// Helpers.
//

static void
check(const char *name, const uint8_t *bytes, uint32_t sz, expected want)
{
	uint8_t *buf = malloc(sz);

	memcpy(buf, bytes, sz);

	backup_config bc;
	cdt_fix cf = { NULL };

	memset(&bc, 0, sizeof(bc));

	bool need_fix = cdt_need_fix(buf, sz, &cf, &bc);
	uint64_t nf_order = bc.cdt_list.nf_order + bc.cdt_map.nf_order;
	uint64_t cf_dupkey = bc.cdt_map.cf_dupkey;

	g_checks++;

	if (need_fix != want.need_fix || nf_order != want.nf_order ||
			cf_dupkey != want.cf_dupkey) {
		g_failures++;
		fprintf(stderr, "FAIL %s: need_fix %d order %lu dupkey %lu, want %d %lu %lu\n",
				name, need_fix, (unsigned long)nf_order,
				(unsigned long)cf_dupkey, want.need_fix,
				(unsigned long)want.nf_order, (unsigned long)want.cf_dupkey);
	}

	free(buf);
}


//==========================================================
// Tests.
//

// 16- and 32-bit integers right after the header: comparing them must only
// read the integer's own bytes.
static void
test_integers_near_start(void)
{
	static const uint8_t ordered_list[] = {
		0x93, 0xc7, 0x00, 0x01, 0xcd, 0x01, 0x2c, 0xcd, 0x01, 0x2b
	};
	static const uint8_t ordered_list_sorted[] = {
		0x93, 0xc7, 0x00, 0x01, 0xcd, 0x01, 0x2b, 0xcd, 0x01, 0x2c
	};
	static const uint8_t map_dup_u16[] = {
		0x82, 0xcd, 0x01, 0x00, 0x01, 0xcd, 0x01, 0x00, 0x02
	};
	static const uint8_t map_u16[] = {
		0x82, 0xcd, 0x01, 0x00, 0x01, 0xcd, 0x01, 0x01, 0x02
	};
	static const uint8_t map_dup_neg32[] = {
		0x82, 0xd2, 0xff, 0xff, 0xff, 0x00, 0x01, 0xd2, 0xff, 0xff, 0xff, 0x00,
		0x02
	};

	// With 8 or more bytes after the integer, the read loads forward.
	static const uint8_t ordered_list_long[] = {
		0x9d, 0xc7, 0x00, 0x01, 0xce, 0x00, 0x01, 0x00, 0x00, 0xcd, 0x01, 0x2c,
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a
	};
	static const uint8_t map_dup_neg32_long[] = {
		0x82, 0xd2, 0xff, 0xff, 0xff, 0x00, 0xa9, 0x03, 'a', 'b', 'c', 'd', 'e',
		'f', 'g', 'h', 0xd2, 0xff, 0xff, 0xff, 0x00, 0x02
	};

	check("ordered list [65536, 300, 1..10]", ordered_list_long,
			sizeof(ordered_list_long),
			(expected){ .need_fix = true, .nf_order = 1 });
	check("map {-256: \"abcdefgh\", -256: 2}", map_dup_neg32_long,
			sizeof(map_dup_neg32_long), (expected){ .cf_dupkey = 1 });
	check("ordered list [300, 299]", ordered_list, sizeof(ordered_list),
			(expected){ .need_fix = true, .nf_order = 1 });
	check("ordered list [299, 300]", ordered_list_sorted,
			sizeof(ordered_list_sorted), (expected){ 0 });
	check("map {256: 1, 256: 2}", map_dup_u16, sizeof(map_dup_u16),
			(expected){ .cf_dupkey = 1 });
	check("map {256: 1, 257: 2}", map_u16, sizeof(map_u16), (expected){ 0 });
	check("map {-256: 1, -256: 2}", map_dup_neg32, sizeof(map_dup_neg32),
			(expected){ .cf_dupkey = 1 });
}


//==========================================================
// Main.
//

int
main(void)
{
	test_integers_near_start();

	printf("test_cdt_check: %u checks, %u failures\n", g_checks, g_failures);

	return g_failures == 0 ? 0 : 1;
}
