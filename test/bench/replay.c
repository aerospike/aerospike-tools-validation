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

// Replays stored list/map bins through the validation paths, without a
// server in the timed part.
//
//   replay dump   -h host -n ns -o corpus     scan raw bins into a file
//   replay verify corpus                      cdt_walk vs reference walk, and
//                                             old checks vs path B counters
//   replay time   corpus -p path [-i iters] [-m max_bins]
//                                             ns per bin for one path
//
// Paths: noop, baseline (master: old checks with map-key check), walk (walker
// only), A (walker without keys + old checks with keys), B (walker with keys +
// old checks without keys).

#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>

#include <cdt_check.h>
#include <cdt_walk.h>

#include "ref_walk.h"

//==========================================================
// Typedefs & constants.
//

#define MAX_DEPTH 64

typedef enum {
	PATH_NOOP,
	PATH_BASELINE,
	PATH_WALK,
	PATH_A,
	PATH_B
} path;

static const char *path_names[] = { "noop", "baseline", "walk", "A", "B" };

typedef struct corpus_s {
	uint8_t *data;
	uint32_t n_bins;
	uint32_t *off;
	uint32_t *len;
} corpus;

uint64_t g_sink;


//==========================================================
// Corpus.
//

static bool
dump_cb(const as_val *val, void *udata)
{
	if (val == NULL) {
		return true;
	}

	as_record *rec = as_record_fromval(val);

	for (uint16_t i = 0; rec != NULL && i < rec->bins.size; i++) {
		as_val *v = (as_val *)rec->bins.entries[i].valuep;

		if (v == NULL || v->type != AS_BYTES) {
			continue;
		}

		as_bytes *b = (as_bytes *)v;
		as_bytes_type type = as_bytes_get_type(b);

		if (type != AS_BYTES_LIST && type != AS_BYTES_MAP) {
			continue;
		}

		uint32_t sz = as_bytes_size(b);

		fwrite(&sz, sizeof(sz), 1, udata);
		fwrite(as_bytes_get(b), 1, sz, udata);
	}

	return true;
}

static int
dump(const char *host, const char *ns, const char *out)
{
	FILE *f = fopen(out, "wb");

	if (f == NULL) {
		perror(out);
		return 1;
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

	as_scan scan;
	as_scan_init(&scan, ns, "");
	scan.deserialize_list_map = false;

	int rc = 0;

	if (aerospike_scan_foreach(&as, &err, NULL, &scan, dump_cb, f) !=
			AEROSPIKE_OK) {
		fprintf(stderr, "scan: %d %s\n", err.code, err.message);
		rc = 1;
	}

	as_scan_destroy(&scan);
	aerospike_close(&as, &err);
	aerospike_destroy(&as);
	fclose(f);

	return rc;
}

static bool
load(const char *path_name, corpus *c)
{
	FILE *f = fopen(path_name, "rb");

	if (f == NULL) {
		perror(path_name);
		return false;
	}

	fseek(f, 0, SEEK_END);

	long fsz = ftell(f);

	fseek(f, 0, SEEK_SET);
	c->data = malloc((size_t)fsz);

	if (fread(c->data, 1, (size_t)fsz, f) != (size_t)fsz) {
		fprintf(stderr, "short read\n");
		return false;
	}

	fclose(f);

	uint32_t cap = 1024;

	c->off = malloc(cap * sizeof(uint32_t));
	c->len = malloc(cap * sizeof(uint32_t));
	c->n_bins = 0;

	for (long pos = 0; pos < fsz;) {
		uint32_t sz;

		memcpy(&sz, c->data + pos, sizeof(sz));
		pos += (long)sizeof(sz);

		if (c->n_bins == cap) {
			cap *= 2;
			c->off = realloc(c->off, cap * sizeof(uint32_t));
			c->len = realloc(c->len, cap * sizeof(uint32_t));
		}

		c->off[c->n_bins] = (uint32_t)pos;
		c->len[c->n_bins] = sz;
		c->n_bins++;
		pos += sz;
	}

	return true;
}


//==========================================================
// Paths.
//

static void
reset_config(backup_config *bc, bool check_map_keys)
{
	memset(bc, 0, sizeof(*bc));
	bc->check_map_keys = check_map_keys;
}

__attribute__((noinline)) void
run_path(path p, const corpus *c, backup_config *bc, uint32_t *walk_flags)
{
	for (uint32_t i = 0; i < c->n_bins; i++) {
		const uint8_t *buf = c->data + c->off[i];
		uint32_t sz = c->len[i];
		cdt_fix cf = { NULL };
		cdt_walk_result r;

		switch (p) {
		case PATH_NOOP:
			g_sink += buf[0];
			break;
		case PATH_BASELINE:
			g_sink += cdt_need_fix(buf, sz, &cf, bc);
			break;
		case PATH_WALK:
			cdt_walk(buf, sz, MAX_DEPTH, true, &r);
			g_sink += r.flags;
			break;
		case PATH_A:
			cdt_walk(buf, sz, MAX_DEPTH, false, &r);
			g_sink += r.flags + cdt_need_fix(buf, sz, &cf, bc);
			break;
		case PATH_B:
			cdt_walk(buf, sz, MAX_DEPTH, true, &r);
			g_sink += r.flags + cdt_need_fix(buf, sz, &cf, bc);
			break;
		}

		if (walk_flags != NULL && p != PATH_NOOP && p != PATH_BASELINE) {
			walk_flags[i] = r.flags;
		}
	}
}

static uint64_t
now_ns(void)
{
	struct timespec ts;

	clock_gettime(CLOCK_MONOTONIC, &ts);

	return (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;
}

static int
time_path(const corpus *c, path p, uint32_t iters)
{
	backup_config bc;

	reset_config(&bc, p == PATH_BASELINE || p == PATH_A);

	uint64_t start = now_ns();

	for (uint32_t it = 0; it < iters; it++) {
		run_path(p, c, &bc, NULL);
	}

	uint64_t ns = now_ns() - start;

	printf("path %-8s bins %u iters %u ns/bin %.1f (sink %" PRIu64 ")\n",
			path_names[p], c->n_bins, iters,
			(double)ns / ((double)iters * c->n_bins), g_sink);

	return 0;
}


//==========================================================
// Verify.
//

static void
print_stats(const char *name, const cdt_stats *a, const cdt_stats *b)
{
	printf("  %-6s %10s %10s\n", name, "baseline", "B");
#define ROW(f) printf("  %-16s %10u %10u%s\n", #f, a->f, b->f, \
		a->f == b->f ? "" : "   <-- differs")
	ROW(count);
	ROW(need_fix);
	ROW(nf_order);
	ROW(nf_padding);
	ROW(cannot_fix);
	ROW(cf_dupkey);
	ROW(cf_nonstorage);
	ROW(cf_corrupt);
	ROW(cf_invalidkey);
#undef ROW
}

static int
verify(const corpus *c)
{
	uint32_t mismatches = 0;
	uint32_t over = 0;
	uint32_t strict_only = 0;
	uint32_t invalid_key = 0;
	uint32_t corrupt = 0;

	for (uint32_t i = 0; i < c->n_bins; i++) {
		const uint8_t *buf = c->data + c->off[i];
		uint32_t sz = c->len[i];
		walk_result ref;
		cdt_walk_result w;

		reference_walk(buf, sz, &ref);
		cdt_walk(buf, sz, MAX_DEPTH, true, &w);

		ref_class rc = ref_classify(&ref, MAX_DEPTH);
		bool w_over = (w.flags & CDT_WALK_OVER) != 0;
		bool w_strict = ! w_over && (w.flags & CDT_WALK_EMPTY_OVER) != 0;
		bool w_corrupt = (w.flags & CDT_WALK_CORRUPT) != 0;
		bool ok = (rc == REF_OVER) == w_over &&
				(rc == REF_STRICT_ONLY) == w_strict &&
				ref.corrupt == w_corrupt;

		if (ok && ! w_over && ! w_corrupt) {
			ok = ref.invalid_key == ((w.flags & CDT_WALK_INVALID_KEY) != 0) &&
					ref.strict_depth == w.depth && ref.padding == (w.sz < sz);
		}

		over += w_over;
		strict_only += w_strict;
		invalid_key += (w.flags & CDT_WALK_INVALID_KEY) != 0;
		corrupt += w_corrupt;

		if (! ok && mismatches++ < 5) {
			fprintf(stderr, "MISMATCH bin %u: ref class %s depth %u/%u key %d "
					"corrupt %d padding %d; walk flags 0x%x depth %u sz %u/%u\n", i,
					ref_class_names[rc], ref.server_depth, ref.strict_depth,
					ref.invalid_key, ref.corrupt, ref.padding, w.flags, w.depth,
					w.sz, sz);
		}
	}

	printf("walker vs reference: %u bins, %u over, %u strict_only, %u invalid_key, "
			"%u corrupt, %u mismatches\n", c->n_bins, over, strict_only,
			invalid_key, corrupt, mismatches);

	backup_config base;
	backup_config b;

	reset_config(&base, true);
	reset_config(&b, false);
	run_path(PATH_BASELINE, c, &base, NULL);
	run_path(PATH_B, c, &b, NULL);

	printf("old checks, baseline (map keys on) vs path B (map keys off):\n");
	print_stats("lists", &base.cdt_list, &b.cdt_list);
	print_stats("maps", &base.cdt_map, &b.cdt_map);

	return mismatches == 0 ? 0 : 1;
}


//==========================================================
// Main.
//

static void
usage(void)
{
	fprintf(stderr,
			"usage: replay dump -h host -n ns -o corpus\n"
			"       replay verify corpus\n"
			"       replay time corpus -p noop|baseline|walk|A|B [-i iters] "
			"[-m max_bins]\n");
	exit(2);
}

int
main(int argc, char **argv)
{
	if (argc < 3) {
		usage();
	}

	const char *mode = argv[1];

	if (strcmp(mode, "dump") == 0) {
		const char *host = "127.0.0.1";
		const char *ns = NULL;
		const char *out = NULL;
		int opt;

		optind = 2;

		while ((opt = getopt(argc, argv, "h:n:o:")) != -1) {
			switch (opt) {
			case 'h':
				host = optarg;
				break;
			case 'n':
				ns = optarg;
				break;
			case 'o':
				out = optarg;
				break;
			default:
				usage();
			}
		}

		if (ns == NULL || out == NULL) {
			usage();
		}

		return dump(host, ns, out);
	}

	corpus c;

	if (! load(argv[2], &c)) {
		return 1;
	}

	if (strcmp(mode, "verify") == 0) {
		return verify(&c);
	}

	if (strcmp(mode, "time") != 0) {
		usage();
	}

	path p = PATH_NOOP;
	uint32_t iters = 1;
	int opt;

	optind = 3;

	while ((opt = getopt(argc, argv, "p:i:m:")) != -1) {
		switch (opt) {
		case 'p': {
			bool found = false;

			for (uint32_t i = 0; i < 5; i++) {
				if (strcmp(optarg, path_names[i]) == 0) {
					p = (path)i;
					found = true;
				}
			}

			if (! found) {
				usage();
			}
			break;
		}
		case 'i':
			iters = (uint32_t)strtoul(optarg, NULL, 10);
			break;
		case 'm': {
			uint32_t max_bins = (uint32_t)strtoul(optarg, NULL, 10);

			if (max_bins < c.n_bins) {
				c.n_bins = max_bins;
			}
			break;
		}
		default:
			usage();
		}
	}

	return time_path(&c, p, iters);
}
