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
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_udf.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_string.h>

#include "ref_walk.h"

//==========================================================
// Typedefs & constants.
//

#define MAX_RANGES 8
#define MAX_ROWS 16
#define MAX_DEPTH 64

typedef struct key_range_s {
	int64_t start;
	int64_t count;
} key_range;

typedef struct outcome_s {
	char label[96];
	uint64_t count[N_REF_CLASSES];
} outcome;

typedef struct tally_s {
	const char *title;
	outcome rows[MAX_ROWS];
	uint32_t n_rows;
} tally;

typedef struct scan_count_s {
	uint64_t records;
} scan_count;

typedef struct udf_ctx_s {
	const char *bin;
	tally t;
} udf_ctx;

extern char *aerospike_client_version;

//==========================================================
// Tally.
//

static void
tally_add(tally *t, ref_class c, const char *label)
{
	for (uint32_t i = 0; i < t->n_rows; i++) {
		if (strcmp(t->rows[i].label, label) == 0) {
			t->rows[i].count[c]++;
			return;
		}
	}

	if (t->n_rows == MAX_ROWS) {
		return;
	}

	outcome *o = &t->rows[t->n_rows++];

	snprintf(o->label, sizeof(o->label), "%s", label);
	o->count[c] = 1;
}

static void
tally_add_status(tally *t, ref_class c, as_status st)
{
	char label[96];

	if (st == AEROSPIKE_OK) {
		snprintf(label, sizeof(label), "ok");
	}
	else {
		snprintf(label, sizeof(label), "error %d %s", st, as_error_string(st));
	}

	tally_add(t, c, label);
}

static void
tally_print(const tally *t)
{
	printf("\n%s\n", t->title);
	printf("  %-44s %10s %12s %10s\n", "outcome", "ok", "strict_only", "over");

	for (uint32_t i = 0; i < t->n_rows; i++) {
		const outcome *o = &t->rows[i];

		printf("  %-44s %10" PRIu64 " %12" PRIu64 " %10" PRIu64 "\n", o->label,
				o->count[REF_OK], o->count[REF_STRICT_ONLY], o->count[REF_OVER]);
	}
}

//==========================================================
// Helpers.
//

static as_status
get_raw(aerospike *as, as_error *err, as_key *key, as_record **rec)
{
	as_policy_read rp;
	as_policy_read_init(&rp);
	rp.deserialize = false;

	return aerospike_key_get(as, err, &rp, key, rec);
}

static ref_class
record_class(const as_record *rec)
{
	ref_class worst = REF_OK;

	for (uint16_t i = 0; i < rec->bins.size; i++) {
		as_val *v = (as_val *)rec->bins.entries[i].valuep;

		if (v == NULL || v->type != AS_BYTES) {
			continue;
		}

		as_bytes *b = (as_bytes *)v;
		as_bytes_type type = as_bytes_get_type(b);

		if (type != AS_BYTES_LIST && type != AS_BYTES_MAP) {
			continue;
		}

		walk_result wr;

		reference_walk(as_bytes_get(b), as_bytes_size(b), &wr);

		ref_class c = ref_classify(&wr, MAX_DEPTH);

		if (c > worst) {
			worst = c;
		}
	}

	return worst;
}

static int64_t
bin_size(const as_record *rec, const char *name)
{
	for (uint16_t i = 0; i < rec->bins.size; i++) {
		const as_bin *bin = &rec->bins.entries[i];

		if (strcmp(bin->name, name) != 0) {
			continue;
		}

		as_val *v = (as_val *)bin->valuep;

		if (v != NULL && v->type == AS_BYTES) {
			return as_bytes_size((as_bytes *)v);
		}

		return 0;
	}

	return -1;
}

//==========================================================
// Modes.
//

typedef void (*key_fn)(aerospike *as, as_key *key, as_record *raw,
		ref_class c, void *udata);

static void
for_each_key(aerospike *as, const char *ns, const key_range *ranges,
		uint32_t n_ranges, key_fn fn, void *udata)
{
	uint64_t missing = 0;

	for (uint32_t r = 0; r < n_ranges; r++) {
		for (int64_t k = ranges[r].start;
				k < ranges[r].start + ranges[r].count; k++) {
			as_key key;
			as_key_init_int64(&key, ns, "", k);

			as_error err;
			as_record *raw = NULL;

			if (get_raw(as, &err, &key, &raw) != AEROSPIKE_OK) {
				if (missing++ == 0) {
					printf("  first raw read error: key %" PRId64 ": %d %s\n", k,
							err.code, err.message);
				}

				continue;
			}

			fn(as, &key, raw, record_class(raw), udata);
			as_record_destroy(raw);
		}
	}

	if (missing != 0) {
		printf("  (%" PRIu64 " key(s) could not be read raw)\n", missing);
	}
}

static void
read_one(aerospike *as, as_key *key, as_record *raw, ref_class c, void *udata)
{
	tally *t = udata;
	as_policy_read rp;
	as_policy_read_init(&rp);
	rp.deserialize = true;

	as_error err;
	as_record *rec = NULL;
	as_status st = aerospike_key_get(as, &err, &rp, key, &rec);

	(void)raw;
	tally_add(&t[0], c, "ok");
	tally_add_status(&t[1], c, st);

	// On a deserialize error the client frees the record but leaves rec
	// pointing at it, so only destroy on success.
	if (st == AEROSPIKE_OK) {
		as_record_destroy(rec);
	}
}

static void
rewrite_one(aerospike *as, as_key *key, as_record *raw, ref_class c,
		void *udata)
{
	as_policy_write wp;
	as_policy_write_init(&wp);
	wp.base.max_retries = 0;

	as_error err;

	tally_add_status(udata, c, aerospike_key_put(as, &err, &wp, key, raw));
}

static void
udf_one(aerospike *as, as_key *key, as_record *raw, ref_class c, void *udata)
{
	udf_ctx *ctx = udata;
	as_arraylist args;
	as_arraylist_init(&args, 1, 0);
	as_arraylist_append_str(&args, ctx->bin);

	as_error err;
	as_val *res = NULL;
	as_status st = aerospike_key_apply(as, &err, NULL, key, "asval",
			"bin_type", (as_list *)&args, &res);

	char label[96];

	if (st != AEROSPIKE_OK) {
		snprintf(label, sizeof(label), "error %d %s", st, as_error_string(st));
	}
	else if (res != NULL && as_val_type(res) == AS_STRING) {
		snprintf(label, sizeof(label), "UDF saw %s",
				as_string_get((as_string *)res));
	}
	else {
		snprintf(label, sizeof(label), "UDF returned no string");
	}

	(void)raw;
	tally_add(&ctx->t, c, label);
	as_val_destroy(res);
	as_arraylist_destroy(&args);
}

static void
udf_write_one(aerospike *as, as_key *key, as_record *raw, ref_class c,
		void *udata)
{
	const char *bin = udata;
	int64_t before = bin_size(raw, bin);
	as_arraylist args;
	as_arraylist_init(&args, 1, 0);
	as_arraylist_append_str(&args, bin);

	as_error err;
	as_val *res = NULL;
	as_status st = aerospike_key_apply(as, &err, NULL, key, "asval",
			"write_back", (as_list *)&args, &res);

	as_val_destroy(res);
	as_arraylist_destroy(&args);

	as_record *after = NULL;
	int64_t after_sz = -1;

	if (get_raw(as, &err, key, &after) == AEROSPIKE_OK) {
		after_sz = bin_size(after, bin);
		as_record_destroy(after);
	}

	printf("  key %-8" PRId64 " %-12s apply %-3d bin '%s' %" PRId64 " bytes -> %s",
			key->value.integer.value, ref_class_names[c], st, bin, before,
			after_sz < 0 ? "MISSING" : "");

	if (after_sz >= 0) {
		printf("%" PRId64 " bytes", after_sz);
	}

	printf("\n");
}

static bool
scan_cb(const as_val *val, void *udata)
{
	if (val != NULL) {
		((scan_count *)udata)->records++;
	}

	return true;
}

static void
scan_both(aerospike *as, const char *ns)
{
	for (int deser = 0; deser < 2; deser++) {
		as_scan scan;
		as_scan_init(&scan, ns, "");
		scan.deserialize_list_map = deser == 1;

		scan_count sc = { 0 };
		as_error err;
		as_status st = aerospike_scan_foreach(as, &err, NULL, &scan, scan_cb,
				&sc);

		printf("scan deserialize_list_map=%s: %" PRIu64 " record(s), status %d%s%s\n",
				deser ? "true " : "false", sc.records, st,
				st == AEROSPIKE_OK ? "" : " - ",
				st == AEROSPIKE_OK ? "" : err.message);
		as_scan_destroy(&scan);
	}
}

static bool
register_udf(aerospike *as, const char *path)
{
	FILE *f = fopen(path, "rb");

	if (f == NULL) {
		perror(path);
		return false;
	}

	uint8_t buf[8192];
	size_t sz = fread(buf, 1, sizeof(buf), f);

	fclose(f);

	as_bytes content;
	as_bytes_init_wrap(&content, buf, (uint32_t)sz, false);

	as_error err;

	if (aerospike_udf_put(as, &err, NULL, "asval.lua", AS_UDF_TYPE_LUA,
			&content) != AEROSPIKE_OK ||
			aerospike_udf_put_wait(as, &err, NULL, "asval.lua", 100) !=
					AEROSPIKE_OK) {
		fprintf(stderr, "udf register: %d %s\n", err.code, err.message);
		return false;
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
			"usage: probe read|scan|rewrite|udf|udf-write -h host -n ns\n"
			"             [--range start:count ...] [--bin name] [--udf-file path]\n"
			"             [--max-error-rate n]\n");
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
	const char *bin = "deep";
	const char *udf_file = "test/upgrade/asval.lua";
	key_range ranges[MAX_RANGES];
	uint32_t n_ranges = 0;
	int64_t max_error_rate = -1;

	static struct option opts[] = {
		{ "host", required_argument, NULL, 'h' },
		{ "namespace", required_argument, NULL, 'n' },
		{ "range", required_argument, NULL, 'r' },
		{ "bin", required_argument, NULL, 'b' },
		{ "udf-file", required_argument, NULL, 'u' },
		{ "max-error-rate", required_argument, NULL, 'e' },
		{ NULL, 0, NULL, 0 }
	};

	optind = 2;

	int opt;

	while ((opt = getopt_long(argc, argv, "h:n:r:b:u:e:", opts, NULL)) != -1) {
		switch (opt) {
		case 'h':
			host = optarg;
			break;
		case 'n':
			ns = optarg;
			break;
		case 'r':
			if (n_ranges == MAX_RANGES ||
					sscanf(optarg, "%" SCNd64 ":%" SCNd64,
							&ranges[n_ranges].start,
							&ranges[n_ranges].count) != 2) {
				usage();
			}
			n_ranges++;
			break;
		case 'b':
			bin = optarg;
			break;
		case 'u':
			udf_file = optarg;
			break;
		case 'e':
			max_error_rate = strtoll(optarg, NULL, 10);
			break;
		default:
			usage();
		}
	}

	if (ns == NULL) {
		usage();
	}

	as_config cfg;
	as_config_init(&cfg);
	as_config_add_host(&cfg, host, 3000);

	// The client resets any max_error_rate / error_rate_window ratio outside
	// 1..100 to 100 / 1, including max_error_rate 0.
	if (max_error_rate > 0) {
		cfg.max_error_rate = (uint32_t)max_error_rate;
		cfg.error_rate_window = (cfg.max_error_rate + 99) / 100;
	}

	printf("client %s, max_error_rate %u per %u tend(s)\n",
			aerospike_client_version, cfg.max_error_rate, cfg.error_rate_window);

	aerospike as;
	as_error err;

	aerospike_init(&as, &cfg);

	if (aerospike_connect(&as, &err) != AEROSPIKE_OK) {
		fprintf(stderr, "connect: %d %s\n", err.code, err.message);
		return 1;
	}

	int rc = 0;

	if (strcmp(mode, "scan") == 0) {
		scan_both(&as, ns);
	}
	else if (strcmp(mode, "read") == 0) {
		char title[96];

		snprintf(title, sizeof(title), "read with deserialize on (client %s)",
				aerospike_client_version);

		tally t[2] = {
			{ .title = "read with deserialize off (raw bytes)" },
			{ .title = title }
		};

		for_each_key(&as, ns, ranges, n_ranges, read_one, t);
		tally_print(&t[0]);
		tally_print(&t[1]);
	}
	else if (strcmp(mode, "rewrite") == 0) {
		tally t = { .title = "whole-record rewrite of the stored bytes" };

		for_each_key(&as, ns, ranges, n_ranges, rewrite_one, &t);
		tally_print(&t);
	}
	else if (strcmp(mode, "udf") == 0 || strcmp(mode, "udf-write") == 0) {
		if (! register_udf(&as, udf_file)) {
			rc = 1;
		}
		else if (strcmp(mode, "udf") == 0) {
			udf_ctx ctx = { .bin = bin, .t = { .title = "UDF reads the bin" } };

			for_each_key(&as, ns, ranges, n_ranges, udf_one, &ctx);
			tally_print(&ctx.t);
		}
		else {
			printf("\nUDF writes the bin back (rec[bin] = rec[bin])\n");
			for_each_key(&as, ns, ranges, n_ranges, udf_write_one, (void *)bin);
		}
	}
	else {
		usage();
	}

	aerospike_close(&as, &err);
	aerospike_destroy(&as);

	return rc;
}
