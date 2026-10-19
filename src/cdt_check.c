/*
 * Copyright 2015-2026 Aerospike, Inc.
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

//==========================================================
// Includes.
//

#include <cdt_check.h>

#include "msgpack_in.h"


//==========================================================
// Typedefs & constants.
//

// Below this many keys, comparing every pair is cheaper than sorting.
#define DUP_KEY_SORT_MIN 16


//==========================================================
// Functions.
//

static void
cdt_check_set_cannotfix(const msgpack_in *mp, cdt_fix *cf, cdt_stats *stat)
{
	cf->need_log = true;
	atomic_incr(&stat->cannot_fix);

	if (mp->has_nonstorage) {
		atomic_incr(&stat->cf_nonstorage);
	}
	else {
		atomic_incr(&stat->cf_corrupt);
	}
}

// Return true for need padding fix.
static bool
cdt_check_sz(msgpack_in *mp, uint32_t sz, cdt_fix *cf, cdt_stats *stat)
{
	if (mp->offset < sz) {
		cf->need_log = true;
		atomic_incr(&stat->need_fix);
		atomic_incr(&stat->nf_padding);
		cf->nf_padding = sz - mp->offset;
		return true;
	}

	if (mp->offset > sz) {
		atomic_incr(&stat->cannot_fix);
		atomic_incr(&stat->cf_corrupt);
	}

	return false;
}

static bool
dup_key_check_pairs(uint32_t ele_count, const uint8_t *contents,
		uint32_t content_sz)
{
	if (ele_count <= 1) {
		return false;
	}

	msgpack_in mp = {
			.buf = contents,
			.buf_sz = content_sz
	};

	// Simple O(n^2 / 2) check for dup keys.
	for (uint32_t i = 1; i < ele_count; i++) {
		uint32_t cur_off = mp.offset;

		msgpack_sz_rep(&mp, 2);

		uint32_t next_off = mp.offset;
		msgpack_in rhs = mp;

		for (uint32_t j = i; j < ele_count; j++) {
			mp.offset = cur_off;

			if (msgpack_cmp(&mp, &rhs) == MSGPACK_CMP_EQUAL) {
				return true;
			}

			msgpack_sz(&rhs);
		}

		mp.offset = next_off;
	}

	return false;
}

typedef struct key_ref_s {
	const uint8_t *buf;
	uint32_t sz;
} key_ref;

static msgpack_cmp_type
key_ref_cmp(const key_ref *a, const key_ref *b)
{
	msgpack_in ma = { .buf = a->buf, .buf_sz = a->sz };
	msgpack_in mb = { .buf = b->buf, .buf_sz = b->sz };

	return msgpack_cmp(&ma, &mb);
}

// Bottom-up merge sort of n keys, using tmp (n entries) as scratch. Returns
// whichever of the two arrays holds the result. It never reads or writes out
// of bounds, whatever the comparisons return.
static key_ref *
key_ref_sort(key_ref *keys, key_ref *tmp, uint32_t n)
{
	key_ref *src = keys;
	key_ref *dst = tmp;

	for (uint64_t width = 1; width < n; width *= 2) {
		for (uint64_t lo = 0; lo < n; lo += 2 * width) {
			uint32_t mid = (uint32_t)(lo + width < n ? lo + width : n);
			uint32_t hi = (uint32_t)(lo + 2 * width < n ? lo + 2 * width : n);
			uint32_t i = (uint32_t)lo;
			uint32_t j = mid;
			uint32_t k = (uint32_t)lo;

			while (i < mid && j < hi) {
				dst[k++] = key_ref_cmp(&src[j], &src[i]) == MSGPACK_CMP_LESS ?
						src[j++] : src[i++];
			}

			while (i < mid) {
				dst[k++] = src[i++];
			}

			while (j < hi) {
				dst[k++] = src[j++];
			}
		}

		key_ref *swap = src;

		src = dst;
		dst = swap;
	}

	return src;
}

// Keys whose msgpack_cmp() order is total: equal keys end up next to each
// other after sorting. Lists and maps (unordered maps do not compare
// reliably) and doubles (NaN compares equal to every double) are not.
static bool
key_sortable(const msgpack_in *mp)
{
	switch (msgpack_peek_type(mp)) {
	case MSGPACK_TYPE_NIL:
	case MSGPACK_TYPE_FALSE:
	case MSGPACK_TYPE_TRUE:
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT:
	case MSGPACK_TYPE_STRING:
	case MSGPACK_TYPE_BYTES:
	case MSGPACK_TYPE_GEOJSON:
		return true;
	default:
		return false;
	}
}

// Return true if two keys are equal. Sorts the keys (O(n log n)) when that
// gives the same answer as comparing every pair, otherwise compares every
// pair. The contents have already been parsed, so every element is complete.
static bool
cdt_map_dup_key_check(uint32_t ele_count, const uint8_t *contents,
		uint32_t content_sz)
{
	if (ele_count < DUP_KEY_SORT_MIN) {
		return dup_key_check_pairs(ele_count, contents, content_sz);
	}

	// A key and its value take at least 2 bytes, whatever the header claims.
	uint32_t max_keys = content_sz / 2 < ele_count ? content_sz / 2 : ele_count;
	key_ref *keys = cf_malloc(2 * (size_t)max_keys * sizeof(key_ref));

	if (keys == NULL) {
		return dup_key_check_pairs(ele_count, contents, content_sz);
	}

	msgpack_in mp = {
			.buf = contents,
			.buf_sz = content_sz
	};

	uint32_t n_keys = 0;

	while (n_keys < max_keys && mp.offset < content_sz) {
		uint32_t off = mp.offset;

		if (! key_sortable(&mp)) {
			cf_free(keys);
			return dup_key_check_pairs(ele_count, contents, content_sz);
		}

		msgpack_sz(&mp);
		keys[n_keys++] = (key_ref){ contents + off, mp.offset - off };
		msgpack_sz(&mp);
	}

	key_ref *sorted = key_ref_sort(keys, keys + max_keys, n_keys);
	bool dup = false;

	for (uint32_t i = 1; i < n_keys && ! dup; i++) {
		dup = key_ref_cmp(&sorted[i - 1], &sorted[i]) == MSGPACK_CMP_EQUAL;
	}

	cf_free(keys);

	return dup;
}

// Return true for need fix.
static bool
cdt_map_need_fix(const uint8_t *buf, uint32_t sz, cdt_fix *cf, cdt_stats *st)
{
	msgpack_in mp = {
			.buf = buf,
			.buf_sz = sz
	};

	uint32_t ele_count;

	if (! msgpack_get_map_ele_count(&mp, &ele_count)) {
		cf->need_log = true;
		atomic_incr(&st->cannot_fix);
		atomic_incr(&st->cf_corrupt);
		return false;
	}

	if (ele_count == 0) { // empty
		cf->ele_count = ele_count;
		cf->contents = mp.buf + mp.offset;
		cf->content_sz = 0;
		return cdt_check_sz(&mp, sz, cf, st);
	}

	msgpack_ext ext;

	if (msgpack_peek_is_ext(&mp)) {
		if (! msgpack_get_ext(&mp, &ext) || msgpack_sz(&mp) == 0) {
			cf->need_log = true;
			atomic_incr(&st->cannot_fix);
			atomic_incr(&st->cf_corrupt);
			return false; // corrupted ext
		}
	}
	else { // not ordered
		cf->ele_count = ele_count;
		cf->contents = mp.buf + mp.offset;

		if (msgpack_sz_rep(&mp, 2 * ele_count) == 0 || mp.has_nonstorage) {
			cdt_check_set_cannotfix(&mp, cf, st);
			return false;
		}

		cf->content_sz = (uint32_t)(mp.buf + mp.offset - cf->contents);

		if (cdt_map_dup_key_check(ele_count, cf->contents, cf->content_sz)) {
			cf->need_log = true;
			atomic_incr(&st->cannot_fix);
			atomic_incr(&st->cf_dupkey);
			return false;
		}

		return cdt_check_sz(&mp, sz, cf, st);
	}

	cf->ele_count = ele_count - 1;
	cf->contents = mp.buf + mp.offset;

	if (cf->ele_count == 0) {
		cf->content_sz = 0;
		return cdt_check_sz(&mp, sz, cf, st);
	}

	msgpack_in mp_prev = mp;

	if (msgpack_sz_rep(&mp, 1) == 0 || mp.has_nonstorage) {
		cdt_check_set_cannotfix(&mp, cf, st);
		return false;
	}

	if (msgpack_sz_rep(&mp, 1) == 0 || mp.has_nonstorage) {
		cdt_check_set_cannotfix(&mp, cf, st);
		return false;
	}

	for (uint32_t i = 1; i < ele_count - 1; i++) {
		msgpack_cmp_type cmp = msgpack_cmp(&mp_prev, &mp);

		if (msgpack_sz(&mp_prev) == 0 || msgpack_sz(&mp) == 0 ||
				mp.has_nonstorage) {
			cdt_check_set_cannotfix(&mp, cf, st);
			return false;
		}

		if (cmp != MSGPACK_CMP_LESS) {
			if (mp.has_nonstorage || (ele_count - i - 2 != 0 &&
					(msgpack_sz_rep(&mp, 2 * (ele_count - i - 2)) == 0 ||
							mp.has_nonstorage))) {
				cdt_check_set_cannotfix(&mp, cf, st);
				return false;
			}

			cf->content_sz = (uint32_t)(mp.buf + mp.offset - cf->contents);
			cf->nf_map_order = true;

			if (mp.offset <= sz) {
				if (cdt_map_dup_key_check(ele_count, cf->contents,
						cf->content_sz)) {
					cf->need_log = true;
					atomic_incr(&st->cannot_fix);
					atomic_incr(&st->cf_dupkey);
					return false;
				}

				atomic_incr(&st->need_fix);
				atomic_incr(&st->nf_order);

				if (mp.offset != sz) {
					atomic_incr(&st->nf_padding);
					cf->nf_padding = sz - mp.offset;
				}

				return true; // fix order and maybe padding
			}

			cf->need_log = true;
			atomic_incr(&st->cannot_fix);
			atomic_incr(&st->cf_corrupt);
			return false;
		}
	}

	cf->content_sz = (uint32_t)(mp.buf + mp.offset - cf->contents);
	return cdt_check_sz(&mp, sz, cf, st);
}

// Return true for need fix.
static bool
cdt_list_need_fix(const uint8_t *buf, uint32_t sz, cdt_fix *cf, cdt_stats *st)
{
	msgpack_in mp = {
			.buf = buf,
			.buf_sz = sz
	};

	uint32_t ele_count;

	if (! msgpack_get_list_ele_count(&mp, &ele_count)) {
		cf->need_log = true;
		atomic_incr(&st->cannot_fix);
		atomic_incr(&st->cf_corrupt);
		return false;
	}

	if (ele_count == 0) { // empty
		cf->ele_count = ele_count;
		cf->contents = mp.buf + mp.offset;
		cf->content_sz = 0;
		return cdt_check_sz(&mp, sz, cf, st);
	}

	msgpack_ext ext;

	if (msgpack_peek_is_ext(&mp)) {
		if (! msgpack_get_ext(&mp, &ext)) {
			cf->need_log = true;
			atomic_incr(&st->cannot_fix);
			atomic_incr(&st->cf_corrupt);
			return false; // corrupted ext
		}
	}
	else { // not ordered
		cf->ele_count = ele_count;
		cf->contents = mp.buf + mp.offset;

		if (msgpack_sz_rep(&mp, ele_count) == 0 || mp.has_nonstorage) {
			cdt_check_set_cannotfix(&mp, cf, st);
			return false;
		}

		cf->content_sz = (uint32_t)(mp.buf + mp.offset - cf->contents);

		return cdt_check_sz(&mp, sz, cf, st);
	}

	cf->ele_count = ele_count - 1;
	cf->contents = mp.buf + mp.offset;

	if (cf->ele_count == 0) {
		cf->content_sz = 0;
		return cdt_check_sz(&mp, sz, cf, st);
	}

	msgpack_in mp_prev = mp;

	if (msgpack_sz_rep(&mp, 1) == 0 || mp.has_nonstorage) {
		cdt_check_set_cannotfix(&mp, cf, st);
		return false;
	}

	for (uint32_t i = 1; i < ele_count - 1; i++) {
		msgpack_cmp_type cmp = msgpack_cmp(&mp_prev, &mp);

		if (cmp != MSGPACK_CMP_LESS && cmp != MSGPACK_CMP_EQUAL) {
			if (mp.has_nonstorage || (ele_count - i - 2 != 0 &&
					(msgpack_sz_rep(&mp, ele_count - i - 2) == 0 ||
							mp.has_nonstorage))) {
				cdt_check_set_cannotfix(&mp, cf, st);
				return false;
			}

			cf->content_sz = (uint32_t)(mp.buf + mp.offset - cf->contents);
			cf->nf_list_order = true;

			if (mp.offset <= sz) {
				atomic_incr(&st->need_fix);
				atomic_incr(&st->nf_order);

				if (mp.offset != sz) {
					atomic_incr(&st->nf_padding);
					cf->nf_padding = sz - mp.offset;
				}

				return true; // fix order and maybe padding
			}

			atomic_incr(&st->cannot_fix);
			atomic_incr(&st->cf_corrupt);
			return false;
		}
	}

	if (mp.has_nonstorage) {
		cf->need_log = true;
		atomic_incr(&st->cannot_fix);
		atomic_incr(&st->cf_nonstorage);
		return false;
	}

	cf->content_sz = (uint32_t)(mp.buf + mp.offset - cf->contents);

	return cdt_check_sz(&mp, sz, cf, st);
}

// Return true to need fix.
bool
cdt_need_fix(const uint8_t *buf, uint32_t sz, cdt_fix *cf, backup_config *bc)
{
	switch (msgpack_buf_peek_type(buf, sz)) {
	case MSGPACK_TYPE_LIST:
		return cdt_list_need_fix(buf, sz, cf, &bc->cdt_list);
	case MSGPACK_TYPE_MAP:
		return cdt_map_need_fix(buf, sz, cf, &bc->cdt_map);
	default:
		break;
	}

	return false;
}
