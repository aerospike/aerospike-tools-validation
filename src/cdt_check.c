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
// Functions.
//

static bool
map_is_key(const uint8_t *buf, uint32_t buf_sz)
{
	msgpack_in mp = {
			.buf = (uint8_t *)buf,
			.buf_sz = buf_sz
	};

	msgpack_type type = msgpack_peek_type(&mp);

	switch (type) {
	case MSGPACK_TYPE_NEGINT:
	case MSGPACK_TYPE_INT:
	case MSGPACK_TYPE_STRING:
		return true;
	case MSGPACK_TYPE_BYTES: {
		uint32_t len;
		const uint8_t *b = msgpack_get_bin(&mp, &len);

		if (b == NULL || len == 0 || *b != AS_BYTES_BLOB) {
			break;
		}

		return true;
	}
	default:
		break;
	}

	return false;
}

const uint8_t *
check_map_keys_internal(const uint8_t *b, const uint8_t *end)
{
	bool has_nonstorage = false;
	bool not_compact = false;
	uint32_t count = 1;
	msgpack_type type;
	const uint8_t *next_b = msgpack_parse(b, end, &count, &type,
			&has_nonstorage, &not_compact);
	uint32_t ele_count = count - 1;

	if (next_b == NULL) {
		return NULL;
	}

	switch (type) {
	case MSGPACK_TYPE_LIST:
	case MSGPACK_TYPE_MAP:
		break;
	default:
		return next_b;
	}

	if (ele_count == 0) {
		return next_b;
	}

	if (msgpack_buf_peek_type(next_b, (uint32_t)(end - next_b)) ==
			MSGPACK_TYPE_EXT) {
		msgpack_type type2;
		next_b = msgpack_parse(next_b, end, &count, &type2, &has_nonstorage,
				&not_compact);

		if (next_b == NULL) {
			return NULL;
		}

		if (type == MSGPACK_TYPE_MAP) {
			next_b = msgpack_parse(next_b, end, &count, &type2, &has_nonstorage,
					&not_compact);
			ele_count--;

			if (next_b == NULL) {
				return NULL;
			}
		}

		ele_count--;
	}

	if (type == MSGPACK_TYPE_LIST) {
		for (uint32_t i = 0; i < ele_count; i++) {
			next_b = check_map_keys_internal(next_b, end);

			if (next_b == NULL) {
				return NULL;
			}
		}
	}
	else { // MAPs
		ele_count /= 2;

		for (uint32_t i = 0; i < ele_count; i++) {
			if (! map_is_key(next_b, (uint32_t)(end - next_b))) {
				return NULL;
			}

			next_b = check_map_keys_internal(next_b, end);

			if (next_b == NULL) {
				return NULL;
			}

			next_b = check_map_keys_internal(next_b, end);

			if (next_b == NULL) {
				return NULL;
			}
		}
	}

	return next_b;
}

// Return true to need fix.
//static bool
//cdt_check_map_keys_recursive(const uint8_t *buf, uint32_t sz, backup_config *bc)
//{
//	msgpack_type type = msgpack_buf_peek_type(buf, sz);
//	cdt_stats *stats = NULL;
//
//	switch (type) {
//	case MSGPACK_TYPE_LIST:
//		stats = &bc->cdt_list;
//		break;
//	case MSGPACK_TYPE_MAP:
//		stats = &bc->cdt_map;
//		break;
//	default:
//		return false;
//	}
//
//	cf_atomic32_incr(&stats->top_count);
//
//	if (check_map_keys_internal(buf, buf + sz, stats, 0) == NULL) {
//		return true;
//	}
//
//	return false;
//}

//// Return true to log the record.
//static bool
//cdt_check_map_keys(aerospike *as, as_record *rec, backup_config *bc)
//{
//	bool need_log = false; // log record if any bin is corrupt
//
//	for (int32_t i = 0; i < rec->bins.size; ++i) {
//		as_bin *bin = &rec->bins.entries[i];
//		as_val *val = (as_val *)bin->valuep;
//
//		if (val->type != AS_BYTES) {
//			continue;
//		}
//
//		as_bytes *b = (as_bytes *)val;
//		as_bytes_type b_type = as_bytes_get_type(b);
//
//		if (b_type != AS_BYTES_LIST && b_type != AS_BYTES_MAP) {
//			continue;
//		}
//
//		uint8_t *buf = as_bytes_get(b);
//		uint32_t buf_sz = as_bytes_size(b);
//		bool check = cdt_check_map_keys_recursive(buf, buf_sz, bc);
//
//		if (check) {
//			need_log = true;
//		}
//	}
//
//	return need_log;
//}

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
cdt_map_dup_key_check(uint32_t ele_count, const uint8_t *contents,
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

// Return true for need fix.
static bool
cdt_map_need_fix(const uint8_t *buf, uint32_t sz, cdt_fix *cf, cdt_stats *st,
		bool check_map_keys)
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

		if (check_map_keys) {
			for (uint32_t i = 0; i < ele_count; i++) {
				const uint8_t *start = mp.buf + mp.offset;
				uint32_t sz = msgpack_sz_rep(&mp, 1);

				if (sz == 0 || mp.has_nonstorage) {
					cdt_check_set_cannotfix(&mp, cf, st);
					return false;
				}

				if (! map_is_key(start, sz)) {
					cf->need_log = true;
					atomic_incr(&st->cf_invalidkey);
					return false;
				}

				start = mp.buf + mp.offset;
				sz = msgpack_sz_rep(&mp, 1);

				if (sz == 0 || mp.has_nonstorage) {
					cdt_check_set_cannotfix(&mp, cf, st);
					return false;
				}

				const uint8_t *end = mp.buf + mp.offset;

				if (check_map_keys_internal(start, end) == NULL) {
					cf->need_log = true;
					atomic_incr(&st->cf_invalidkey);
					return false;
				}
			}
		}
		else if (msgpack_sz_rep(&mp, 2 * ele_count) == 0 || mp.has_nonstorage) {
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
	const uint8_t *start = mp.buf + mp.offset;
	uint32_t ele_sz = msgpack_sz_rep(&mp, 1);

	if (ele_sz == 0 || mp.has_nonstorage) {
		cdt_check_set_cannotfix(&mp, cf, st);
		return false;
	}

	if (check_map_keys && ! map_is_key(start, ele_sz)) {
		cf->need_log = true;
		atomic_incr(&st->cf_invalidkey);
		return false;
	}

	start = mp.buf + mp.offset;

	if (msgpack_sz_rep(&mp, 1) == 0 || mp.has_nonstorage) {
		cdt_check_set_cannotfix(&mp, cf, st);
		return false;
	}

	const uint8_t *end = mp.buf + mp.offset;

	if (check_map_keys && check_map_keys_internal(start, end) == NULL) {
		cf->need_log = true;
		atomic_incr(&st->cf_invalidkey);
		return false;
	}

	for (uint32_t i = 1; i < ele_count - 1; i++) {
		const uint8_t *start = mp.buf + mp.offset;
		msgpack_cmp_type cmp = msgpack_cmp(&mp_prev, &mp);
		uint32_t ele_sz = (uint32_t)(mp.buf + mp.offset - start);

		if (check_map_keys && ! map_is_key(start, ele_sz)) {
			cf->need_log = true;
			atomic_incr(&st->cf_invalidkey);
			return false;
		}

		start = mp.buf + mp.offset;

		if (msgpack_sz(&mp_prev) == 0 || msgpack_sz(&mp) == 0 ||
				mp.has_nonstorage) {
			cdt_check_set_cannotfix(&mp, cf, st);
			return false;
		}

		if (cmp != MSGPACK_CMP_LESS) {
			if (mp.has_nonstorage || (ele_count - i - 1 != 0 &&
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

		const uint8_t *end = mp.buf + mp.offset;

		if (check_map_keys && check_map_keys_internal(start, end) == NULL) {
			cf->need_log = true;
			atomic_incr(&st->cf_invalidkey);
			return false;
		}
	}

	cf->content_sz = (uint32_t)(mp.buf + mp.offset - cf->contents);
	return cdt_check_sz(&mp, sz, cf, st);
}

// Return true for need fix.
static bool
cdt_list_need_fix(const uint8_t *buf, uint32_t sz, cdt_fix *cf, cdt_stats *st,
		bool check_map_keys)
//		backup_config *bc)
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

		if (check_map_keys) {
			for (uint32_t i = 0; i < ele_count; i++) {
				const uint8_t *start = mp.buf + mp.offset;

				if (msgpack_sz_rep(&mp, 1) == 0 || mp.has_nonstorage) {
					cdt_check_set_cannotfix(&mp, cf, st);
					return false;
				}

				const uint8_t *end = mp.buf + mp.offset;

				if (check_map_keys_internal(start, end) == NULL) {
					cf->need_log = true;
					atomic_incr(&st->cf_invalidkey);
					return false;
				}
			}
		}
		else if (msgpack_sz_rep(&mp, ele_count) == 0 || mp.has_nonstorage) {
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
	const uint8_t *start = mp.buf + mp.offset;

	if (msgpack_sz_rep(&mp, 1) == 0 || mp.has_nonstorage) {
		cdt_check_set_cannotfix(&mp, cf, st);
		return false;
	}

	const uint8_t *end = mp.buf + mp.offset;

	if (check_map_keys && check_map_keys_internal(start, end) == NULL) {
		cf->need_log = true;
		atomic_incr(&st->cf_invalidkey);
		return false;
	}

	for (uint32_t i = 1; i < ele_count - 1; i++) {
		const uint8_t *start = mp.buf + mp.offset;
		msgpack_cmp_type cmp = msgpack_cmp(&mp_prev, &mp);
		const uint8_t *end = mp.buf + mp.offset;

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

		if (check_map_keys && check_map_keys_internal(start, end) == NULL) {
			cf->need_log = true;
			atomic_incr(&st->cf_invalidkey);
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
		atomic_incr(&bc->cdt_list.count);
		return cdt_list_need_fix(buf, sz, cf, &bc->cdt_list,
				bc->check_map_keys);
	case MSGPACK_TYPE_MAP:
		atomic_incr(&bc->cdt_map.count);
		return cdt_map_need_fix(buf, sz, cf, &bc->cdt_map,
				bc->check_map_keys);
	default:
		break;
	}

	return false;
}
