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
