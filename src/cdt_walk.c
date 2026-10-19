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

//==========================================================
// Includes.
//

#include <stddef.h>

#include <cdt_walk.h>


//==========================================================
// Typedefs & constants.
//

#define AS_STRING_TYPE 3
#define AS_BLOB_TYPE 4

typedef enum {
	ELE_SCALAR,
	ELE_LIST,
	ELE_MAP,
	ELE_CORRUPT
} ele_kind;


//==========================================================
// Inlines & macros.
//

static inline uint32_t
read_be16(const uint8_t *p)
{
	return (uint32_t)p[0] << 8 | p[1];
}

static inline uint32_t
read_be32(const uint8_t *p)
{
	return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 | (uint32_t)p[2] << 8 |
			p[3];
}

// Matches msgpack_buf_peek_type() == MSGPACK_TYPE_EXT on the server, which is
// what makes a leading map key the ordered-map meta key. fixext1 is not one.
static inline bool
is_meta_ext(uint8_t b)
{
	return (b >= 0xc7 && b <= 0xc9) || (b >= 0xd5 && b <= 0xd8);
}

// Parses the header of the element at *pp. A list/map returns its element
// count and leaves *pp at its first element; anything else is skipped whole,
// ext payloads included. key_ok says whether a scalar is a valid map key.
static inline ele_kind
next_ele(const uint8_t **pp, const uint8_t *end, uint32_t *count, bool *key_ok)
{
	const uint8_t *p = *pp;
	uint64_t skip;

	if (p >= end) {
		return ELE_CORRUPT;
	}

	uint8_t b = *p++;
	size_t avail = (size_t)(end - p);

	*key_ok = false;

	switch (b) {
	case 0x00 ... 0x7f:
	case 0xe0 ... 0xff:
		*key_ok = true;
		*pp = p;
		return ELE_SCALAR;
	case 0x80 ... 0x8f:
		*count = b & 0x0f;
		*pp = p;
		return ELE_MAP;
	case 0x90 ... 0x9f:
		*count = b & 0x0f;
		*pp = p;
		return ELE_LIST;
	case 0xa0 ... 0xbf:
		skip = b & 0x1f;
		goto raw;
	case 0xc0:
	case 0xc2:
	case 0xc3:
		*pp = p;
		return ELE_SCALAR;
	case 0xc4:
	case 0xd9:
		if (avail < 1) {
			return ELE_CORRUPT;
		}

		skip = p[0];
		p += 1;
		goto raw;
	case 0xc5:
	case 0xda:
		if (avail < 2) {
			return ELE_CORRUPT;
		}

		skip = read_be16(p);
		p += 2;
		goto raw;
	case 0xc6:
	case 0xdb:
		if (avail < 4) {
			return ELE_CORRUPT;
		}

		skip = read_be32(p);
		p += 4;
		goto raw;
	case 0xc7:
		if (avail < 1) {
			return ELE_CORRUPT;
		}

		skip = 1 + 1 + (uint64_t)p[0];
		break;
	case 0xc8:
		if (avail < 2) {
			return ELE_CORRUPT;
		}

		skip = 2 + 1 + (uint64_t)read_be16(p);
		break;
	case 0xc9:
		if (avail < 4) {
			return ELE_CORRUPT;
		}

		skip = 4 + 1 + (uint64_t)read_be32(p);
		break;
	case 0xca:
	case 0xce:
	case 0xd2:
		*key_ok = b != 0xca;
		skip = 4;
		break;
	case 0xcb:
	case 0xcf:
	case 0xd3:
		*key_ok = b != 0xcb;
		skip = 8;
		break;
	case 0xcc:
	case 0xd0:
		*key_ok = true;
		skip = 1;
		break;
	case 0xcd:
	case 0xd1:
		*key_ok = true;
		skip = 2;
		break;
	case 0xd4 ... 0xd8:
		skip = 1 + ((uint64_t)1 << (b - 0xd4));
		break;
	case 0xdc:
	case 0xde:
		if (avail < 2) {
			return ELE_CORRUPT;
		}

		*count = read_be16(p);
		*pp = p + 2;
		return b == 0xdc ? ELE_LIST : ELE_MAP;
	case 0xdd:
	case 0xdf:
		if (avail < 4) {
			return ELE_CORRUPT;
		}

		*count = read_be32(p);
		*pp = p + 4;
		return b == 0xdd ? ELE_LIST : ELE_MAP;
	default: // 0xc1 is never used
		return ELE_CORRUPT;
	}

	if (skip > avail) {
		return ELE_CORRUPT;
	}

	*pp = p + skip;
	return ELE_SCALAR;

raw:
	if (skip > (uint64_t)(end - p)) {
		return ELE_CORRUPT;
	}

	*key_ok = skip != 0 && (p[0] == AS_STRING_TYPE || p[0] == AS_BLOB_TYPE);
	*pp = p + skip;

	return ELE_SCALAR;
}


//==========================================================
// Public API.
//

void
cdt_walk(const uint8_t *buf, uint32_t buf_sz, uint32_t max_depth,
		bool check_keys, cdt_walk_result *res)
{
	uint32_t remaining[CDT_WALK_MAX_DEPTH];
	bool is_map[CDT_WALK_MAX_DEPTH];
	uint32_t n_levels = 0;
	const uint8_t *p = buf;
	const uint8_t *end = buf + buf_sz;

	if (max_depth > CDT_WALK_MAX_DEPTH) {
		max_depth = CDT_WALK_MAX_DEPTH;
	}

	res->flags = 0;
	res->depth = 0;
	res->sz = 0;

	while (true) {
		bool is_key = false;

		if (n_levels != 0) {
			uint32_t top = n_levels - 1;

			// A map counts down two per pair, so keys sit at even counts.
			is_key = is_map[top] && (remaining[top] & 1) == 0;
			remaining[top]--;
		}

		uint32_t count;
		bool key_ok;
		ele_kind kind = next_ele(&p, end, &count, &key_ok);

		if (kind == ELE_CORRUPT) {
			res->flags |= CDT_WALK_CORRUPT;
			return;
		}

		if (kind == ELE_SCALAR) {
			if (is_key && check_keys && ! key_ok) {
				res->flags |= CDT_WALK_INVALID_KEY;
			}
		}
		else {
			uint32_t level = n_levels + 1;

			if (is_key && check_keys) {
				res->flags |= CDT_WALK_INVALID_KEY;
			}

			if (level > res->depth) {
				res->depth = level;
			}

			if (count == 0) {
				if (level > max_depth) {
					res->flags |= CDT_WALK_EMPTY_OVER;
				}
			}
			else if (level > max_depth) {
				res->flags |= CDT_WALK_OVER;
				return;
			}
			else {
				bool map = kind == ELE_MAP;
				uint32_t avail = (uint32_t)(end - p);

				// Every element takes at least a byte, which also keeps the
				// doubled map count from overflowing.
				if (map ? count > avail / 2 : count > avail) {
					res->flags |= CDT_WALK_CORRUPT;
					return;
				}

				uint32_t n_eles = map ? 2 * count : count;

				if (map && p < end && is_meta_ext(*p)) {
					uint32_t dummy;

					if (next_ele(&p, end, &dummy, &key_ok) != ELE_SCALAR ||
							next_ele(&p, end, &dummy, &key_ok) != ELE_SCALAR) {
						res->flags |= CDT_WALK_CORRUPT;
						return;
					}

					n_eles -= 2;
				}

				if (n_eles != 0) {
					remaining[n_levels] = n_eles;
					is_map[n_levels] = map;
					n_levels++;
				}
			}
		}

		while (n_levels != 0 && remaining[n_levels - 1] == 0) {
			n_levels--;
		}

		if (n_levels == 0) {
			break;
		}
	}

	res->sz = (uint32_t)(p - buf);
}
