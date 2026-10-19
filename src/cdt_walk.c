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

typedef enum {
	HDR_FIXED, // scalar, 'n' more bytes
	HDR_FIXRAW, // string/blob, length in the low 5 bits
	HDR_RAW, // string/blob, 'n'-byte length
	HDR_EXT, // ext, 'n'-byte length, then a type byte
	HDR_FIXLIST,
	HDR_FIXMAP,
	HDR_LIST, // 'n'-byte count
	HDR_MAP, // 'n'-byte count
	HDR_BAD
} hdr_kind;

typedef struct hdr_info_s {
	uint8_t kind;
	uint8_t n;
	bool key_ok;
} hdr_info;

// One entry per msgpack header byte, so the common fixed-size scalars take a
// single, well-predicted branch.
static const hdr_info hdr_table[256] = {
	[0x00 ... 0x7f] = { HDR_FIXED, 0, true },
	[0x80 ... 0x8f] = { HDR_FIXMAP, 0, false },
	[0x90 ... 0x9f] = { HDR_FIXLIST, 0, false },
	[0xa0 ... 0xbf] = { HDR_FIXRAW, 0, false },
	[0xc0] = { HDR_FIXED, 0, false },
	[0xc1] = { HDR_BAD, 0, false },
	[0xc2] = { HDR_FIXED, 0, false },
	[0xc3] = { HDR_FIXED, 0, false },
	[0xc4] = { HDR_RAW, 1, false },
	[0xc5] = { HDR_RAW, 2, false },
	[0xc6] = { HDR_RAW, 4, false },
	[0xc7] = { HDR_EXT, 1, false },
	[0xc8] = { HDR_EXT, 2, false },
	[0xc9] = { HDR_EXT, 4, false },
	[0xca] = { HDR_FIXED, 4, false },
	[0xcb] = { HDR_FIXED, 8, false },
	[0xcc] = { HDR_FIXED, 1, true },
	[0xcd] = { HDR_FIXED, 2, true },
	[0xce] = { HDR_FIXED, 4, true },
	[0xcf] = { HDR_FIXED, 8, true },
	[0xd0] = { HDR_FIXED, 1, true },
	[0xd1] = { HDR_FIXED, 2, true },
	[0xd2] = { HDR_FIXED, 4, true },
	[0xd3] = { HDR_FIXED, 8, true },
	[0xd4] = { HDR_FIXED, 2, false },
	[0xd5] = { HDR_FIXED, 3, false },
	[0xd6] = { HDR_FIXED, 5, false },
	[0xd7] = { HDR_FIXED, 9, false },
	[0xd8] = { HDR_FIXED, 17, false },
	[0xd9] = { HDR_RAW, 1, false },
	[0xda] = { HDR_RAW, 2, false },
	[0xdb] = { HDR_RAW, 4, false },
	[0xdc] = { HDR_LIST, 2, false },
	[0xdd] = { HDR_LIST, 4, false },
	[0xde] = { HDR_MAP, 2, false },
	[0xdf] = { HDR_MAP, 4, false },
	[0xe0 ... 0xff] = { HDR_FIXED, 0, true }
};


//==========================================================
// Inlines & macros.
//

static inline uint32_t
read_be(const uint8_t *p, uint32_t n)
{
	switch (n) {
	case 1:
		return p[0];
	case 2:
		return (uint32_t)p[0] << 8 | p[1];
	default:
		return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
				(uint32_t)p[2] << 8 | p[3];
	}
}

// Matches msgpack_buf_peek_type() == MSGPACK_TYPE_EXT on the server, which is
// what makes a leading map key the ordered-map meta key. fixext1 is not one,
// nor is an ext8 holding the wildcard or infinity query marker.
static inline bool
is_meta_ext(const uint8_t *p, const uint8_t *end)
{
	uint8_t b = p[0];

	if (b == 0xc7) {
		return ! (end - p >= 4 && p[1] == 1 && p[2] == 0xff && p[3] <= 1);
	}

	return b == 0xc8 || b == 0xc9 || (b >= 0xd5 && b <= 0xd8);
}

// Whether the scalar at 'ele' may be a map key: an integer, or a string/blob
// whose first content byte says string or blob. The caller has already parsed
// the element, so its bytes are known to be in the buffer.
static inline bool
scalar_key_ok(const uint8_t *ele)
{
	const hdr_info *h = &hdr_table[ele[0]];

	switch (h->kind) {
	case HDR_FIXED:
		return h->key_ok;
	case HDR_FIXRAW:
		return (ele[0] & 0x1f) != 0 &&
				(ele[1] == AS_STRING_TYPE || ele[1] == AS_BLOB_TYPE);
	case HDR_RAW: {
		const uint8_t *data = ele + 1 + h->n;

		return read_be(ele + 1, h->n) != 0 &&
				(data[0] == AS_STRING_TYPE || data[0] == AS_BLOB_TYPE);
	}
	default:
		return false;
	}
}

// Parses the header of the element at *pp. A list/map returns its element
// count and leaves *pp at its first element; anything else is skipped whole,
// ext payloads included.
static inline ele_kind
next_ele(const uint8_t **pp, const uint8_t *end, uint32_t *count)
{
	const uint8_t *p = *pp;

	if (p >= end) {
		return ELE_CORRUPT;
	}

	uint8_t b = *p++;
	const hdr_info *h = &hdr_table[b];
	size_t avail = (size_t)(end - p);
	uint64_t len;

	if (h->kind == HDR_FIXED) {
		if (h->n > avail) {
			return ELE_CORRUPT;
		}

		*pp = p + h->n;
		return ELE_SCALAR;
	}

	switch (h->kind) {
	case HDR_FIXRAW:
		len = b & 0x1f;
		break;
	case HDR_RAW:
		if (h->n > avail) {
			return ELE_CORRUPT;
		}

		len = read_be(p, h->n);
		p += h->n;
		avail -= h->n;
		break;
	case HDR_EXT:
		if (h->n > avail) {
			return ELE_CORRUPT;
		}

		len = (uint64_t)read_be(p, h->n) + 1;
		p += h->n;
		avail -= h->n;

		if (len > avail) {
			return ELE_CORRUPT;
		}

		*pp = p + len;
		return ELE_SCALAR;
	case HDR_FIXLIST:
	case HDR_FIXMAP:
		*count = b & 0x0f;
		*pp = p;
		return h->kind == HDR_FIXLIST ? ELE_LIST : ELE_MAP;
	case HDR_LIST:
	case HDR_MAP:
		if (h->n > avail) {
			return ELE_CORRUPT;
		}

		*count = read_be(p, h->n);
		*pp = p + h->n;
		return h->kind == HDR_LIST ? ELE_LIST : ELE_MAP;
	default:
		return ELE_CORRUPT;
	}

	if (len > avail) {
		return ELE_CORRUPT;
	}

	*pp = p + len;

	return ELE_SCALAR;
}


//==========================================================
// Public API.
//

void
cdt_walk(const uint8_t *buf, uint32_t buf_sz, uint32_t max_depth,
		bool check_keys, cdt_walk_result *res)
{
	// Parent levels; the innermost open level lives in rem and keys.
	uint32_t saved_rem[CDT_WALK_MAX_DEPTH];
	bool saved_keys[CDT_WALK_MAX_DEPTH];
	uint32_t n_open = 0;
	const uint8_t *p = buf;
	const uint8_t *end = buf + buf_sz;
	uint32_t flags = 0;
	uint32_t depth = 0;

	if (max_depth > CDT_WALK_MAX_DEPTH) {
		max_depth = CDT_WALK_MAX_DEPTH;
	}

	// Elements left in the innermost open level, and whether its keys are
	// checked (a map, with check_keys). Before the first list/map opens, that
	// level is the top-level value itself.
	uint32_t rem = 1;
	bool keys = false;

	while (true) {
		while (rem != 0) {
			// A map counts down two per pair, so keys sit at even counts.
			bool is_key = keys && (rem & 1) == 0;
			const uint8_t *ele = p;
			uint32_t count;

			rem--;

			ele_kind kind = next_ele(&p, end, &count);

			if (kind == ELE_SCALAR) {
				if (is_key && ! scalar_key_ok(ele)) {
					flags |= CDT_WALK_INVALID_KEY;
				}

				continue;
			}

			if (kind == ELE_CORRUPT) {
				flags |= CDT_WALK_CORRUPT;
				goto stop;
			}

			uint32_t level = n_open + 1;

			if (is_key) {
				flags |= CDT_WALK_INVALID_KEY;
			}

			if (level > depth) {
				depth = level;
			}

			if (count == 0) {
				if (level > max_depth) {
					flags |= CDT_WALK_EMPTY_OVER;
				}

				continue;
			}

			if (level > max_depth) {
				flags |= CDT_WALK_OVER;
				goto stop;
			}

			bool child_map = kind == ELE_MAP;
			uint32_t avail = (uint32_t)(end - p);

			// Every element takes at least a byte, which also keeps the
			// doubled map count from overflowing.
			if (child_map ? count > avail / 2 : count > avail) {
				flags |= CDT_WALK_CORRUPT;
				goto stop;
			}

			uint32_t n_eles = child_map ? 2 * count : count;

			if (child_map && p < end && is_meta_ext(p, end)) {
				uint32_t dummy;

				if (next_ele(&p, end, &dummy) != ELE_SCALAR ||
						next_ele(&p, end, &dummy) != ELE_SCALAR) {
					flags |= CDT_WALK_CORRUPT;
					goto stop;
				}

				n_eles -= 2;
			}

			if (n_eles != 0) {
				saved_rem[n_open] = rem;
				saved_keys[n_open] = keys;
				n_open++;
				rem = n_eles;
				keys = child_map && check_keys;
			}
		}

		if (n_open == 0) {
			break;
		}

		n_open--;
		rem = saved_rem[n_open];
		keys = saved_keys[n_open];
	}

	res->flags = flags;
	res->depth = depth;
	res->sz = (uint32_t)(p - buf);
	return;

stop:
	res->flags = flags;
	res->depth = depth;
	res->sz = 0;
}
