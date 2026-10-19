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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ref_walk.h"

const char *ref_class_names[N_REF_CLASSES] = { "ok", "strict_only", "over" };

//==========================================================
// Reference depth walk.
//
// Deliberately simple: heap-grown stack, no depth cap, no shortcuts. Used to
// compute the expected class of every stored bin.
//

typedef struct frame_s {
	uint64_t remaining;
	bool is_map;
} frame;

static bool
rd_be(const uint8_t **p, const uint8_t *end, uint32_t n_bytes, uint64_t *v)
{
	if ((size_t)(end - *p) < n_bytes) {
		return false;
	}

	*v = 0;

	for (uint32_t i = 0; i < n_bytes; i++) {
		*v = (*v << 8) | *(*p)++;
	}

	return true;
}

static bool
skip_bytes(const uint8_t **p, const uint8_t *end, uint64_t n)
{
	if ((uint64_t)(end - *p) < n) {
		return false;
	}

	*p += n;
	return true;
}

typedef enum {
	EL_SCALAR,
	EL_LIST,
	EL_MAP,
	EL_EXT,
	EL_ERROR
} el_kind;

// Parses one element header. Containers return their element count, scalars
// are skipped whole. key_ok reports whether a scalar may be a map key.
static el_kind
parse_el(const uint8_t **p, const uint8_t *end, uint64_t *count, bool *key_ok)
{
	uint64_t len = 0;

	*key_ok = false;

	if (*p >= end) {
		return EL_ERROR;
	}

	uint8_t b = *(*p)++;

	if (b < 0x80 || b >= 0xe0) {
		*key_ok = true;
		return EL_SCALAR;
	}

	if ((b & 0xf0) == 0x80) {
		*count = b & 0x0f;
		return EL_MAP;
	}

	if ((b & 0xf0) == 0x90) {
		*count = b & 0x0f;
		return EL_LIST;
	}

	uint32_t raw_hdr = 0;

	if ((b & 0xe0) == 0xa0) {
		len = b & 0x1f;
	}
	else {
		switch (b) {
		case 0xc0:
		case 0xc2:
		case 0xc3:
			return EL_SCALAR;
		case 0xcc:
		case 0xcd:
		case 0xce:
		case 0xcf:
		case 0xd0:
		case 0xd1:
		case 0xd2:
		case 0xd3: {
			uint32_t w = 1u << ((b & 0x03));

			*key_ok = true;
			return skip_bytes(p, end, w) ? EL_SCALAR : EL_ERROR;
		}
		case 0xca:
			return skip_bytes(p, end, 4) ? EL_SCALAR : EL_ERROR;
		case 0xcb:
			return skip_bytes(p, end, 8) ? EL_SCALAR : EL_ERROR;
		case 0xc4:
		case 0xd9:
			raw_hdr = 1;
			break;
		case 0xc5:
		case 0xda:
			raw_hdr = 2;
			break;
		case 0xc6:
		case 0xdb:
			raw_hdr = 4;
			break;
		case 0xdc:
			return rd_be(p, end, 2, count) ? EL_LIST : EL_ERROR;
		case 0xdd:
			return rd_be(p, end, 4, count) ? EL_LIST : EL_ERROR;
		case 0xde:
			return rd_be(p, end, 2, count) ? EL_MAP : EL_ERROR;
		case 0xdf:
			return rd_be(p, end, 4, count) ? EL_MAP : EL_ERROR;
		case 0xd4:
		case 0xd5:
		case 0xd6:
		case 0xd7:
		case 0xd8:
			return skip_bytes(p, end, 1 + (1u << (b - 0xd4))) ? EL_EXT : EL_ERROR;
		case 0xc7:
			if (! rd_be(p, end, 1, &len)) {
				return EL_ERROR;
			}
			return skip_bytes(p, end, 1 + len) ? EL_EXT : EL_ERROR;
		case 0xc8:
			if (! rd_be(p, end, 2, &len)) {
				return EL_ERROR;
			}
			return skip_bytes(p, end, 1 + len) ? EL_EXT : EL_ERROR;
		case 0xc9:
			if (! rd_be(p, end, 4, &len)) {
				return EL_ERROR;
			}
			return skip_bytes(p, end, 1 + len) ? EL_EXT : EL_ERROR;
		default:
			return EL_ERROR;
		}

		if (! rd_be(p, end, raw_hdr, &len)) {
			return EL_ERROR;
		}
	}

	if ((uint64_t)(end - *p) < len) {
		return EL_ERROR;
	}

	*key_ok = len != 0 && ((*p)[0] == AS_STR_TYPE || (*p)[0] == AS_BLOB_TYPE);
	*p += len;

	return EL_SCALAR;
}

void
reference_walk(const uint8_t *buf, uint32_t sz, walk_result *res)
{
	const uint8_t *p = buf;
	const uint8_t *end = buf + sz;
	frame *stack = NULL;
	size_t sp = 0;
	size_t cap = 0;
	bool started = false;

	memset(res, 0, sizeof(*res));

	while (true) {
		while (sp > 0 && stack[sp - 1].remaining == 0) {
			sp--;
		}

		if (sp == 0 && started) {
			break;
		}

		started = true;

		bool is_key = sp > 0 && stack[sp - 1].is_map &&
				stack[sp - 1].remaining % 2 == 0;

		if (sp > 0) {
			stack[sp - 1].remaining--;
		}

		uint64_t count = 0;
		bool key_ok;
		el_kind kind = parse_el(&p, end, &count, &key_ok);

		if (kind == EL_ERROR) {
			res->corrupt = true;
			break;
		}

		if (kind != EL_LIST && kind != EL_MAP) {
			if (is_key && ! key_ok) {
				res->invalid_key = true;
			}

			continue;
		}

		if (is_key) {
			res->invalid_key = true;
		}

		uint32_t level = (uint32_t)sp + 1;

		if (level > res->strict_depth) {
			res->strict_depth = level;
		}

		if (count != 0 && level > res->server_depth) {
			res->server_depth = level;
		}

		if (count == 0) {
			continue;
		}

		if (sp == cap) {
			cap = cap == 0 ? 64 : cap * 2;
			stack = realloc(stack, cap * sizeof(frame));

			if (stack == NULL) {
				fprintf(stderr, "out of memory\n");
				exit(1);
			}
		}

		bool is_map = kind == EL_MAP;

		stack[sp].remaining = is_map ? 2 * count : count;
		stack[sp].is_map = is_map;
		sp++;

		bool marker = end - p >= 4 && *p == 0xc7 && p[1] == 1 && p[2] == 0xff &&
				p[3] <= 1;

		if (is_map && p < end && ! marker && (*p == 0xc7 || *p == 0xc8 ||
				*p == 0xc9 || (*p >= 0xd5 && *p <= 0xd8))) {
			if (parse_el(&p, end, &count, &key_ok) != EL_EXT) {
				res->corrupt = true;
				break;
			}

			kind = parse_el(&p, end, &count, &key_ok);

			if (kind != EL_SCALAR && kind != EL_EXT) {
				res->corrupt = true;
				break;
			}

			stack[sp - 1].remaining -= 2;
		}
	}

	if (! res->corrupt && p < end) {
		res->padding = true;
	}

	free(stack);
}

ref_class
ref_classify(const walk_result *res, uint32_t max_depth)
{
	if (res->server_depth > max_depth) {
		return REF_OVER;
	}

	return res->strict_depth > max_depth ? REF_STRICT_ONLY : REF_OK;
}
