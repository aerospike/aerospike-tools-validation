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

#pragma once

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Growable byte buffer for building msgpack test input.
typedef struct tbuf_s {
	uint8_t *buf;
	uint32_t sz;
	uint32_t cap;
} tbuf;

static inline void
tb_reserve(tbuf *tb, uint32_t extra)
{
	if (tb->sz + extra <= tb->cap) {
		return;
	}

	uint32_t cap = tb->cap == 0 ? 64 : tb->cap;

	while (cap < tb->sz + extra) {
		cap *= 2;
	}

	tb->buf = realloc(tb->buf, cap);

	if (tb->buf == NULL) {
		fprintf(stderr, "out of memory\n");
		exit(1);
	}

	tb->cap = cap;
}

static inline void
tb_u8(tbuf *tb, uint8_t b)
{
	tb_reserve(tb, 1);
	tb->buf[tb->sz++] = b;
}

static inline void
tb_raw(tbuf *tb, const void *p, uint32_t sz)
{
	tb_reserve(tb, sz);

	if (sz != 0) {
		memcpy(tb->buf + tb->sz, p, sz);
	}

	tb->sz += sz;
}

static inline void
tb_be(tbuf *tb, uint64_t v, uint32_t n_bytes)
{
	for (uint32_t i = 0; i < n_bytes; i++) {
		tb_u8(tb, (uint8_t)(v >> (8 * (n_bytes - 1 - i))));
	}
}

static inline void
tb_free(tbuf *tb)
{
	free(tb->buf);
	tb->buf = NULL;
	tb->sz = tb->cap = 0;
}
