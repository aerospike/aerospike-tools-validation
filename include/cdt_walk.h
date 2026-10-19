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

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>


//==========================================================
// Typedefs & constants.
//

#define CDT_WALK_MAX_DEPTH 255

// Server 8.2 rejects a non-empty list/map nested deeper than its limit.
#define CDT_WALK_OVER 0x01u
// Only an empty list/map is too deep: the server accepts it, the shared msgpack
// deserializer (client 7.6+, UDFs) does not.
#define CDT_WALK_EMPTY_OVER 0x02u
// A map key that is not an integer, string or blob.
#define CDT_WALK_INVALID_KEY 0x04u
#define CDT_WALK_CORRUPT 0x08u

typedef struct cdt_walk_result_s {
	uint32_t flags;
	uint32_t depth; // deepest list/map level reached, at most max_depth + 1
	uint32_t sz; // bytes in the top-level value, set when the walk completes
} cdt_walk_result;


//==========================================================
// Public API.
//

// Walks one msgpack value without recursion. OVER and CORRUPT stop the walk;
// EMPTY_OVER and INVALID_KEY do not. Bytes after the value are not examined.
void cdt_walk(const uint8_t *buf, uint32_t buf_sz, uint32_t max_depth,
		bool check_keys, cdt_walk_result *res);
