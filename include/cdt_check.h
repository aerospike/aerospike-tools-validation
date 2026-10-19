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

#pragma once

//==========================================================
// Includes.
//

#include <stdbool.h>
#include <stdint.h>

#include <backup.h>


//==========================================================
// Typedefs & constants.
//

typedef struct cdt_fix_s {
	const uint8_t *contents;
	uint32_t content_sz;
	uint32_t ele_count;
	uint32_t nf_padding;

	bool nf_list_order;

	bool nf_map_order;
	bool nf_map_dupkey;

	bool need_log;
} cdt_fix;


//==========================================================
// Public API.
//

// Order, duplicate-key, padding and corruption checks of one list/map bin.
// Map keys and depth are checked by cdt_walk().
bool cdt_need_fix(const uint8_t *buf, uint32_t sz, cdt_fix *cf,
		backup_config *bc);
