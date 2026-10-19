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

#include <stdbool.h>
#include <stdint.h>

#define AS_STR_TYPE 3
#define AS_BLOB_TYPE 4

typedef struct walk_result_s {
	uint32_t server_depth;
	uint32_t strict_depth;
	bool invalid_key;
	bool corrupt;
	bool padding;
} walk_result;

typedef enum {
	REF_OK,
	REF_STRICT_ONLY,
	REF_OVER,
	N_REF_CLASSES
} ref_class;

extern const char *ref_class_names[N_REF_CLASSES];

void reference_walk(const uint8_t *buf, uint32_t sz, walk_result *res);
ref_class ref_classify(const walk_result *res, uint32_t max_depth);
