#
# Aerospike Backup/Restore
#
# Copyright (c) 2008-2026 Aerospike, Inc. All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

ifndef CLIENTREPO
$(error Please set the CLIENTREPO environment variable)
endif

OS := $(shell uname -s)
ARCH := $(shell uname -m)
PLATFORM := $(OS)-$(ARCH)

CC := cc

DWARF := $(shell $(CC) -Wall -Wextra -O2 -o /tmp/asflags_$${$$} src/flags.c; \
		/tmp/asflags_$${$$}; rm /tmp/asflags_$${$$})
CFLAGS := -std=gnu99 $(DWARF) -O2 -fno-common -fno-strict-aliasing \
		-Wall -Wextra -Wconversion -Wsign-conversion -Wmissing-declarations \
		-D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -D_FORTIFY_SOURCE=2 -DMARCH_$(ARCH)

ifeq ($(OS), Linux)
  CFLAGS += -pthread -fstack-protector -Wa,--noexecstack
endif

ifeq ($(ARCH), x86_64)
  CFLAGS += -march=nocona
endif

ifeq ($(ARCH), aarch64)
  CFLAGS += -mcpu=neoverse-n1

  ifeq ($(OS), ubuntu18.04)
    CC =gcc-9
  endif
endif

LD := $(CC)
LDFLAGS := $(CFLAGS)

DIR_INC := include
DIR_SRC := src
DIR_OBJ := obj
DIR_BIN := bin
DIR_DOCS := docs
DIR_ENV := env
DIR_TOML := src/toml

INCLUDES := -I$(DIR_INC)
INCLUDES += -I$(DIR_TOML)
INCLUDES += -I$(CLIENTREPO)/src/include
INCLUDES += -I$(CLIENTREPO)/modules/common/src/include
INCLUDES += -I/usr/local/opt/openssl/include

LIBRARIES := $(CLIENTREPO)/target/$(PLATFORM)/lib/libaerospike.a
LIBRARIES += -L/usr/local/lib
ifeq ($(OPENSSL_STATIC_PATH),)
  OPENSSL_LIBRARIES := -L/usr/local/opt/openssl/lib -lssl -lcrypto
else
  OPENSSL_LIBRARIES := $(OPENSSL_STATIC_PATH)/libssl.a $(OPENSSL_STATIC_PATH)/libcrypto.a
endif
LIBRARIES += $(OPENSSL_LIBRARIES)
LIBRARIES += -lyaml
LIBRARIES += -lpthread
LIBRARIES += -lm
LIBRARIES += -lz

ifeq ($(OS), Linux)
LIBRARIES += -ldl -lrt
LIBRARIES += -L$(DIR_TOML) -Wl,-l,:libtoml.a
else
LIBRARIES += $(DIR_TOML)/libtoml.a
endif

src_to_obj = $(1:$(DIR_SRC)/%.c=$(DIR_OBJ)/%.o)
obj_to_dep = $(1:%.o=%.d)
src_to_lib =

BACKUP_INC := $(DIR_INC)/backup.h $(DIR_INC)/cdt_check.h $(DIR_INC)/cdt_walk.h $(DIR_INC)/enc_text.h $(DIR_INC)/shared.h $(DIR_INC)/utils.h $(DIR_INC)/msgpack_in.h $(DIR_INC)/version.h
BACKUP_SRC := $(DIR_SRC)/backup.c $(DIR_SRC)/cdt_check.c $(DIR_SRC)/cdt_walk.c $(DIR_SRC)/conf.c $(DIR_SRC)/utils.c $(DIR_SRC)/enc_text.c $(DIR_SRC)/msgpack_in.c
BACKUP_OBJ := $(call src_to_obj, $(BACKUP_SRC))
BACKUP_DEP := $(call obj_to_dep, $(BACKUP_OBJ))

BACKUP := $(DIR_BIN)/asvalidation
TOML := $(DIR_TOML)/libtoml.a

INCS := $(BACKUP_INC)
SRCS := $(BACKUP_SRC)
OBJS := $(BACKUP_OBJ)
DEPS := $(BACKUP_DEP)
BINS := $(TOML) $(BACKUP)

# sort removes duplicates
INCS := $(sort $(INCS))
SRCS := $(sort $(SRCS))
OBJS := $(sort $(OBJS))
DEPS := $(sort $(DEPS))

DIR_TEST := test/unit
DIR_TEST_BIN := $(DIR_BIN)/test

TEST_CFLAGS := -std=gnu99 -g -O1 -fno-omit-frame-pointer \
		-fsanitize=address,undefined -fno-sanitize-recover=all \
		-Wall -Wextra -Wconversion -Wsign-conversion -Wmissing-declarations \
		-D_GNU_SOURCE -pthread

TEST_INCLUDES := -I$(DIR_INC) -I$(DIR_TOML) \
		-isystem $(CLIENTREPO)/src/include \
		-isystem $(CLIENTREPO)/modules/common/src/include

TEST_LIBRARIES := $(CLIENTREPO)/target/$(PLATFORM)/lib/libaerospike.a \
		-L/usr/local/lib $(OPENSSL_LIBRARIES) -lyaml -lpthread -lm -lz
ifeq ($(OS), Linux)
  TEST_LIBRARIES += -ldl -lrt
endif

TESTS := $(DIR_TEST_BIN)/test_cdt_walk $(DIR_TEST_BIN)/test_cdt_diff

.PHONY: all clean ragel test

all: $(BINS)

clean:
	$(MAKE) -C $(DIR_TOML) clean
	rm -f $(DEPS) $(OBJS) $(BINS)
	rm -rf $(DIR_TEST_BIN)
	if [ -d $(DIR_OBJ) ]; then rmdir $(DIR_OBJ); fi
	if [ -d $(DIR_BIN) ]; then rmdir $(DIR_BIN); fi
	if [ -d $(DIR_DOCS) ]; then rm -r $(DIR_DOCS); fi
	if [ -d $(DIR_ENV) ]; then rm -r $(DIR_ENV); fi

ragel:
	ragel $(DIR_SRC)/spec.rl

.PHONY: rpm
rpm:
	$(MAKE) -f pkg/Makefile.rpm

.PHONY: deb
deb:
	$(MAKE) -f pkg/Makefile.deb

$(DIR_DOCS): $(INCS) $(SRCS) README.md
	if [ ! -d $(DIR_DOCS) ]; then mkdir $(DIR_DOCS); fi
	doxygen doxyfile

$(DIR_OBJ):
	mkdir $(DIR_OBJ)

$(DIR_BIN):
	mkdir $(DIR_BIN)

$(DIR_OBJ)/%.o: $(DIR_SRC)/%.c | $(DIR_OBJ)
	$(CC) $(CFLAGS) -MMD -o $@ -c $(INCLUDES) $<

test: $(TESTS)
	@for t in $(TESTS); do $$t || exit 1; done

$(DIR_TEST_BIN):
	mkdir -p $(DIR_TEST_BIN)

$(DIR_TEST_BIN)/test_cdt_walk: $(DIR_TEST)/test_cdt_walk.c $(DIR_TEST)/test_buf.h \
		$(DIR_SRC)/cdt_walk.c $(DIR_INC)/cdt_walk.h | $(DIR_TEST_BIN)
	$(CC) $(TEST_CFLAGS) -I$(DIR_INC) -o $@ $(DIR_TEST)/test_cdt_walk.c \
		$(DIR_SRC)/cdt_walk.c

$(DIR_TEST_BIN)/test_cdt_diff: $(DIR_TEST)/test_cdt_diff.c $(DIR_TEST)/test_buf.h \
		$(DIR_SRC)/cdt_walk.c $(DIR_SRC)/cdt_check.c $(DIR_SRC)/msgpack_in.c \
		$(INCS) | $(DIR_TEST_BIN)
	$(CC) $(TEST_CFLAGS) -fno-sanitize=alignment $(TEST_INCLUDES) -o $@ \
		$(DIR_TEST)/test_cdt_diff.c \
		$(DIR_SRC)/cdt_walk.c $(DIR_SRC)/cdt_check.c $(DIR_SRC)/msgpack_in.c \
		$(TEST_LIBRARIES)

$(BACKUP): $(BACKUP_OBJ) | $(DIR_BIN)
	$(CC) $(LDFLAGS) -o $(BACKUP) $(BACKUP_OBJ) $(LIBRARIES)

$(TOML):
	$(MAKE) -C $(DIR_TOML)

-include $(BACKUP_DEP)
-include $(RESTORE_DEP)

