ifndef CLIENTREPO
$(error Please set the CLIENTREPO environment variable)
endif

PLATFORM := $(shell uname -s)-$(shell uname -m)

CC := cc
CFLAGS := -std=gnu99 -g -O2 -Wall -Wextra -Wconversion -Wsign-conversion \
		-D_GNU_SOURCE -pthread
INCLUDES := -isystem $(CLIENTREPO)/src/include \
		-isystem $(CLIENTREPO)/modules/common/src/include
LIBRARIES := $(CLIENTREPO)/target/$(PLATFORM)/lib/libaerospike.a \
		-lssl -lcrypto -lyaml -lpthread -lm -lz -ldl -lrt

DIR_BIN := bin

.PHONY: all clean

all: $(DIR_BIN)/gen_cdt $(DIR_BIN)/probe

$(DIR_BIN):
	mkdir -p $@

$(DIR_BIN)/gen_cdt: gen/gen_cdt.c gen/ref_walk.c gen/ref_walk.h | $(DIR_BIN)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ gen/gen_cdt.c gen/ref_walk.c $(LIBRARIES)

$(DIR_BIN)/probe: upgrade/probe.c gen/ref_walk.c gen/ref_walk.h | $(DIR_BIN)
	$(CC) $(CFLAGS) $(INCLUDES) -Igen -o $@ upgrade/probe.c gen/ref_walk.c \
		$(LIBRARIES)

# The replay harness builds the walker and old checks from a walker checkout:
#   make replay WALKER=/path/to/checkout
WALKER ?= /src
REPLAY_CFLAGS := -std=gnu99 -g -O2 -fno-common -fno-strict-aliasing \
		-D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -pthread
ifeq ($(shell uname -m), aarch64)
  REPLAY_CFLAGS += -mcpu=neoverse-n1
endif
ifeq ($(shell uname -m), x86_64)
  REPLAY_CFLAGS += -march=nocona
endif
REPLAY_SRC := bench/replay.c gen/ref_walk.c $(WALKER)/src/cdt_walk.c \
		$(WALKER)/src/cdt_check.c $(WALKER)/src/msgpack_in.c

.PHONY: replay

replay: $(DIR_BIN)/replay

$(DIR_BIN)/replay: $(REPLAY_SRC) | $(DIR_BIN)
	$(CC) $(REPLAY_CFLAGS) -I$(WALKER)/include -I$(WALKER)/src/toml -Igen \
		$(INCLUDES) -o $@ $(REPLAY_SRC) $(LIBRARIES)

clean:
	rm -rf $(DIR_BIN)
