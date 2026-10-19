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

clean:
	rm -rf $(DIR_BIN)
