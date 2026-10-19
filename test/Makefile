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

all: $(DIR_BIN)/gen_cdt

$(DIR_BIN):
	mkdir -p $@

$(DIR_BIN)/gen_cdt: gen/gen_cdt.c | $(DIR_BIN)
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBRARIES)

clean:
	rm -rf $(DIR_BIN)
