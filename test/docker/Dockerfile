FROM debian:13@sha256:f324c7ff54321e8d9c588493a20244965938ce0aa50bbd1022d38010e9ffc4b1

RUN apt-get update && apt-get install -y --no-install-recommends \
		build-essential ca-certificates gdb git libssl-dev libyaml-dev \
		linux-perf procps python3 time valgrind zlib1g-dev \
	&& rm -rf /var/lib/apt/lists/*

ARG CLIENT_VERSIONS="7.3.0 7.6.2"

RUN for v in ${CLIENT_VERSIONS}; do \
		git clone --quiet --depth 1 --branch "$v" --recurse-submodules \
			--shallow-submodules https://github.com/aerospike/aerospike-client-c.git \
			"/opt/client-$v" \
		&& make -C "/opt/client-$v" -j"$(nproc)" >/dev/null \
		|| exit 1; \
	done

ENV CLIENTREPO=/opt/client-7.6.2
WORKDIR /src
