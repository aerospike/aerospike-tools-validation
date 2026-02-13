FROM ubuntu:24.10 AS install

ARG TARGETARCH

RUN \
  apt update -y \
  && apt install -y \
  wget \
  libreadline8

ARG TOOL_VERSION=2.1.1
ARG TOOL_ARTIFACT_URL_BASE="https://download.aerospike.com/artifacts/aerospike-tools-validation/${TOOL_VERSION}/asvalidation_${TOOL_VERSION}-1ubuntu24.04"

# Work from /aerospike
WORKDIR /aerospike
ENV PATH=/aerospike:$PATH

# Install Aerospike asvalidation tool
RUN \
  wget "${TOOL_ARTIFACT_URL_BASE}_${TARGETARCH}.deb" -O aerospike-asvalidation-tools.deb  \
  && dpkg -i /aerospike/aerospike-asvalidation-tools.deb \
  && rm -rf aerospike /var/lib/apt/lists/* /var/cache/apt/* /aerospike/aerospike-asvalidation-tools.deb

# Addition of wrapper script
ADD wrapper.sh /aerospike/wrapper

RUN chmod u+x /aerospike/wrapper

# Use a mount point validation for output
RUN mkdir -p /output 
RUN echo "Volume not mounted!" > /output/no-volume.txt

WORKDIR /output

# Wrapper script entrypoint
ENTRYPOINT ["wrapper"]