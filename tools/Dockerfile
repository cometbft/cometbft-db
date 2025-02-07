# This file defines the container image used to build and test tm-db in CI.
# The CI workflows use the latest tag of cometbft/cometbft-db-testing built
# from these settings.
#
# The jobs defined in the Build & Push workflow will build and update the image
# when changes to this file are merged.  If you have other changes that require
# updates here, merge the changes here first and let the image get updated (or
# push a new version manually) before PRs that depend on them.

# PLEASE BUMP THE VERSION OF THE IMAGE IN `.github/workflows/ci.yml` WHEN YOU
# MODIFY THIS FILE.

FROM golang:1.23.5 AS build

ENV LD_LIBRARY_PATH=/usr/local/lib

RUN apt update \
    && apt install -y \
       libgflags-dev libsnappy-dev libzstd-dev zlib1g-dev liblz4-dev \
       make tar wget build-essential g++ cmake \
       libleveldb-dev libleveldb1d

ARG ROCKSDB=9.8.4

# Install RocksDB
RUN \
  wget -q https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB}.tar.gz \
  && tar -zxf v${ROCKSDB}.tar.gz \
  && cd rocksdb-${ROCKSDB} \
  && DEBUG_LEVEL=0 PORTABLE=1 make -j$(nproc) static_lib \
  && make install \
  && cd .. \
  && rm -rf v${ROCKSDB}.tar.gz rocksdb-${ROCKSDB}
