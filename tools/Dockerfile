FROM golang:1.23

RUN apt update \
    && apt install -y \
       libbz2-dev libgflags-dev libsnappy-dev libzstd-dev zlib1g-dev liblz4-dev \
       make tar wget build-essential g++ cmake \
       libleveldb-dev libleveldb1d

ARG ROCKSDB=9.8.4

# Install RocksDB
# GCC 12 reports false-positive uninitialized warnings from its AVX-512
# intrinsic headers while compiling RocksDB's bundled xxHash implementation.
RUN \
  wget -q https://github.com/facebook/rocksdb/archive/refs/tags/v${ROCKSDB}.tar.gz \
  && tar -zxf v${ROCKSDB}.tar.gz \
  && cd rocksdb-${ROCKSDB} \
  && DEBUG_LEVEL=0 CXXFLAGS="-Wno-maybe-uninitialized -Wno-error=uninitialized" make -j4 shared_lib \
  && make install-shared \
  && ldconfig \
  && cd .. \
  && rm -rf v${ROCKSDB}.tar.gz rocksdb-${ROCKSDB}
