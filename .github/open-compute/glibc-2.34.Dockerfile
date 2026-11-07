# Linux release builder for the open-compute glibc 2.34 baseline (RHEL/Rocky/Alma 9).
# manylinux_2_34 provides AlmaLinux 9 glibc; kernel.org supplies
# LLVM 22, and libc++/libc++abi/libunwind are rebuilt here so the static C++
# runtime links only glibc 2.34 symbols. The system GCC 11 supplies crt objects and
# libgcc: gcc-toolset-14's libgcc_eh calls _dl_find_object, which AlmaLinux 9 backports
# as GLIBC_2.35 and upstream glibc 2.34 does not provide.
# Remove when upstream release_linux builds target a glibc 2.34 sysroot.
ARG MANYLINUX=quay.io/pypa/manylinux_2_34_x86_64@sha256:b9bb1e7780cebc4a8375c032428411ee1a78c4cd0b8edd1313983166dd3d6cf1
FROM ${MANYLINUX}
ARG LLVM_ARCH
ARG LLVM_SHA256
ARG BAZELISK_ARCH
ARG BAZELISK_SHA256

ENV PATH=/opt/python/cp313-cp313/bin:/opt/llvm/bin:$PATH
RUN dnf install -y gcc git tar gzip xz unzip zip patch which perl-Digest-SHA libxml2 libzstd zlib-devel \
    && dnf clean all \
    && python -m pip install --no-cache-dir cmake==3.31.6 ninja==1.13.0
RUN mkdir -p /opt/llvm /build \
    && curl -fsSL "https://mirrors.edge.kernel.org/pub/tools/llvm/files/llvm-22.1.8-${LLVM_ARCH}.tar.xz" -o /build/clang.tar.xz \
    && echo "${LLVM_SHA256}  /build/clang.tar.xz" | sha256sum -c - \
    && tar -xJf /build/clang.tar.xz --strip-components=1 -C /opt/llvm \
    && for driver in clang clang++; do \
         echo '--gcc-toolchain=/usr' > "/opt/llvm/bin/$driver.cfg"; \
       done \
    && curl -fsSL https://github.com/llvm/llvm-project/releases/download/llvmorg-22.1.8/llvm-project-22.1.8.src.tar.xz -o /build/llvm.tar.xz \
    && echo '922f1817a0df7b1489272d18134ee0087a8b068828f87ac63b9861b1a9965888  /build/llvm.tar.xz' | sha256sum -c - \
    && tar -xJf /build/llvm.tar.xz -C /build \
    && cmake -G Ninja -S /build/llvm-project-22.1.8.src/runtimes -B /build/runtimes \
         -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
         -DCMAKE_C_FLAGS=--gcc-toolchain=/usr \
         -DCMAKE_CXX_FLAGS=--gcc-toolchain=/usr \
         -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
         -DCMAKE_INSTALL_PREFIX=/opt/llvm -DCMAKE_INSTALL_LIBDIR=lib \
         -DLLVM_ENABLE_PER_TARGET_RUNTIME_DIR=OFF \
         '-DLLVM_ENABLE_RUNTIMES=libcxx;libcxxabi;libunwind' \
         -DLIBCXX_ENABLE_SHARED=OFF -DLIBCXXABI_ENABLE_SHARED=OFF -DLIBUNWIND_ENABLE_SHARED=OFF \
         -DLIBCXXABI_USE_LLVM_UNWINDER=ON \
         -DLIBCXX_STATICALLY_LINK_ABI_IN_STATIC_LIBRARY=ON \
         -DLIBCXXABI_STATICALLY_LINK_UNWINDER_IN_STATIC_LIBRARY=ON \
    && cmake --build /build/runtimes \
    && cmake --install /build/runtimes \
    && rm -rf /build
RUN curl -fsSL "https://github.com/bazelbuild/bazelisk/releases/download/v1.27.0/bazelisk-linux-${BAZELISK_ARCH}" -o /usr/local/bin/bazel \
    && echo "${BAZELISK_SHA256}  /usr/local/bin/bazel" | sha256sum -c - \
    && chmod 0755 /usr/local/bin/bazel \
    && test "$(getconf GNU_LIBC_VERSION)" = 'glibc 2.34'
