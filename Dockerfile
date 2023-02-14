FROM ubuntu:20.04 as builder

RUN apt-get -y update && \
    apt-get -y install curl && \
    apt-get -y install make && \
    apt-get -y install build-essential && \
    apt-get -y install wget

RUN wget --no-check-certificate -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -

RUN echo "deb http://apt.llvm.org/focal/ llvm-toolchain-focal main\n" >> /etc/apt/sources.list && \
    echo "deb-src http://apt.llvm.org/focal/ llvm-toolchain-focal main\n" >> /etc/apt/sources.list && \
    echo "deb http://apt.llvm.org/focal/ llvm-toolchain-focal-15 main\n" >> /etc/apt/sources.list && \
    echo "deb-src http://apt.llvm.org/focal/ llvm-toolchain-focal-15 main\n" >> /etc/apt/sources.list && \
    echo "deb http://apt.llvm.org/focal/ llvm-toolchain-focal-16 main\n" >> /etc/apt/sources.list && \
    echo "deb-src http://apt.llvm.org/focal/ llvm-toolchain-focal-16 main" >> /etc/apt/sources.list

RUN DEBIAN_FRONTEND=noninteractive apt install -y tzdata

# Install clang
RUN apt-get -y update && \
    apt-get -y install llvm && \
    apt-get -y install clang

# Install Rustup
RUN curl https://sh.rustup.rs -sSf | sh -s -- --no-modify-path --default-toolchain nightly -y
ENV PATH /root/.cargo/bin/:$PATH

# Install the Rust toolchain
WORKDIR /mapuche
COPY rust-toolchain ./
RUN rustup self update \
  && rustup set profile minimal \
  && rustup default $(cat "rust-toolchain")

COPY src ./src/
COPY Cargo* ./
COPY rust-toolchain ./
COPY Makefile ./

RUN make nightly-release

FROM ubuntu:20.04

COPY --from=builder /mapuche/target/release/mapuche-server /mapuche-server

EXPOSE 6380 18080

ENTRYPOINT ["/mapuche-server"]
