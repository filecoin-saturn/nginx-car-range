FROM rust:1.67 as builder

WORKDIR /opt/nginx-car-range/

# basics
RUN apt update && apt install -y build-essential unzip llvm-dev libclang-dev clang libpcre3 libpcre3-dev zlib1g-dev

# nginx to build against. pinned @ 1.18 as distributed by saturn
RUN curl -LO https://nginx.org/download/nginx-1.18.0.tar.gz && mkdir /opt/nginx && tar -xf nginx-1.18.0.tar.gz --strip-components=1 -C /opt/nginx && ls /opt/nginx && rm nginx-1.18.0.tar.gz
RUN cd /opt/nginx && ./configure && make && make install && cd /opt/nginx-car-range/

# protobuf. pinned @ v3.22.1
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v22.1/protoc-22.1-linux-x86_64.zip && unzip protoc-22.1-linux-x86_64.zip -d /usr/local && rm protoc-22.1-linux-x86_64.zip

# build the plugin
COPY . .

RUN NGINX_DIR=/opt/nginx cargo build -v
