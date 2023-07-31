FROM rust:1.67 as builder

ARG NGINX_VERSION="1.23.3"

WORKDIR /opt/nginx-car-range/

# basics
RUN apt update && apt install -y build-essential git unzip llvm-dev libclang-dev clang libpcre3 libpcre3-dev zlib1g-dev

# nginx to build against. pinned @ 1.23 as distributed by saturn
RUN curl -LO https://nginx.org/download/nginx-${NGINX_VERSION}.tar.gz && mkdir /opt/nginx && tar -xf nginx-${NGINX_VERSION}.tar.gz --strip-components=1 -C /opt/nginx && ls /opt/nginx && rm nginx-${NGINX_VERSION}.tar.gz
RUN cd /opt/nginx && ./configure --prefix=/usr/local/nginx --with-debug && make && make install && cd /opt/nginx-car-range/

# protobuf. pinned @ v3.22.1
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v22.1/protoc-22.1-linux-x86_64.zip && unzip protoc-22.1-linux-x86_64.zip -d /usr/local && rm protoc-22.1-linux-x86_64.zip

# car pinned @2.8.0
RUN curl -LO https://github.com/ipld/go-car/releases/download/v2.8.0/go-car_2.8.0_linux_amd64.tar.gz && tar xzf go-car_2.8.0_linux_amd64.tar.gz

# download CAR fixture
RUN curl https://ipfs.io/ipfs/bafybeifpz6onienrgwvb3mw5rg7piq5jh63ystjn7s5wk6ttezy2gy5xwu/Mexico.JPG?format=car > /usr/local/nginx/html/midfixture.car \
  && curl https://saturn.ms/ipfs/QmafUYju2Ab4ETi5HJG1cqjmnjs2xw9PUuBKzU7Hi3zvXU/MC_TheSource.mp4?format=car > /usr/local/nginx/html/bigfixture.car \
  && curl https://saturn.ms/ipfs/bafybeieyd7lbyfjexjaqb6wvryaixjcmgnpljaca5umfwkbit72ozaotem?format=car > /usr/local/nginx/html/fixture3.car

# build the plugin
COPY . .

RUN cargo build -v --config net.git-fetch-with-cli=true && cargo build --release -v --config net.git-fetch-with-cli=true

FROM scratch as release
COPY --from=builder /opt/nginx-car-range/target/release/libnginx_car_range.so /libnginx_car_range.so

FROM buildpack-deps:bullseye-curl as ci

# put on a base nginx for config / etc.
RUN apt update && apt install -y nginx

COPY --from=builder /usr/local/nginx /usr/local/nginx
COPY --from=builder /opt/nginx-car-range/target/debug/libnginx_car_range.so /usr/local/lib/libnginx_car_range.so
COPY --from=builder /opt/nginx-car-range/car /usr/local/bin/car

COPY config/nginx.conf /etc/nginx/nginx.conf
COPY ci.sh /ci.sh
RUN chmod u+rwx /ci.sh
