FROM dlangchina/dlang-dmd

EXPOSE 1323

WORKDIR /usr/src/isuumo

RUN apt-get update \
    && apt-get install -y \
      wget \
      default-mysql-client \
      build-essential \
      ca-certificates \
      pkg-config \
      zlib1g-dev \
      libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY dub.sdl dub.selections.json ./
RUN mkdir source
COPY source ./source
RUN dub build -b release
