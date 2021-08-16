FROM dlang2/ldc2-ubuntu:1.26.0

EXPOSE 1323

WORKDIR /usr/src/isuumo

ENV DOCKERIZE_VERSION v0.6.1
RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz && \
    rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz

COPY dub.sdl dub.selection.json ./
RUN mdkir source
COPY source ./source
RUN touch source/app.d && dub build -b release

RUN apt-get update && \
    apt-get install -y wget default-mysql-client && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

CMD ["/usr/src/isuumo/isuumo"]
