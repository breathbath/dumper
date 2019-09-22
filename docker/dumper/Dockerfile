# -------------
# Build stage

FROM golang:1.12 AS build-env

RUN mkdir -p /build/dumper

WORKDIR /build/dumper
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .

ARG version=unknown

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo \
        -o /build/dumper/dumper \
        -ldflags \
        "-X github.com/breathbath/dumper/cmd.Version=$version" \
        main.go

# -------------
# Image creation stage

FROM debian:stable-slim
MAINTAINER Andrey P

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget gzip \
    gnupg gnupg1 gnupg2 zlib1g-dev apt-utils lsb-release

RUN wget -c https://dev.mysql.com/get/mysql-apt-config_0.8.12-1_all.deb --no-check-certificate && \
dpkg -i mysql-apt-config_0.8.12-1_all.deb && \
apt-get update && \
apt-get install -y mysql-client

WORKDIR /app
COPY --from=build-env /build/dumper/dumper /app/
COPY config/demo.json /app/config.json
COPY env.dist /app/.env
COPY ./scripts /app/scripts

CMD /app/dumper dump
