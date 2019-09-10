FROM debian:stable-slim
MAINTAINER Andrey P

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget zip unzip \
    gnupg gnupg1 gnupg2 zlib1g-dev apt-utils lsb-release

RUN echo "deb http://repo.yandex.ru/yandex-disk/deb/ stable main" | \
tee -a /etc/apt/sources.list.d/yandex-disk.list > /dev/null && \
wget http://repo.yandex.ru/yandex-disk/YANDEX-DISK-KEY.GPG -O- | apt-key add - && \
apt-get update && \
apt-get install -y yandex-disk

WORKDIR /home/backups/

ARG password_config_path=/root/.config/yandex-disk/passwd
ARG data_path=/root/Yandex.Disk
ARG exclude_dirs

CMD yandex-disk -D -d "$data_path" -a "$password_config_path" --proxy=no --exclude-dirs=$exclude_dirs
