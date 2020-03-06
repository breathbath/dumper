FROM mysql:latest

RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/* \
&& echo "LC_ALL=en_US.UTF-8" >> /etc/environment \
&& echo "en_US.UTF-8 UTF-8" >> /etc/locale.gen \
&& echo "LANG=en_US.UTF-8" > /etc/locale.conf \
&& locale-gen en_US.UTF-8

ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8
