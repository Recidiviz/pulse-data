FROM ubuntu:latest

RUN apt update -y && \
    apt install -y \
        locales \
        git \
        libxml2-dev libxslt1-dev \
        python3.7-dev python3-pip \
        default-jre

RUN locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LANG en_US.UTF-8

ENV TZ America/New_York

RUN pip3 install pipenv

ADD . /app
WORKDIR /app

RUN pipenv sync

EXPOSE 8080
CMD pipenv run gunicorn -c gunicorn.conf.py -b :8080 recidiviz.server:app
