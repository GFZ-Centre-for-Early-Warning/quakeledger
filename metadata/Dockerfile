FROM python:3.6.9-buster

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-sqlalchemy

WORKDIR /usr/share/git/quakeledger

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .
