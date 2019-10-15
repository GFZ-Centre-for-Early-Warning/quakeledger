FROM python:3.6.9-buster

RUN apt-get update && apt-get install python3 python3-pip python3-sqlalchemy -y

WORKDIR /usr/share/git/quakeledger

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . .

RUN unzip sqlite3.db.zip && rm sqlite3.db.zip
