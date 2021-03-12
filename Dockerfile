FROM python:3.8-slim-buster


COPY requirements.txt /requirements.txt
COPY dataset /dataset
COPY q1_data_source.json /q1_data_source.json

RUN pip3 install -r requirements.txt
