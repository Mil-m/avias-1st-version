FROM python:3.6.8

RUN mkdir -p /app

COPY ./requirements.txt /app/
RUN pip3 install --upgrade pip
RUN pip3 install -r /app/requirements.txt

COPY ./avias_client /app/avias_client
COPY ./avias_api /app/avias_api

ENV PYTHONPATH "/app"

WORKDIR /app/
