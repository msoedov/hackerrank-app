FROM python:3.5


RUN mkdir /app
COPY requirements.txt /app

WORKDIR /app

RUN pip install -r requirements.txt

COPY black_mountain.py /app

ENTRYPOINT python black_mountain.py
