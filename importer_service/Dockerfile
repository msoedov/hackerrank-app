FROM python:3.5


RUN mkdir /app
COPY requirements.txt /app

WORKDIR /app

RUN pip install -r requirements.txt

COPY frosty_paper.py /app

ENTRYPOINT python frosty_paper.py

