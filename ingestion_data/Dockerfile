FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN pip install flask requests kafka-python redis

EXPOSE 5000

ENV FLASK_APP=ingestion.py

ENV FLASK_ENV=production

CMD ["python","ingestion.py"]
