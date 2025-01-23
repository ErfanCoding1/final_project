FROM python:3.9-slim

WORKDIR /app

COPY spark-streaming.py /app/spark-streaming.py

RUN pip install pyspark requests


ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

CMD ["python", "spark-streaming.py"]
