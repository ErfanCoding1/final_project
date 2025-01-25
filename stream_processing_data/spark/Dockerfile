FROM openjdk:11-slim AS builder

# Install Python, pip, and minimal dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip procps -qq && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Install Python dependencies manually using pip
RUN pip3 install --no-cache-dir \
    pyspark \
    requests

# Copy the Spark Streaming script into the container
COPY spark-streaming.py /app/spark-streaming.py

# Set environment variables for Java and Spark
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_HOME=/usr/local/lib/python3.9/dist-packages/pyspark
ENV PYTHONPATH=/usr/local/lib/python3.9/dist-packages/pyspark/python:/usr/local/lib/python3.9/dist-packages/pyspark/python/lib/py4j-0.10.9.5-src.zip

# Command to run the Spark Streaming application
#CMD ["python3", "spark-streaming.py"]
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3", "--master", "local[*]", "/app/spark-streaming.py"]
