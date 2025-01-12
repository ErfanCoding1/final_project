FROM bitnami/spark:latest


ENV SPARK_MODE=master
ENV SPARK_MASTER_URL=spark://spark-master.kafka.svc.cluster.local:7077

EXPOSE 7077 8080 8081


CMD if [ "$SPARK_MODE" = "master" ]; then \
        echo "Starting Spark Master..."; \
        start-spark master; \
    else \
        echo "Starting Spark Worker..."; \
        start-spark worker --master $SPARK_MASTER_URL; \
    fi
