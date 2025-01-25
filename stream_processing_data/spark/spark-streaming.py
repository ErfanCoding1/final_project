from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, when, coalesce, window, lag, from_unixtime, expr
from pyspark.sql.types import StructType, StructField, FloatType, LongType, StringType
from pyspark.sql.window import Window
import requests

# Create Spark session
spark = SparkSession.builder \
    .appName("FinancialIndicators") \
    .master("spark://spark-master1.kafka.svc.cluster.local:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

# Kafka configuration
kafka_brokers = "kafka.kafka.svc.cluster.local:9092"
kafka_input_topic = "dataTopic"

# Define schema for incoming data
schema = StructType([
    StructField("stock_symbol", StringType(), True),
    StructField("opening_price", FloatType(), True),
    StructField("closing_price", FloatType(), True),
    StructField("high", FloatType(), True),
    StructField("low", FloatType(), True),
    StructField("volume", LongType(), True),
    StructField("timestamp", FloatType(), True),  # Unix timestamp as Float
    StructField("data_type", StringType(), True),
    StructField("market_cap", FloatType(), True),
    StructField("pe_ratio", FloatType(), True),
    StructField("order_type", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", LongType(), True),
    StructField("sentiment_score", FloatType(), True),
    StructField("sentiment_magnitude", FloatType(), True),
    StructField("indicator_name", StringType(), True),
    StructField("value", FloatType(), True)
])

# Read raw data from Kafka
raw_data_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_input_topic) \
    .load()

# Parse and convert timestamp
parsed_data_df = raw_data_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast("timestamp"))

# Window specifications
time_window = window("timestamp", "10 minutes").alias("window")
stock_window = Window.partitionBy("stock_symbol").orderBy("timestamp")

# Moving Average Calculation
moving_avg_df = parsed_data_df \
    .groupBy("stock_symbol", time_window) \
    .agg(avg("price").alias("moving_avg")) \
    .withColumn("timestamp", col("window.start"))

# EMA Calculation
ema_df = moving_avg_df.withColumn(
    "ema",
    coalesce(
        (col("moving_avg") * 0.2) + (lag(col("moving_avg"), 1).over(stock_window) * 0.8),
        col("moving_avg")
    )
)

# RSI Calculation
rsi_calc_df = parsed_data_df.withColumn(
    "price_change", 
    col("price") - coalesce(lag("price", 1).over(stock_window), col("price"))
).withColumn(
    "gain", 
    when(col("price_change") > 0, col("price_change")).otherwise(0)
).withColumn(
    "loss", 
    when(col("price_change") < 0, -col("price_change")).otherwise(0)
)

rsi_window = Window.partitionBy("stock_symbol").orderBy("timestamp").rowsBetween(-14, 0)

rsi_df = rsi_calc_df \
    .withColumn("avg_gain", avg("gain").over(rsi_window)) \
    .withColumn("avg_loss", avg("loss").over(rsi_window)) \
    .withColumn("rs", when(col("avg_loss") == 0, 0).otherwise(col("avg_gain")/col("avg_loss"))) \
    .withColumn("rsi", 100 - (100 / (1 + col("rs"))))

# Join all indicators
final_df = rsi_df.join(
    moving_avg_df, 
    ["stock_symbol", "timestamp"], 
    "left"
).join(
    ema_df.select("stock_symbol", "timestamp", "ema"), 
    ["stock_symbol", "timestamp"], 
    "left"
)

# Final selection
indicators_df = final_df.select(
    "stock_symbol", "opening_price", "closing_price", "high", "low", "volume",
    "timestamp", "market_cap", "pe_ratio", "order_type", "price", "quantity",
    "sentiment_score", "sentiment_magnitude", "indicator_name", "value",
    "moving_avg", "ema", "rsi"
)

# Send to signal generator
def send_to_signal_generator(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    for row in batch_df.collect():
        data = row.asDict()
        try:
            response = requests.post(
                "http://signal-generator:5000/process_indicators",
                json=data,
                timeout=10
            )
            print(f"Sent data: {response.status_code}")
        except Exception as e:
            print(f"Error: {str(e)}")

# Start streaming
query = indicators_df.writeStream \
    .foreachBatch(send_to_signal_generator) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()




