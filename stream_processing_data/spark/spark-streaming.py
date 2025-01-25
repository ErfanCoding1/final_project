from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, lag, when, coalesce
from pyspark.sql.types import StructType, StringType, FloatType, LongType
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
schema = StructType() \
    .add("stock_symbol", StringType()) \
    .add("opening_price", FloatType()) \
    .add("closing_price", FloatType()) \
    .add("high", FloatType()) \
    .add("low", FloatType()) \
    .add("volume", LongType()) \
    .add("timestamp", FloatType()) \
    .add("data_type", StringType()) \
    .add("market_cap", FloatType()) \
    .add("pe_ratio", FloatType()) \
    .add("order_type", StringType()) \
    .add("price", FloatType()) \
    .add("quantity", LongType()) \
    .add("sentiment_score", FloatType()) \
    .add("sentiment_magnitude", FloatType()) \
    .add("indicator_name", StringType()) \
    .add("value", FloatType())

# Read raw data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_input_topic) \
    .load()

# Parse the data and apply schema
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Compute indicators
window_spec = Window.partitionBy("stock_symbol").orderBy("timestamp").rowsBetween(-4, 0)

# Moving Average
moving_avg_df = parsed_df.withColumn("moving_avg", avg("price").over(window_spec))

# Exponential Moving Average (EMA)
alpha = 0.2
ema_df = moving_avg_df.withColumn(
    "ema",
    col("price") * alpha + 
    coalesce(lag("price", 1).over(window_spec), col("price")) * (1 - alpha)
)

# Relative Strength Index (RSI)
price_changes_df = ema_df.withColumn("change", col("price") - lag("price", 1).over(window_spec))
rsi_df = price_changes_df.withColumn("gain", when(col("change") > 0, col("change")).otherwise(0)) \
    .withColumn("loss", when(col("change") < 0, -col("change")).otherwise(0)) \
    .withColumn("avg_gain", avg("gain").over(window_spec)) \
    .withColumn("avg_loss", avg("loss").over(window_spec)) \
    .withColumn("rs", when(col("avg_loss") == 0, 0).otherwise(col("avg_gain") / col("avg_loss"))) \
    .withColumn("rsi", 100 - (100 / (1 + col("rs"))))

# Combine all indicators into one DataFrame
indicators_df = rsi_df.select(
    "stock_symbol", "opening_price", "closing_price", "high", "low", "volume", 
    "timestamp", "market_cap", "pe_ratio", "order_type", "price", "quantity",
    "sentiment_score", "sentiment_magnitude", "indicator_name", "value", 
    "moving_avg", "ema", "rsi"
)

# Function to send data to signal-generator.py
def send_to_signal_generator(batch_df, batch_id):
    # Convert DataFrame rows to JSON and send to signal-generator.py
    for row in batch_df.collect():
        data = {
            "stock_symbol": row["stock_symbol"],
            "opening_price": row["opening_price"],
            "closing_price": row["closing_price"],
            "high": row["high"],
            "low": row["low"],
            "volume": row["volume"],
            "timestamp": row["timestamp"],
            "market_cap": row["market_cap"],
            "pe_ratio": row["pe_ratio"],
            "order_type": row["order_type"],
            "price": row["price"],
            "quantity": row["quantity"],
            "sentiment_score": row["sentiment_score"],
            "sentiment_magnitude": row["sentiment_magnitude"],
            "indicator_name": row["indicator_name"],
            "value": row["value"],
            "moving_avg": row["moving_avg"],
            "ema": row["ema"],
            "rsi": row["rsi"]
        }
        try:
            response = requests.post("http://signal-generator:5000/process_indicators", json=data)
            print(f"Sent data to signal-generator: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"Failed to send data to signal-generator: {e}")

# Use foreachBatch to process and send indicators
query = indicators_df.writeStream \
    .foreachBatch(send_to_signal_generator) \
    .outputMode("update") \
    .start()

query.awaitTermination()

