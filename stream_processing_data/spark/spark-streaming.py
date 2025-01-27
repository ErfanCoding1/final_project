from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, when, from_unixtime, lag, lit, window
from pyspark.sql.types import StructType, StructField, FloatType, LongType, StringType, TimestampType
from pyspark.sql.window import Window  # Import Window for partitioning and ordering
import requests

# Create Spark session
spark = SparkSession.builder \
    .appName("FinancialIndicatorsProcessing") \
    .master("spark://spark-master1.kafka.svc.cluster.local:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

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
    StructField("market_cap", FloatType(), True),
    StructField("pe_ratio", FloatType(), True),
    StructField("sentiment_score", FloatType(), True),
    StructField("sentiment_magnitude", FloatType(), True)
])

# Read raw data from Kafka
raw_data_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_input_topic) \
    .load()

# Parse and preprocess data
parsed_data_df = raw_data_df.selectExpr("CAST(value AS STRING) AS json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", from_unixtime(col("timestamp")).cast(TimestampType()))

# Aggregate calculations
window_duration = "5 minutes"
moving_avg_df = parsed_data_df.groupBy(
    window(col("timestamp"), window_duration),
    col("stock_symbol")
).agg(
    avg("closing_price").alias("moving_average")
)

# Function to calculate EMA
def calculate_ema(df, period):
    alpha = 2 / (period + 1)
    
    # First, initialize EMA with the closing price for the first row
    df = df.withColumn("ema", col("closing_price"))
    
    # Define the WindowSpec for partitioning and ordering
    window_spec = Window.partitionBy("stock_symbol").orderBy("timestamp")
    
    # Apply the EMA calculation using lag function
    df = df.withColumn("previous_ema", lag("ema", 1).over(window_spec))
    
    df = df.withColumn(
        "ema",
        when(col("previous_ema").isNull(), col("closing_price"))
        .otherwise((col("closing_price") - col("previous_ema")) * alpha + col("previous_ema"))
    )
    return df

# Calculate EMA
parsed_data_df = calculate_ema(parsed_data_df, 14)  # Example period for EMA calculation

# Calculate RSI
rsi_calculations_df = parsed_data_df.withColumn("change", col("closing_price") - lag("closing_price", 1).over(
    Window.partitionBy("stock_symbol").orderBy("timestamp")
))

rsi_calculations_df = rsi_calculations_df.withColumn("gain", when(col("change") > 0, col("change")).otherwise(0)) \
    .withColumn("loss", when(col("change") < 0, -col("change")).otherwise(0))

rsi_df = rsi_calculations_df.groupBy(
    window(col("timestamp"), window_duration),
    col("stock_symbol")
).agg(
    avg("gain").alias("avg_gain"),
    avg("loss").alias("avg_loss")
).withColumn("rs", col("avg_gain") / col("avg_loss")) \
    .withColumn("rsi", when(col("avg_loss") == 0, lit(100))
                .otherwise(100 - (100 / (1 + col("rs")))))

# Merge results
final_df = moving_avg_df.join(
    rsi_df,
    ["window", "stock_symbol"],
    "left"
).select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "stock_symbol", "moving_average", "rsi", "ema"
)
# Function to send data to signal generator
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
            print(f"Error sending data: {str(e)}")

# Write stream to external signal generator
query = final_df.writeStream \
    .foreachBatch(send_to_signal_generator) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/financial_indicators") \
    .start()

query.awaitTermination()
