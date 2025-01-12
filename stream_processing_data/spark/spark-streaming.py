from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, lag, exp, when, sum, udf, coalesce
from pyspark.sql.types import StructType, StringType, FloatType, LongType
from pyspark.sql.window import Window
import requests

spark = SparkSession.builder \
    .appName("FinancialIndicators") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

kafka_brokers = "kafka.kafka.svc.cluster.local:9092"
kafka_topic = "dataTopic"

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
    .add("order_type", StringType())\
    .add("price", FloatType()) \
    .add("quantity", LongType()) \
    .add("sentiment_score", FloatType()) \
    .add("sentiment_magnitude", FloatType()) \
    .add("indicator_name", StringType()) \
    .add("value", FloatType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

#Moving Avg
window_spec = Window.partitionBy("stock_symbol").orderBy("timestamp").rowsBetween(-4, 0)
moving_avg_df = parsed_df.withColumn("moving_avg", avg("price").over(window_spec))

#Exponential Moving Avg
alpha = 0.2
ema_df = moving_avg_df.withColumn(
    "ema",
    col("price") * alpha + 
    coalesce(lag("price", 1).over(window_spec), col("price")) * (1 - alpha)
)

#RSI
price_changes_df = parsed_df.withColumn("change", col("price") - lag("price", 1).over(window_spec))
rsi_df = price_changes_df.withColumn("gain", when(col("change") > 0, col("change")).otherwise(0)) \
    .withColumn("loss", when(col("change") < 0, -col("change")).otherwise(0)) \
    .withColumn("avg_gain", avg("gain").over(window_spec)) \
    .withColumn("avg_loss", avg("loss").over(window_spec)) \
    .withColumn("rs", when(col("avg_loss") == 0, 0).otherwise(col("avg_gain") / col("avg_loss"))) \
    .withColumn("rsi", 100 - (100 / (1 + col("rs"))))

moving_avg_query = moving_avg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

ema_query = ema_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

price_query = price_changes_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

moving_avg_query.awaitTermination()
ema_query.awaitTermination()
price_query.awaitTermination()

