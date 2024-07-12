from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Define the schema of the incoming data
stock_schema = StructType([
    StructField('symbol', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('timestamp', TimestampType(), True)
])

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("StockPriceAnalysis") \
    .getOrCreate()

# Read messages from Kafka
raw_stock_data = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", 'kafka:9092') \
    .option("subscribe", 'stock-prices') \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data and apply the schema
parsed_stock_data = raw_stock_data \
    .select(from_json(col("value").cast("string"), stock_schema).alias("data")) \
    .select("data.*")

# Perform aggregation to calculate the average price of each stock in a 1-minute window
average_stock_price = parsed_stock_data \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("symbol")
    ) \
    .agg(avg("price").alias("average_price"))

# Output the results to the console (for demonstration purposes)
# In a production environment, you might write them to a distributed storage system
query = average_stock_price \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(Trigger.ProcessingTime("1 minute")) \
    .start()

query.awaitTermination()
