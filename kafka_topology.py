from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import findspark

# Initialize Spark
findspark.init()

# Define schema for incoming JSON data
json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("user", StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True)
    ]), True),
    StructField("items", ArrayType(StructType([
        StructField("item_id", IntegerType(), True),
        StructField("price", IntegerType(), True)
    ])), True),
    StructField("timestamp", StringType(), True)
])

# Create Spark session
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("kafkastreams1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Static data for item details
items_data = [
    (1, "Cake"),
    (2, "Ice Cream"),
    (3, "Soda"),
    (4, "Chips"),
    (5, "Cookies")
]
items_df = spark.createDataFrame(items_data, ["item_id", "item_name"])

# Read data from Kafka source
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.7:9092") \
    .option("subscribe", "src_json") \
    .load()


# Parse JSON data from Kafka
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data")) \
    .select("data.*")

# Transform parsed data
parsed_df = parsed_df.select(
    col("id"),
    col("user.name").alias("user_name"),
    col("user.email").alias("user_email"),
    explode(col("items")).alias("item"),
    col("timestamp")
).select(
    col("id"),
    col("user_name"),
    col("user_email"),
    col("item.item_id").alias("item_id"),
    col("item.price").alias("item_price"),
    col("timestamp")
)

# Join with static item details
joined_stream = parsed_df.join(items_df, parsed_df.item_id == items_df.item_id, "left") \
    .select(
        parsed_df.id,
        parsed_df.user_name,
        parsed_df.user_email,
        parsed_df.item_id,
        items_df.item_name.alias("item_name"),
        parsed_df.item_price,
        parsed_df.timestamp
    )

# Convert joined data to JSON format
json_df = joined_stream.select(to_json(struct("*")).alias("value"))

# Write transformed data to Kafka
json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("topic", "downstream") \
    .option("checkpointLocation", "/tmp/kafka_output_checkpoint3") \
    .outputMode("append") \
    .start() \
    .awaitTermination()



# query = json_df.writeStream.format("console").outputMode("append").start().awaitTermination()