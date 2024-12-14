from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
import pyspark.sql.functions as F

# Kafka Bootstrap Servers Configuration
bootstrap_servers = "pkc-e0zxq.eu-west-3.aws.confluent.cloud:9092"
kafka_sasl_config = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    "username='QAW37UQ37UHQI2GS' password='EAVg1yXjOqFD+tUUtv0Wr3j5vU1zkYO7Ntfzx8DnDq2gICeVaBqJQ5n20ZFykSp0';"
)

# Define User Schema
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True)  # Correction: StringType au lieu de IntegerType
])

# Define Transaction Schema
transaction_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("item_name", StringType(), True),
    StructField("item_type", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TD5_Users_Stream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

# Read User Stream from Kafka
user_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", kafka_sasl_config) \
    .option("subscribe", "TD5_users") \
    .option("startingOffsets", "earliest") \
    .load()

user_df = user_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .withColumn("data", F.from_json(F.col("json_data"), user_schema)) \
    .select("data.*")

user_df.writeStream \
    .format("memory") \
    .queryName("user_stream") \
    .outputMode("append") \
    .start()

# Read Transaction Stream from Kafka
transaction_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", kafka_sasl_config) \
    .option("subscribe", "TD5_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

transaction_df = transaction_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .withColumn("data", F.from_json(F.col("json_data"), transaction_schema)) \
    .select("data.*")

transaction_df.writeStream \
    .format("memory") \
    .queryName("transaction_stream") \
    .outputMode("append") \
    .start()

# Process Queries
spark.streams.awaitAnyTermination()

# Query 1: Proportion of Users Under 30
users_under_30 = spark.sql("""
    SELECT COUNT(DISTINCT t.user_id) AS under_30_buyers
    FROM transaction_stream t
    INNER JOIN user_stream u ON t.user_id = u.user_id
    WHERE u.age < 30 AND t.timestamp >= current_timestamp() - INTERVAL 30 MINUTE
""")

total_buyers = spark.sql("""
    SELECT COUNT(DISTINCT user_id) AS total_buyers
    FROM transaction_stream
    WHERE timestamp >= current_timestamp() - INTERVAL 30 MINUTE
""")

proportion_under_30 = users_under_30.crossJoin(total_buyers).withColumn(
    "proportion", (F.col("under_30_buyers") / F.col("total_buyers")) * 100
)
proportion_under_30.show()

# Query 2: Top 3 Item Types in Last 10 Minutes
top_item_types_10m = spark.sql("""
    SELECT item_type, COUNT(*) AS count
    FROM transaction_stream
    WHERE timestamp >= current_timestamp() - INTERVAL 10 MINUTE
    GROUP BY item_type
    ORDER BY count DESC
    LIMIT 3
""")
top_item_types_10m.show()

# Query 3: Teenagers Buying Items in Last 10 Minutes
teenager_item_types_10m = spark.sql("""
    SELECT t.item_type, COUNT(*) AS count
    FROM transaction_stream t
    INNER JOIN user_stream u ON t.user_id = u.user_id
    WHERE u.age BETWEEN 13 AND 19
      AND t.timestamp >= current_timestamp() - INTERVAL 10 MINUTE
    GROUP BY t.item_type
    ORDER BY count DESC
    LIMIT 3
""")
teenager_item_types_10m.show()

# Query 4: Teenagers Buying Alcohol
teenagers_buying_alcohol = spark.sql("""
    SELECT COUNT(*) AS alcohol_buyers
    FROM transaction_stream t
    INNER JOIN user_stream u ON t.user_id = u.user_id
    WHERE u.age BETWEEN 13 AND 19
      AND t.item_type = 'alcohol'
      AND t.timestamp >= current_timestamp() - INTERVAL 30 MINUTE
""")
teenagers_buying_alcohol.show()

# Query 5: Top 3 Items by Revenue by Age
highest_revenue_items_by_age = spark.sql("""
    SELECT u.age, t.item_name, SUM(t.unit_price * t.amount) AS total_revenue
    FROM transaction_stream t
    INNER JOIN user_stream u ON t.user_id = u.user_id
    GROUP BY u.age, t.item_name
    ORDER BY total_revenue DESC
    LIMIT 3
""")
highest_revenue_items_by_age.show()

# Query 6: Top 3 Countries by Total Items Bought
top_countries_items_bought = spark.sql("""
    SELECT u.country, COUNT(*) AS total_items_bought
    FROM transaction_stream t
    INNER JOIN user_stream u ON t.user_id = u.user_id
    GROUP BY u.country
    ORDER BY total_items_bought DESC
    LIMIT 3
""")
top_countries_items_bought.show()
