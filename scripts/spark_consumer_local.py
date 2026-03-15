"""
Spark Consumer - VERSION PARA EJECUTAR DESDE EL HOST
=====================================================
Usa localhost:9092 para conectarse a Kafka en Docker.

Uso:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_consumer_local.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "weather-data"

weather_schema = StructType([
    StructField("city", StringType(), True),
    StructField("region", StringType(), True),
    StructField("country", StringType(), True),
    StructField("temperature_c", FloatType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("wind_kph", FloatType(), True),
    StructField("condition", StringType(), True),
    StructField("timestamp", StringType(), True)
])

spark = SparkSession.builder \
    .appName("WeatherDataConsumer") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

weather_df = df.select(
    from_json(col("value").cast("string"), weather_schema).alias("data")
).select("data.*")

query = weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("Spark Consumer LOCAL iniciado. Conectado a localhost:9092")
print("Presiona Ctrl+C para detener.")

query.awaitTermination()
