"""
Spark Structured Streaming Consumer para Kafka
================================================
Lee datos del topic 'weather-data' usando Spark Structured Streaming.

Uso desde el contenedor Spark:
    docker exec -it spark-master spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        /opt/spark-scripts/spark_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# ============================================
# CONFIGURACION
# Desde dentro de Docker usa "kafka:9093"
# ============================================
KAFKA_BOOTSTRAP = "kafka:9093"
KAFKA_TOPIC = "weather-data"

# ============================================
# SCHEMA del JSON que envia el producer
# ============================================
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

# ============================================
# SPARK SESSION
# ============================================
spark = SparkSession.builder \
    .appName("WeatherDataConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================
# LEER STREAM DESDE KAFKA
# ============================================
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parsear el JSON del value de Kafka
weather_df = df.select(
    from_json(col("value").cast("string"), weather_schema).alias("data")
).select("data.*")

# ============================================
# OUTPUT - Mostrar en consola
# ============================================
query = weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print("Spark Consumer iniciado. Esperando datos del topic 'weather-data'...")
print("Presiona Ctrl+C para detener.")

query.awaitTermination()
