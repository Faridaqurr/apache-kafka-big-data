from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("SensorConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", 
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.kafka:kafka-clients:3.5.0,"
        "org.apache.commons:commons-pool2:2.11.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Definisi schema
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Stream data suhu
df_suhu_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

df_suhu = df_suhu_raw \
    .selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_suhu).alias("data"), col("timestamp")) \
    .select("data.*", "timestamp") \
    .withWatermark("timestamp", "10 seconds")

# Stream data kelembaban
df_kelembaban_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

df_kelembaban = df_kelembaban_raw \
    .selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_kelembaban).alias("data"), col("timestamp")) \
    .select("data.*", "timestamp") \
    .withWatermark("timestamp", "10 seconds")

# Buat window 5 detik dan agregasi max suhu per gudang_id dan window waktu
df_suhu_windowed = df_suhu.groupBy(
    col("gudang_id"),
    window(col("timestamp"), "5 seconds")
).agg(expr("max(suhu) as suhu"))

# Buat window 5 detik dan agregasi max kelembaban per gudang_id dan window waktu
df_kelembaban_windowed = df_kelembaban.groupBy(
    col("gudang_id"),
    window(col("timestamp"), "5 seconds")
).agg(expr("max(kelembaban) as kelembaban"))

# Join berdasarkan gudang_id dan window
joined = df_suhu_windowed.join(
    df_kelembaban_windowed,
    on=["gudang_id", "window"]
)

# Tambahkan kolom status
alert = joined.withColumn(
    "status", expr("""
        CASE
            WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
            WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
            WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
            ELSE 'Aman'
        END
    """)
)

# Pilih kolom untuk output supaya lebih rapi
output = alert.select(
    "gudang_id",
    "suhu",
    "kelembaban",
    "window.start",
    "window.end",
    "status"
)

# Tampilkan hasil ke console
query = output.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()