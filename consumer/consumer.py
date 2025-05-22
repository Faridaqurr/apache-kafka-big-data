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

# Schema
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# ===== STREAM SUHU =====
df_suhu_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

df_suhu = df_suhu_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_suhu).alias("data"), col("timestamp")) \
    .select("data.*", "timestamp") \
    .withWatermark("timestamp", "10 seconds")

# ===== STREAM KELEMBABAN =====
df_kelembaban_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

df_kelembaban = df_kelembaban_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
    .select(from_json(col("value"), schema_kelembaban).alias("data"), col("timestamp")) \
    .select("data.*", "timestamp") \
    .withWatermark("timestamp", "10 seconds")

# ===== PERINGATAN INDIVIDUAL =====
peringatan_suhu = df_suhu.filter("suhu > 80").selectExpr(
    "'[Peringatan Suhu Tinggi]' as jenis", "gudang_id", "concat('Suhu ', suhu, '°C') as detail"
)

peringatan_kelembaban = df_kelembaban.filter("kelembaban > 70").selectExpr(
    "'[Peringatan Kelembaban Tinggi]' as jenis", "gudang_id", "concat('Kelembaban ', kelembaban, '%') as detail"
)

peringatan_union = peringatan_suhu.union(peringatan_kelembaban)

peringatan_query = peringatan_union.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 100) \
    .start()

# ===== JOIN DAN GABUNGKAN (WINDOW 10s) =====
df_suhu_window = df_suhu.groupBy(
    col("gudang_id"),
    window(col("timestamp"), "10 seconds")
).agg(expr("max(suhu) as suhu"))

df_kelembaban_window = df_kelembaban.groupBy(
    col("gudang_id"),
    window(col("timestamp"), "10 seconds")
).agg(expr("max(kelembaban) as kelembaban"))

gabung = df_suhu_window.join(df_kelembaban_window, ["gudang_id", "window"])

alert_gabungan = gabung.withColumn(
    "status", expr("""
        CASE
            WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
            WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
            WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
            ELSE 'Aman'
        END
    """)
)

output_gabungan = alert_gabungan.selectExpr(
    "'[HASIL GABUNGAN]' as jenis",
    "gudang_id",
    "concat('Suhu: ', suhu, '°C') as suhu",
    "concat('Kelembaban: ', kelembaban, '%') as kelembaban",
    "status",
    "window.start as waktu_mulai",
    "window.end as waktu_selesai"
)

gabungan_query = output_gabungan.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 100) \
    .start()

# Tunggu kedua stream selesai
peringatan_query.awaitTermination()
gabungan_query.awaitTermination()
