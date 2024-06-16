import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window, col, avg, from_unixtime, date_format
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType,
)

from dotenv import find_dotenv, dotenv_values, load_dotenv, get_key

dotenv_path = find_dotenv(raise_error_if_not_found=True)
successfully_loaded = load_dotenv(dotenv_path)

if not successfully_loaded:
    raise EnvironmentError(f"Dotenv file {dotenv_path} not found!")
else:
    for name, value in dotenv_values().items():
        logging.warning(f"{name}=`{value}`")

OUTPUT_PATH = get_key(dotenv_path, "OUTPUT_PATH")
CHECKPOINTS_PATH = get_key(dotenv_path, "CHECKPOINTS_PATH")


spark = (
    SparkSession.builder.master("local[6]")  # type: ignore
    .appName("IoTTelemetryDataProcessor")
    .getOrCreate()
)

# Objasni kako je moglo parsiranje preko seme
# schema = StructType(
#     [
#         StructField("ts", DoubleType(), True),
#         StructField("device", StringType(), True),
#         StructField("co", DoubleType(), True),
#         StructField("humidity", DoubleType(), True),
#         StructField("light", BooleanType(), True),
#         StructField("lpg", DoubleType(), True),
#         StructField("motion", BooleanType(), True),
#         StructField("smoke", DoubleType(), True),
#         StructField("temp", DoubleType(), True),
#     ]
# )

socket_df = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

split_df = socket_df.select(split(socket_df.value, ", ").alias("attributes"))

parsed_df = split_df.select(
    split_df.attributes.getItem(0).cast(DoubleType()).alias("timestamp"),
    split_df.attributes.getItem(1).cast(StringType()).alias("device_mac"),
    split_df.attributes.getItem(2).cast(DoubleType()).alias("carbon_oxide"),
    split_df.attributes.getItem(3).cast(DoubleType()).alias("humidity"),
    split_df.attributes.getItem(4).cast(BooleanType()).alias("light"),
    split_df.attributes.getItem(5).cast(DoubleType()).alias("liquid_petroleum_gas"),
    split_df.attributes.getItem(6).cast(BooleanType()).alias("motion"),
    split_df.attributes.getItem(7).cast(DoubleType()).alias("smoke"),
    split_df.attributes.getItem(8).cast(DoubleType()).alias("temperature"),
)

parsed_df = parsed_df.withColumn("timestamp", from_unixtime(col("timestamp")))

parsed_df = parsed_df.withColumn(
    "timestamp",
    date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"),
)

filtered_df = parsed_df.filter(parsed_df.motion == False)

windowed_df = (
    filtered_df.withWatermark("timestamp", "60 seconds")  # Objasni
    .groupBy(
        window(col("timestamp"), "30 seconds", "30 seconds").alias("window"),
        col("device_mac"),
    )
    .agg(
        avg("carbon_oxide").alias("avg_carbon_oxide"),
        avg("humidity").alias("avg_humidity"),
        avg("liquid_petroleum_gas").alias("avg_liquid_petroleum_gas"),
        avg("smoke").alias("avg_smoke"),
        avg("temperature").alias("avg_temperature"),
    )
    .select(
        date_format(col("window.start"), "yyyy-MM-dd HH:mm:ss").alias("window_start"),
        col("device_mac"),
        col("avg_carbon_oxide"),
        col("avg_humidity"),
        col("avg_liquid_petroleum_gas"),
        col("avg_smoke"),
        col("avg_temperature"),
    )
)

query = (
    windowed_df.writeStream.format("csv")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINTS_PATH)  # Objasni
    .outputMode("append")  # Objasni
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
