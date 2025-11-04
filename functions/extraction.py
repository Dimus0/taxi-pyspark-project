from os import path
from pyspark.sql.types import DateType, StructType, StructField, StringType, LongType, DoubleType, IntegerType, \
    TimestampType
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pathlib import Path

"""
    Етап видобування
"""
trip_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True),
    StructField("dispatching_base_num", StringType(), True),
    StructField("originating_base_num", StringType(), True),
    StructField("request_datetime", TimestampType(), True),
    StructField("on_scene_datetime", TimestampType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", LongType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    StructField("tolls", DoubleType(), True),
    StructField("bcf", DoubleType(), True),
    StructField("sales_tax", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", IntegerType(), True),
    StructField("tips", DoubleType(), True),
    StructField("driver_pay", DoubleType(), True),
    StructField("shared_request_flag", StringType(), True),
    StructField("shared_match_flag", StringType(), True),
    StructField("access_a_ride_flag", StringType(), True),
    StructField("wav_request_flag", StringType(), True),
    StructField("wav_match_flag", IntegerType(), True)
])

# Додаткові таблиці
dispatch_base_schema = StructType([
    StructField("dispatch_base_num", StringType(), True)
])

# Таблиця бази, де поїздка починається
origin_base_schema = StructType([
    StructField("origin_base_num", StringType(), True)
])

vehicle_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True)
])


def run_extracion(spark: SparkSession, input_path: str):
    path = Path(input_path)
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Директорія не знайдена: {input_path}")

    parquet_files = sorted(path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"У директорії {input_path} немає parquet файлів")

    print(f"Знайдено {len(parquet_files)} parquet файлів. Починаємо зчитування...")

    df_trip = spark.read.schema(trip_schema).parquet(*[str(f) for f in parquet_files])

    print(f"Дані зчитано. Рядків: {df_trip.count()}, колонок: {len(df_trip.columns)}")

    df_dispatch_base = df_trip.selectExpr("dispatching_base_num as dispatch_base_num").distinct()
    df_origin_base = df_trip.selectExpr("originating_base_num as origin_base_num").distinct()

    df_vehicle = df_trip.select("hvfhs_license_num").distinct()

    print("DataFrames успішно створені:")
    print(f" - df_trip: {df_trip.count()} рядків")
    print(f" - df_dispatch_base: {df_dispatch_base.count()} унікальних dispatching_base_num")
    print(f" - df_origin_base: {df_origin_base.count()} унікальних originating_base_num")
    print(f" - df_vehicle: {df_vehicle.count()} унікальних hvfhs_license_num")

    print("\nПеревірка DataFrames:")
    print("df_trip:")
    df_trip.select("hvfhs_license_num", "pickup_datetime", "trip_miles").show(5, truncate=False)
    print("df_dispatch_base:")
    df_dispatch_base.show(5, truncate=False)
    print("df_origin_base:")
    df_origin_base.show(5, truncate=False)
    print("df_vehicle:")
    df_vehicle.show(5, truncate=False)

    print("Перевірка типів колонок:")
    df_trip.printSchema()
    df_dispatch_base.printSchema()
    df_origin_base.printSchema()
    df_vehicle.printSchema()
    return df_trip, df_dispatch_base, df_origin_base, df_vehicle
