from os import path
from pyspark.sql.types import DateType,StructType, StructField,StringType, LongType, DoubleType,IntegerType, TimestampType
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
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", IntegerType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    StructField("tolls", DoubleType(), True),
    StructField("bcf", DoubleType(), True),
    StructField("sales_tax", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("tips", DoubleType(), True),
    StructField("driver_pay", DoubleType(), True)
])

# Додаткові таблиці
base_schema = StructType([
    StructField("base_num", StringType(), True)
])

vehicle_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True)
])

zone_schema = StructType([
    StructField("zone_id", IntegerType(), True)
])

def run_extracion(spark: SparkSession,input_path: str):
    
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found {input_path}")

    pass