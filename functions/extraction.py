from pyspark.sql.types import DateType,StructType, StructField,StringType, LongType, DoubleType,IntegerType, TimestampType

"""
    Етап видобування
"""

# Створенно схему набору даних
trip_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True),       # ID компанії-оператора
    StructField("dispatching_base_num", StringType(), True),    # база, яка надіслала замовлення
    StructField("originating_base_num", StringType(), True),    # база походження
    StructField("request_datetime", TimestampType(), True),
    StructField("on_scene_datetime", TimestampType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
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

bases_schema = StructType([
    StructField("base_num", StringType(), False),   # Primary key
    StructField("base_name", StringType(), True),
    StructField("base_city", StringType(), True),
    StructField("fleet_size", IntegerType(), True)
])

locations_schema = StructType([
    StructField("LocationID", LongType(), False),  # Primary key
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])

drivers_schema = StructType([
    StructField("driver_id", StringType(), False),   # Primary key
    StructField("hvfhs_license_num", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("experience_years", IntegerType(), True),
    StructField("rating", DoubleType(), True)
])

calendar_schema = StructType([
    StructField("datetime", TimestampType(), False),
    StructField("date", DateType(), True),
    StructField("hour", IntegerType(), True),
    StructField("day_of_week", StringType(), True),
    StructField("month", IntegerType(), True),
    StructField("year", IntegerType(), True)
])

def run_extracion():


    pass # <---- 